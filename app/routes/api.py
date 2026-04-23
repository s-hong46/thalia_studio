from flask import Blueprint, request, jsonify, current_app, Response, send_from_directory
import logging
from app.db import get_session
from app.models import (
    User,
    Draft,
    DraftVersion,
    StyleLabel,
    StylePreset,
    Performance,
    PerformanceEvent,
    VideoAsset,
)
from app.services.llm_service import (
    build_punchline_prompt,
    build_suggestion_prompt,
    build_feedback_prompt,
    build_performer_prompt,
    build_critic_prompt,
    build_audience_prompt,
    build_review_prompt,
    generate_text,
)
from app.services.embedding_service import embed_text
from app.services.pinecone_client import ensure_indexes
from app.services.process_map import (
    build_similarity_process_map,
    fake_process_nodes,
    link_references_to_markers,
)
from app.services.text_feedback_service import build_text_only_feedback
from app.services.embedding_service import classify_style_label
import uuid
from app.services.sse_hub import get_queue, publish_event
from app.services.analysis_service import split_paragraphs
from app.services.tts_service import generate_speech
from app.services.asr_service import transcribe_audio_file, transcribe_audio_segments
from app.services.rehearsal_service import analyze_rehearsal_take, build_evidence_clip_url
from app.services.video_dataset_ingest_service import (
    begin_foreground_analysis,
    build_video_preview_clip,
    build_video_preview_clip_result,
    end_foreground_analysis,
    get_video_dataset_status_payload,
    resolve_asset_file_path,
)
from app.services.video_match_service import match_video_references, match_comedian_profiles, match_focus_note_videos
import json
import time
import re
import io
import os
from typing import Dict, List

api = Blueprint("api", __name__)
logger = logging.getLogger(__name__)

MIN_ASR_TOKEN_COUNT = 3
MIN_REHEARSAL_TOKEN_COUNT = 4


def _looks_like_missing_openai_key(err: Exception) -> bool:
    return "openai_api_key is not set" in str(err).lower()


def _split_fallback_transcript_text(text: str):
    raw = str(text or "").strip()
    if not raw:
        return []

    pieces = re.split(r"(?<=[.!?])\s+|\n+|(?<=[,;:—-])\s+", raw)
    out = []
    for piece in pieces:
        cleaned = str(piece or "").strip()
        if not cleaned:
            continue

        words = re.findall(r"[A-Za-z0-9']+", cleaned)
        if len(words) <= 14:
            out.append(cleaned)
            continue

        chunk = []
        count = 0
        for token in cleaned.split():
            chunk.append(token)
            if re.search(r"[A-Za-z0-9']", token):
                count += 1
            if count >= 14 and re.search(r"[,:;.!?]$|\b(?:so|but|then|like|yeah|well|actually)\b", token, re.I):
                out.append(" ".join(chunk).strip())
                chunk = []
                count = 0
        if chunk:
            out.append(" ".join(chunk).strip())

    return out or [raw]


def _build_fallback_transcript_segments(text: str, total_duration_sec: float | None = None):
    parts = _split_fallback_transcript_text(text)
    if not parts:
        return []
    total_words = sum(max(1, len(re.findall(r"[A-Za-z0-9']+", part))) for part in parts)
    if total_duration_sec is None or total_duration_sec <= 0:
        total_duration_sec = max(2.0, total_words / 2.6)
    total_duration_sec = max(1.0, float(total_duration_sec))
    cursor = 0.0
    segments = []
    for idx, part in enumerate(parts):
        word_count = max(1, len(re.findall(r"[A-Za-z0-9']+", part)))
        duration = total_duration_sec * (word_count / max(1, total_words))
        if idx == len(parts) - 1:
            end = total_duration_sec
        else:
            end = min(total_duration_sec, cursor + max(0.8, duration))
        if end <= cursor:
            end = min(total_duration_sec, cursor + 0.8)
        segments.append(
            {
                "start": round(cursor, 3),
                "end": round(end, 3),
                "text": part,
                "source": "fallback-text",
            }
        )
        cursor = end
    return segments


def _parse_optional_float(value, default: float | None = None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _count_meaningful_tokens(text: str) -> int:
    latin = re.findall(r"[A-Za-z0-9']+", text or "")
    cjk = re.findall(r"[\u4e00-\u9fff]", text or "")
    return len(latin) + len(cjk)


def _parse_bool_flag(value, default: bool = False) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _hydrate_video_preview_urls(video_references: List[Dict]) -> List[Dict]:
    hydrated = []
    for ref in video_references or []:
        if not isinstance(ref, dict):
            continue
        item = dict(ref)
        preview_video_url = str(item.get("preview_video_url", "")).strip()
        preview_url = str(item.get("preview_url", "")).strip()
        if not preview_video_url and preview_url.startswith("/static/"):
            preview_video_url = preview_url
        item["preview_video_url"] = preview_video_url
        hydrated.append(item)
    return hydrated


def _safe_generate_marker_demo_audio(text: str):
    demo_text = str(text or "").strip()
    if not demo_text:
        return None
    try:
        return generate_speech(demo_text)
    except Exception:
        logger.exception("marker demo audio generation failed")
        return None


def _parse_audience_payload(raw_text: str):
    cleaned = raw_text.strip()
    cleaned = re.sub(r"```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
    json_match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(0))
            reaction = str(data.get("reaction", "")).strip()
            score = float(data.get("score", 0))
            score = max(0.0, min(10.0, score))
            if reaction:
                return reaction, score
        except Exception:
            pass
    try:
        data = json.loads(cleaned)
        reaction = str(data.get("reaction", "")).strip()
        score = float(data.get("score", 0))
        score = max(0.0, min(10.0, score))
        if reaction:
            return reaction, score
    except Exception:
        pass
    score_match = re.search(r"score\\s*:?\\s*([0-9]+(?:\\.[0-9]+)?)", cleaned, re.I)
    score = float(score_match.group(1)) if score_match else 0.0
    score = max(0.0, min(10.0, score))
    lines = [x.strip() for x in cleaned.splitlines() if x.strip()]
    lines = [
        x
        for x in lines
        if not re.match(r"^score\\b", x, re.I)
        and x.lower() != "audience"
        and x.lower() != "reaction"
    ]
    reaction = " ".join(lines).strip()
    if reaction:
        return reaction, score
    match = re.search(r"([0-9]+(?:\\.[0-9]+)?)", cleaned)
    score = float(match.group(1)) if match else 0.0
    score = max(0.0, min(10.0, score))
    reaction = cleaned.strip()
    return reaction, score


def build_evidence_url(audio_bytes: bytes, filename: str, marker_time_range):
    return build_evidence_clip_url(audio_bytes, filename, marker_time_range)


@api.route("/api/drafts", methods=["GET", "POST"])
def drafts():
    if request.method == "GET":
        nickname = request.args.get("nickname", "").strip()
        if not nickname:
            return jsonify({"error": "nickname required"}), 400
        db = get_session()
        try:
            user = db.query(User).filter_by(nickname=nickname).first()
            if not user:
                return jsonify({"items": []})
            drafts = (
                db.query(Draft)
                .filter_by(user_id=user.id, status="active")
                .order_by(Draft.updated_at.desc())
                .all()
            )
            return jsonify(
                {
                    "items": [
                        {
                            "id": d.id,
                            "title": d.title,
                            "updated_at": d.updated_at.isoformat()
                            if d.updated_at
                            else None,
                        }
                        for d in drafts
                    ]
                }
            )
        finally:
            db.close()

    data = request.get_json(force=True)
    nickname = data.get("nickname", "").strip()
    if not nickname:
        return jsonify({"error": "nickname required"}), 400
    title = data.get("title", "Untitled Draft")
    db = get_session()
    try:
        user = db.query(User).filter_by(nickname=nickname).first()
        if not user:
            user = User(nickname=nickname)
            db.add(user)
            db.commit()
            db.refresh(user)
        draft = Draft(user_id=user.id, title=title, content="")
        db.add(draft)
        db.commit()
        db.refresh(draft)
        return jsonify({"draft_id": draft.id})
    finally:
        db.close()


@api.post("/api/save")
def save_draft():
    data = request.get_json(force=True)
    draft_id = data.get("draft_id")
    content = data.get("content", "")
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400
    db = get_session()
    try:
        draft = db.query(Draft).filter_by(id=draft_id).first()
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        draft.content = content
        db.add(DraftVersion(draft_id=draft_id, content=content))
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@api.get("/api/drafts/<int:draft_id>")
def get_draft(draft_id: int):
    nickname = request.args.get("nickname", "").strip()
    if not nickname:
        return jsonify({"error": "nickname required"}), 400
    db = get_session()
    try:
        user = db.query(User).filter_by(nickname=nickname).first()
        if not user:
            return jsonify({"error": "user not found"}), 404
        draft = (
            db.query(Draft)
            .filter_by(id=draft_id, user_id=user.id, status="active")
            .first()
        )
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        return jsonify(
            {
                "id": draft.id,
                "title": draft.title,
                "content": draft.content,
            }
        )
    finally:
        db.close()


@api.patch("/api/drafts/<int:draft_id>")
def rename_draft(draft_id: int):
    data = request.get_json(force=True)
    nickname = data.get("nickname", "").strip()
    title = data.get("title", "").strip()
    if not nickname or not title:
        return jsonify({"error": "nickname and title required"}), 400
    db = get_session()
    try:
        user = db.query(User).filter_by(nickname=nickname).first()
        if not user:
            return jsonify({"error": "user not found"}), 404
        draft = (
            db.query(Draft)
            .filter_by(id=draft_id, user_id=user.id, status="active")
            .first()
        )
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        draft.title = title
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@api.post("/api/drafts/<int:draft_id>/archive")
def archive_draft(draft_id: int):
    data = request.get_json(force=True)
    nickname = data.get("nickname", "").strip()
    if not nickname:
        return jsonify({"error": "nickname required"}), 400
    db = get_session()
    try:
        user = db.query(User).filter_by(nickname=nickname).first()
        if not user:
            return jsonify({"error": "user not found"}), 404
        draft = (
            db.query(Draft)
            .filter_by(id=draft_id, user_id=user.id, status="active")
            .first()
        )
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        draft.status = "archived"
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@api.route("/api/style-presets", methods=["GET", "POST"])
def style_presets():
    if request.method == "GET":
        nickname = request.args.get("nickname", "").strip()
        if not nickname:
            return jsonify({"error": "nickname required"}), 400
        db = get_session()
        try:
            user = db.query(User).filter_by(nickname=nickname).first()
            if not user:
                return jsonify({"items": []})
            items = (
                db.query(StylePreset)
                .filter_by(user_id=user.id)
                .order_by(StylePreset.updated_at.desc(), StylePreset.id.desc())
                .all()
            )
            return jsonify(
                {
                    "items": [
                        {
                            "id": item.id,
                            "name": item.name,
                            "description": item.description,
                            "updated_at": item.updated_at.isoformat()
                            if item.updated_at
                            else None,
                        }
                        for item in items
                    ]
                }
            )
        finally:
            db.close()

    data = request.get_json(force=True)
    nickname = str(data.get("nickname", "")).strip()
    name = str(data.get("name", "")).strip()
    description = str(data.get("description", "")).strip()
    if not nickname:
        return jsonify({"error": "nickname required"}), 400
    if not name:
        return jsonify({"error": "name required"}), 400

    db = get_session()
    try:
        user = db.query(User).filter_by(nickname=nickname).first()
        if not user:
            user = User(nickname=nickname)
            db.add(user)
            db.commit()
            db.refresh(user)
        item = (
            db.query(StylePreset)
            .filter_by(user_id=user.id, name=name)
            .first()
        )
        if item:
            item.description = description
        else:
            item = StylePreset(user_id=user.id, name=name, description=description)
            db.add(item)
        db.commit()
        db.refresh(item)
        return jsonify(
            {
                "ok": True,
                "item": {
                    "id": item.id,
                    "name": item.name,
                    "description": item.description,
                    "updated_at": item.updated_at.isoformat()
                    if item.updated_at
                    else None,
                },
            }
        )
    finally:
        db.close()


@api.post("/api/punchlines")
def punchlines():
    data = request.get_json(force=True)
    topic = data.get("topic", "")
    prompt = build_punchline_prompt(topic)
    text = generate_text(prompt)
    lines = [x.strip() for x in text.split("\n") if x.strip()]
    return jsonify({"items": lines})


@api.post("/api/suggestions")
def suggestions():
    data = request.get_json(force=True)
    draft = data.get("draft", "")
    prompt = build_suggestion_prompt(draft)
    text = generate_text(prompt)
    return jsonify({"suggestion": text.strip()})


@api.post("/api/feedback")
def feedback():
    data = request.get_json(force=True)
    draft = data.get("draft", "")
    pc = ensure_indexes()
    settings = current_app.config["SETTINGS"]
    vec = embed_text(draft)
    index = pc.Index(settings.pinecone_index_anti)
    result = index.query(vector=vec, top_k=5, include_metadata=True)
    anti = [m["metadata"].get("text", "") for m in result["matches"]]
    prompt = build_feedback_prompt(draft, anti)
    feedback_text = generate_text(prompt)
    return jsonify({"feedback": feedback_text, "anti_examples": anti})


@api.get("/api/process-logs")
def process_logs():
    draft_id = request.args.get("draft_id")
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400
    return jsonify({"nodes": fake_process_nodes()})


@api.post("/api/accept-suggestion")
def accept_suggestion():
    data = request.get_json(force=True)
    draft_id = data.get("draft_id")
    text = data.get("text", "").strip()
    if not draft_id or not text:
        return jsonify({"error": "draft_id and text required"}), 400
    db = get_session()
    try:
        draft = db.query(Draft).filter_by(id=draft_id).first()
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        label, confidence = classify_style_label(text)
        db.add(
            StyleLabel(
                user_id=draft.user_id, label=label, confidence=confidence
            )
        )
        db.commit()
        pc = ensure_indexes()
        settings = current_app.config["SETTINGS"]
        vec = embed_text(text)
        index = pc.Index(settings.pinecone_index_preferences)
        index.upsert(
            vectors=[
                {
                    "id": str(uuid.uuid4()),
                    "values": vec,
                    "metadata": {
                        "user_id": draft.user_id,
                        "draft_id": draft_id,
                        "label": label,
                        "text": text,
                    },
                }
            ]
        )
        return jsonify({"ok": True, "label": label, "confidence": confidence})
    finally:
        db.close()


@api.post("/api/performance/start")
def start_performance():
    data = request.get_json(force=True)
    draft_id = data.get("draft_id")
    text = data.get("text", "").strip()
    if not draft_id or not text:
        return jsonify({"error": "draft_id and text required"}), 400
    db = get_session()
    try:
        draft = db.query(Draft).filter_by(id=draft_id).first()
        if not draft:
            return jsonify({"error": "draft not found"}), 404
        performance = Performance(
            draft_id=draft_id, status="running", source_text=text, score=0.0
        )
        db.add(performance)
        db.commit()
        db.refresh(performance)

        performer_text = generate_text(build_performer_prompt(text)).strip()
        db.add(
            PerformanceEvent(
                performance_id=performance.id,
                role="performer",
                text=performer_text,
            )
        )
        db.commit()
        publish_event(
            draft_id,
            "stage_event",
            json.dumps(
                {
                    "performance_id": performance.id,
                    "role": "performer",
                    "text": performer_text,
                }
            ),
        )

        critic_text = generate_text(
            build_critic_prompt(text, performer_text)
        ).strip()
        db.add(
            PerformanceEvent(
                performance_id=performance.id,
                role="critic",
                text=critic_text,
            )
        )
        db.commit()
        publish_event(
            draft_id,
            "stage_event",
            json.dumps(
                {
                    "performance_id": performance.id,
                    "role": "critic",
                    "text": critic_text,
                }
            ),
        )

        audience_raw = generate_text(
            build_audience_prompt(text, performer_text)
        ).strip()
        reaction, score = _parse_audience_payload(audience_raw)
        db.add(
            PerformanceEvent(
                performance_id=performance.id,
                role="audience",
                text=reaction,
            )
        )
        performance.score = score
        performance.status = "completed"
        db.commit()
        publish_event(
            draft_id,
            "stage_event",
            json.dumps(
                {
                    "performance_id": performance.id,
                    "role": "audience",
                    "text": reaction,
                    "score": score,
                }
            ),
        )
        publish_event(
            draft_id,
            "stage_end",
            json.dumps(
                {
                    "performance_id": performance.id,
                    "score": score,
                    "status": performance.status,
                }
            ),
        )
        return jsonify(
            {
                "performance_id": performance.id,
                "status": performance.status,
                "score": score,
            }
        )
    finally:
        db.close()


@api.post("/api/performance/review")
def performance_review():
    data = request.get_json(force=True)
    performance_id = data.get("performance_id")
    if not performance_id:
        return jsonify({"error": "performance_id required"}), 400
    db = get_session()
    try:
        performance = db.query(Performance).filter_by(id=performance_id).first()
        if not performance:
            return jsonify({"error": "performance not found"}), 404
        events = (
            db.query(PerformanceEvent)
            .filter_by(performance_id=performance_id)
            .order_by(PerformanceEvent.created_at.asc())
            .all()
        )
        performer_text = next((e.text for e in events if e.role == "performer"), "")
        critic_text = next((e.text for e in events if e.role == "critic"), "")
        audience_text = next((e.text for e in events if e.role == "audience"), "")
        prompt = build_review_prompt(
            performance.source_text,
            performer_text,
            critic_text,
            audience_text,
            performance.score,
        )
        review_text = generate_text(prompt).strip()
        audio_url = None
        try:
            audio_url = generate_speech(review_text)
        except Exception:
            audio_url = None
        return jsonify({"text": review_text, "audio_url": audio_url})
    finally:
        db.close()


@api.post("/api/performance/cancel")
def cancel_performance():
    data = request.get_json(force=True)
    performance_id = data.get("performance_id")
    save = bool(data.get("save", False))
    if not performance_id:
        return jsonify({"error": "performance_id required"}), 400
    db = get_session()
    try:
        performance = db.query(Performance).filter_by(id=performance_id).first()
        if not performance:
            return jsonify({"error": "performance not found"}), 404
        if save:
            performance.status = "canceled"
            db.commit()
            return jsonify({"ok": True, "status": performance.status})
        db.query(PerformanceEvent).filter_by(performance_id=performance_id).delete()
        db.delete(performance)
        db.commit()
        return jsonify({"ok": True, "status": "discarded"})
    finally:
        db.close()


@api.get("/api/performances")
def list_performances():
    draft_id = request.args.get("draft_id")
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400
    db = get_session()
    try:
        items = (
            db.query(Performance)
            .filter_by(draft_id=draft_id)
            .order_by(Performance.created_at.desc())
            .all()
        )
        return jsonify(
            {
                "items": [
                    {
                        "id": p.id,
                        "status": p.status,
                        "score": p.score,
                        "created_at": p.created_at.isoformat()
                        if p.created_at
                        else None,
                    }
                    for p in items
                ]
            }
        )
    finally:
        db.close()


@api.get("/api/performances/<int:performance_id>")
def get_performance(performance_id: int):
    db = get_session()
    try:
        performance = db.query(Performance).filter_by(id=performance_id).first()
        if not performance:
            return jsonify({"error": "performance not found"}), 404
        events = (
            db.query(PerformanceEvent)
            .filter_by(performance_id=performance_id)
            .order_by(PerformanceEvent.created_at.asc())
            .all()
        )
        return jsonify(
            {
                "id": performance.id,
                "status": performance.status,
                "score": performance.score,
                "created_at": performance.created_at.isoformat()
                if performance.created_at
                else None,
                "events": [
                    {
                        "id": e.id,
                        "role": e.role,
                        "text": e.text,
                        "created_at": e.created_at.isoformat()
                        if e.created_at
                        else None,
                    }
                    for e in events
                ],
            }
        )
    finally:
        db.close()


@api.get("/api/stream")
def stream():
    draft_id = request.args.get("draft_id")
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400

    q = get_queue(draft_id)

    def gen():
        while True:
            try:
                event, data = q.get(timeout=15)
                yield f"event: {event}\n"
                yield f"data: {data}\n\n"
            except Exception:
                yield "event: ping\ndata: {}\n\n"
                time.sleep(1)

    return Response(gen(), mimetype="text/event-stream")


@api.post("/api/analysis")
def analysis():
    data = request.get_json(force=True)
    draft_id = data.get("draft_id")
    draft = data.get("draft", "")
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400
    paragraphs = split_paragraphs(draft)
    pc = ensure_indexes()
    settings = current_app.config["SETTINGS"]
    index = pc.Index(settings.pinecone_index_anti)
    matched_segments = []
    all_anti = []
    for para in paragraphs:
        vec = embed_text(para)
        result = index.query(vector=vec, top_k=3, include_metadata=True)
        anti = [m["metadata"].get("text", "") for m in result["matches"]]
        all_anti.extend(anti)
        matched_segments.append(
            {
                "segment": para[:160],
                "examples": anti,
            }
        )
    prompt = build_feedback_prompt(draft, all_anti[:6])
    feedback_text = generate_text(prompt)
    publish_event(
        draft_id,
        "feedback",
        json.dumps(
            {
                "feedback": feedback_text,
                "matched_segments": matched_segments,
            }
        ),
    )
    publish_event(
        draft_id,
        "process_map",
        json.dumps(
            build_similarity_process_map(
                style_label="general",
                markers=[],
                video_references=[],
            )
        ),
    )
    return jsonify({"ok": True})


@api.post("/api/asr/transcribe")
def asr_transcribe():
    audio = request.files.get("audio")
    if not audio:
        return jsonify({"error": "audio file required"}), 400
    if not audio.filename:
        return jsonify({"error": "audio filename required"}), 400
    fallback_text = str(request.form.get("fallback_text", "") or "").strip()
    source = "openai"
    try:
        text = transcribe_audio_file(audio.stream, audio.filename)
    except Exception as err:
        if _looks_like_missing_openai_key(err) and fallback_text:
            text = fallback_text
            source = "browser-fallback"
        else:
            return jsonify({"error": f"ASR failed: {err}"}), 500
    if not text:
        return jsonify({"error": "empty transcription"}), 400
    if _count_meaningful_tokens(text) < MIN_ASR_TOKEN_COUNT:
        return jsonify({"error": "input too short for reliable transcription"}), 400
    return jsonify({"text": text, "source": source})


@api.post("/api/rehearsal/analyze")
def rehearsal_analyze():
    draft_id = request.form.get("draft_id", "").strip()
    script = request.form.get("script", "").strip()
    style_preset = request.form.get("style_preset", "").strip()
    include_video_dataset = _parse_bool_flag(
        request.form.get("include_video_dataset", None),
        default=_parse_bool_flag(request.form.get("include_video_reference", "1"), default=True),
    )
    audio = request.files.get("audio")
    if not audio:
        return jsonify({"error": "audio file required"}), 400
    if not audio.filename:
        return jsonify({"error": "audio filename required"}), 400

    audio_bytes = audio.read()
    if not audio_bytes:
        return jsonify({"error": "empty audio"}), 400
    logger.info(
        "rehearsal request: draft_id=%s include_video_dataset=%s script_len=%s filename=%s",
        draft_id or "-",
        include_video_dataset,
        len(script),
        audio.filename,
    )
    fallback_transcript_text = str(request.form.get("transcript_text", "") or "").strip()
    fallback_audio_duration_sec = _parse_optional_float(request.form.get("audio_duration_sec"), None)
    begin_foreground_analysis()
    try:
        transcript_source = "openai"
        try:
            transcript_segments = transcribe_audio_segments(
                io.BytesIO(audio_bytes), audio.filename
            )
        except Exception as err:
            if not _looks_like_missing_openai_key(err):
                raise
            if not fallback_transcript_text:
                return jsonify({"error": f"ASR failed: {err}"}), 500
            transcript_source = "browser-fallback"
            transcript_segments = _build_fallback_transcript_segments(
                fallback_transcript_text,
                total_duration_sec=fallback_audio_duration_sec,
            )
        if not transcript_segments and fallback_transcript_text:
            transcript_source = "browser-fallback"
            transcript_segments = _build_fallback_transcript_segments(
                fallback_transcript_text,
                total_duration_sec=fallback_audio_duration_sec,
            )
        logger.info(
            "rehearsal stage: transcription done segments=%s source=%s",
            len(transcript_segments),
            transcript_source,
        )
        if not transcript_segments:
            return jsonify({"error": "empty transcription segments"}), 400

        transcript_text = " ".join(
            str(seg.get("text", "")).strip()
            for seg in transcript_segments
            if isinstance(seg, dict) and str(seg.get("text", "")).strip()
        )
        if _count_meaningful_tokens(transcript_text) < MIN_REHEARSAL_TOKEN_COUNT:
            return jsonify(
                {
                    "error": (
                        "input too short for rehearsal analysis; please speak at least one full sentence"
                    )
                }
            ), 400

        try:
            auto_style_label, auto_style_confidence = classify_style_label(
                f"{script}\n{transcript_text}"
            )
        except Exception:
            auto_style_label, auto_style_confidence = "general", 0.5

        effective_style = style_preset or auto_style_label

        analysis_result = analyze_rehearsal_take(
            script=script,
            transcript_segments=transcript_segments,
            style_preset=effective_style,
            audio_bytes=audio_bytes,
            audio_filename=audio.filename,
        )
        utterances = [dict(item) for item in (analysis_result.get("utterances", []) or []) if isinstance(item, dict)]
        logger.info(
            "rehearsal stage: utterance analysis done utterances=%s focus_notes=%s markers=%s",
            len(utterances),
            len(analysis_result.get("focus_notes", []) or []),
            len(analysis_result.get("markers", []) or []),
        )
        markers = []
        marker_by_utterance_id = {}
        for marker in analysis_result.get("markers", []):
            hydrated = dict(marker)
            hydrated["evidence_audio_url"] = build_evidence_url(
                audio_bytes=audio_bytes,
                filename=audio.filename,
                marker_time_range=hydrated.get("time_range", [0.0, 0.0]),
            )
            hydrated["demo_audio_url"] = _safe_generate_marker_demo_audio(
                hydrated.get("demo_text", "")
            )
            markers.append(hydrated)
            utterance_id = str(hydrated.get("utterance_id", "")).strip()
            if utterance_id:
                marker_by_utterance_id[utterance_id] = hydrated
        logger.info("rehearsal stage: marker hydration done markers=%s", len(markers))

        focus_notes = []
        for note in analysis_result.get("focus_notes", []) or []:
            if not isinstance(note, dict):
                continue
            hydrated_note = dict(note)
            utterance_id = str(hydrated_note.get("utterance_id", "")).strip()
            linked_marker = marker_by_utterance_id.get(utterance_id)
            if linked_marker:
                hydrated_note["marker_id"] = linked_marker.get("id")
                hydrated_note["evidence_audio_url"] = linked_marker.get("evidence_audio_url")
            else:
                hydrated_note["evidence_audio_url"] = build_evidence_url(
                    audio_bytes=audio_bytes,
                    filename=audio.filename,
                    marker_time_range=hydrated_note.get("time_range", [0.0, 0.0]),
                )
            focus_notes.append(hydrated_note)

        issue_types = [str(marker.get("issue_type", "")).strip() for marker in markers]
        dataset_status = get_video_dataset_status_payload()
        video_references = []
        focus_note_video_groups = []
        if include_video_dataset:
            settings = current_app.config.get("SETTINGS")
            top_k = int(getattr(settings, "video_dataset_top_k", 3) or 3)
            initial_top_k = int(getattr(settings, "video_dataset_initial_top_k", 20) or 20)
            focus_note_video_groups = match_focus_note_videos(
                script=script,
                utterances=utterances,
                focus_notes=focus_notes,
                style_label=effective_style,
                audio_bytes=audio_bytes,
                audio_filename=audio.filename,
                top_k=top_k,
                initial_top_k=initial_top_k,
            )
            refs_flat = []
            for group in focus_note_video_groups:
                items = group.get("items", []) if isinstance(group, dict) else []
                for item in items:
                    refs_flat.append(item)
            video_references = _hydrate_video_preview_urls(refs_flat)
            if not video_references and markers:
                legacy_refs = match_video_references(
                    script=script,
                    transcript_segments=transcript_segments,
                    markers=markers,
                    style_label=effective_style,
                    audio_bytes=audio_bytes,
                    audio_filename=audio.filename,
                    issue_types=issue_types,
                    top_k=top_k,
                    initial_top_k=initial_top_k,
                )
                legacy_refs = link_references_to_markers(markers, legacy_refs)
                video_references = _hydrate_video_preview_urls(legacy_refs)
            logger.info(
                "rehearsal stage: focus-note video match done groups=%s refs=%s dataset_status=%s",
                len(focus_note_video_groups),
                len(video_references),
                dataset_status.get("status", "unknown"),
            )
        else:
            video_references = []

        refs_by_note_id = {}
        for group in focus_note_video_groups:
            if not isinstance(group, dict):
                continue
            note_id = str(group.get("note_id", "")).strip()
            items = _hydrate_video_preview_urls(group.get("items", []) or [])
            if note_id:
                refs_by_note_id[note_id] = items
        for note in focus_notes:
            note_id = str(note.get("id", "")).strip()
            note_refs = refs_by_note_id.get(note_id, [])
            note["video_references"] = note_refs
            note["video_reference"] = note_refs[0] if note_refs else None

        comedian_matches = match_comedian_profiles(
            script=script,
            transcript_segments=transcript_segments,
            markers=markers,
            style_label=effective_style,
            audio_bytes=audio_bytes,
            audio_filename=audio.filename,
        )
        feedback_payload = build_text_only_feedback(
            style_label=effective_style,
            script=script,
            transcript_text=transcript_text,
            markers=markers,
        )
        process_map_payload = build_similarity_process_map(
            style_label=effective_style,
            markers=markers,
            video_references=video_references,
            comedian_matches=comedian_matches,
        )

        payload = {
            "alignment": analysis_result.get("alignment"),
            "markers": markers,
            "utterances": utterances,
            "joke_units": analysis_result.get("joke_units", []),
            "focus_notes": focus_notes,
            "script": script,
            "style_detection": {
                "label": auto_style_label,
                "confidence": round(float(auto_style_confidence), 3),
                "source": "manual_override" if style_preset else "auto",
                "effective_style": effective_style,
            },
            "transcript_source": transcript_source,
            "video_dataset_enabled": include_video_dataset,
            "video_reference_enabled": include_video_dataset,
            "video_dataset_status": dataset_status,
            "video_references": video_references,
            "comedian_matches": comedian_matches,
            "feedback": feedback_payload,
            "process_map": process_map_payload,
        }
        logger.info(
            "rehearsal result: utterances=%s focus_notes=%s refs=%s feedback_items=%s process_map_status=%s",
            len(utterances),
            len(focus_notes),
            len(video_references),
            len(feedback_payload.get("items", [])),
            process_map_payload.get("status", "unknown"),
        )
        if draft_id:
            publish_event(
                draft_id,
                "rehearsal_analysis",
                json.dumps(
                    {
                        "draft_id": draft_id,
                        "alignment": payload["alignment"],
                        "markers": payload["markers"],
                        "utterances": payload["utterances"],
                        "joke_units": payload["joke_units"],
                        "focus_notes": payload["focus_notes"],
                        "script": payload["script"],
                        "style_detection": payload["style_detection"],
                        "video_dataset_enabled": payload["video_dataset_enabled"],
                        "video_reference_enabled": payload["video_reference_enabled"],
                        "video_dataset_status": payload["video_dataset_status"],
                        "video_references": payload["video_references"],
                        "comedian_matches": payload["comedian_matches"],
                        "feedback": payload["feedback"],
                        "process_map": payload["process_map"],
                    }
                ),
            )
        return jsonify(payload)
    except Exception as err:
        logger.exception("rehearsal analysis failed")
        return jsonify({"error": f"rehearsal analysis failed: {err}"}), 500
    finally:
        end_foreground_analysis()


@api.get("/api/video-dataset/status")
def video_dataset_status():
    return jsonify(get_video_dataset_status_payload())


@api.get("/api/video-dataset/preview")
def video_dataset_preview():
    asset_id = request.args.get("asset_id")
    start_sec = request.args.get("start_sec")
    end_sec = request.args.get("end_sec")
    if not asset_id or start_sec is None or end_sec is None:
        return jsonify({"error": "asset_id/start_sec/end_sec required"}), 400
    preview_url = build_video_preview_clip(
        asset_id=int(_to_float(asset_id, 0)),
        start_sec=_to_float(start_sec, 0.0),
        end_sec=_to_float(end_sec, 0.0),
    )
    if preview_url:
        return jsonify(
            {
                "preview_url": preview_url,
                "preview_file": os.path.basename(str(preview_url).rstrip("/")),
            }
        )
    result = build_video_preview_clip_result(
        asset_id=int(_to_float(asset_id, 0)),
        start_sec=_to_float(start_sec, 0.0),
        end_sec=_to_float(end_sec, 0.0),
    )
    if not result.get("ok"):
        return jsonify({"error": result.get("error") or "failed to build preview"}), 500
    return jsonify({"preview_url": result.get("preview_url"), "preview_file": result.get("preview_file")})


@api.get("/api/video-dataset/preview-file/<path:filename>")
def video_dataset_preview_file(filename: str):
    settings = current_app.config.get("SETTINGS")
    preview_dir = getattr(settings, "video_dataset_preview_dir", "") if settings else ""
    if not preview_dir:
        return jsonify({"error": "preview directory unavailable"}), 500
    full_path = os.path.join(preview_dir, filename)
    if not os.path.isfile(full_path):
        return jsonify({"error": "preview file missing"}), 404
    if os.path.getsize(full_path) < 20 * 1024:
        return jsonify({"error": "preview file too small"}), 500
    return send_from_directory(preview_dir, filename, mimetype="video/mp4", conditional=True)


@api.get("/api/video-dataset/source-file/<int:asset_id>")
def video_dataset_source_file(asset_id: int):
    db = get_session()
    try:
        asset = db.query(VideoAsset).filter_by(id=int(asset_id)).first()
        if not asset:
            return jsonify({"error": "source asset not found"}), 404
        settings = current_app.config.get("SETTINGS")
        source_path = resolve_asset_file_path(asset, settings, db=db) if settings else None
        if source_path is None or not source_path.exists():
            return jsonify({"error": "source file missing"}), 404
        directory = os.path.dirname(str(source_path))
        filename = os.path.basename(str(source_path))
        response = send_from_directory(directory, filename, conditional=True)
        start_sec = request.args.get("start_sec")
        end_sec = request.args.get("end_sec")
        if start_sec is not None or end_sec is not None:
            response.headers["X-Clip-Start-Sec"] = str(_to_float(start_sec, 0.0))
            response.headers["X-Clip-End-Sec"] = str(_to_float(end_sec, 0.0))
        return response
    finally:
        db.close()
