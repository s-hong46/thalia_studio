import io
import logging
import os
import re
import subprocess
import tempfile
import wave
from typing import Dict, Iterable, List, Optional, Tuple

from app.config import Settings
from app.db import get_session
from app.models import VideoAsset, VideoChunk
from app.services.audio_compat import audioop
from app.services.dataset_reference_service import load_dataset_reference_spans
from app.services.embedding_service import embed_text
from app.services.llm_service import (
    adjudicate_transferable_candidate,
    generate_pedagogical_retrieval_spec,
    screen_pedagogical_candidate,
)
from app.services.pinecone_client import ensure_indexes
from app.services.video_catalog_service import resolve_video_metadata
from app.services.video_span_service import load_structured_video_spans

logger = logging.getLogger(__name__)


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _style_similarity(user_style: str, candidate_style: str) -> float:
    left = (user_style or "").strip().lower()
    right = (candidate_style or "").strip().lower()
    if not left or not right:
        return 0.5
    if left == right:
        return 1.0
    if left in right or right in left:
        return 0.8
    return 0.45


def _rhythm_similarity(user_profile: Dict, candidate: Dict) -> float:
    user_pace = _to_float(user_profile.get("pace_wps"), 0.0)
    user_pause = _to_float(user_profile.get("pause_density"), 0.0)
    user_energy = _to_float(user_profile.get("energy_rms"), 0.0)
    cand_pace = _to_float(candidate.get("pace_wps"), 0.0)
    cand_pause = _to_float(candidate.get("pause_density"), 0.0)
    cand_energy = _to_float(candidate.get("energy_rms"), 0.0)

    pace_diff = min(1.0, abs(user_pace - cand_pace) / 3.5)
    pause_diff = min(1.0, abs(user_pause - cand_pause) / 0.25)
    energy_diff = min(1.0, abs(user_energy - cand_energy) / 0.7)
    return round(max(0.0, 1.0 - (pace_diff * 0.5 + pause_diff * 0.3 + energy_diff * 0.2)), 4)


def rank_video_candidates(user_profile: Dict, candidates: List[Dict], top_k: int = 3) -> List[Dict]:
    ranked = []
    for item in candidates:
        semantic_score = max(0.0, min(1.0, _to_float(item.get("semantic_score"), 0.0)))
        style_score = _style_similarity(
            str(user_profile.get("style_label", "")),
            str(item.get("style_label", "")),
        )
        rhythm_score = _rhythm_similarity(user_profile, item)
        match_score = round(semantic_score * 0.6 + style_score * 0.2 + rhythm_score * 0.2, 4)
        enriched = dict(item)
        enriched["style_score"] = round(style_score, 4)
        enriched["rhythm_score"] = round(rhythm_score, 4)
        enriched["match_score"] = match_score
        ranked.append(enriched)
    ranked.sort(key=lambda item: item.get("match_score", 0.0), reverse=True)
    return ranked[: max(1, int(top_k))]


def _issue_alignment_score(marker: Dict, candidate: Dict) -> float:
    issue_type = str(marker.get("issue_type", "")).strip()
    cand_pace = _to_float(candidate.get("pace_wps"), 0.0)
    cand_pause = _to_float(candidate.get("pause_density"), 0.0)
    cand_energy = _to_float(candidate.get("energy_rms"), 0.0)
    if issue_type == "speed-up":
        pace_score = 1.0 - min(1.0, max(0.0, cand_pace - 2.3) / 2.2)
        pause_score = min(1.0, cand_pause / 0.18)
        return round(max(0.0, min(1.0, pace_score * 0.7 + pause_score * 0.3)), 4)
    if issue_type == "pause-too-short":
        return round(max(0.0, min(1.0, cand_pause / 0.18)), 4)
    if issue_type == "low-energy":
        return round(max(0.0, min(1.0, cand_energy / 0.45)), 4)
    if issue_type in {"tone-flat", "falling-intonation"}:
        tone_support = 0.55 + min(0.45, cand_energy * 0.35 + cand_pause * 0.4)
        return round(max(0.0, min(1.0, tone_support)), 4)
    if issue_type == "rhythm-break":
        stable_pace = 1.0 - min(1.0, abs(cand_pace - 2.4) / 2.5)
        stable_pause = 1.0 - min(1.0, abs(cand_pause - 0.12) / 0.2)
        return round(max(0.0, min(1.0, stable_pace * 0.5 + stable_pause * 0.5)), 4)
    return 0.55


def _marker_query_text(marker: Dict) -> str:
    parts = [
        str(marker.get("demo_text", "")).strip(),
        str(marker.get("instruction", "")).strip(),
        str(marker.get("rationale", "")).strip(),
        str(marker.get("issue_type", "")).strip(),
    ]
    return " ".join(part for part in parts if part)


def _score_candidate_for_marker(marker: Dict, candidate: Dict) -> float:
    marker_tokens = _tokenize(_marker_query_text(marker))
    lexical_score = _lexical_similarity(
        marker_tokens,
        str(candidate.get("transcript_excerpt", "")).strip(),
    )
    global_score = _to_float(candidate.get("match_score"), 0.0)
    issue_score = _issue_alignment_score(marker, candidate)
    return round(global_score * 0.55 + lexical_score * 0.25 + issue_score * 0.2, 4)


def _reference_target_count(markers: Optional[List[Dict]], top_k: int) -> int:
    marker_count = len([item for item in (markers or []) if isinstance(item, dict)])
    return max(max(1, int(top_k)), marker_count)


def _assign_candidates_to_markers(
    ranked_candidates: List[Dict],
    markers: Optional[List[Dict]],
    target_count: int,
) -> Tuple[List[Dict], Dict[str, int], int]:
    valid_markers = [item for item in (markers or []) if isinstance(item, dict) and str(item.get("id", "")).strip()]
    if not valid_markers:
        return [dict(item) for item in ranked_candidates[:target_count]], {}, 0

    marker_rankings: Dict[str, List[Dict]] = {}
    coverage_counts: Dict[str, int] = {}
    for marker in valid_markers:
        marker_id = str(marker.get("id", "")).strip()
        scored = []
        for candidate in ranked_candidates:
            enriched = dict(candidate)
            enriched["marker_match_score"] = _score_candidate_for_marker(marker, candidate)
            scored.append(enriched)
        scored.sort(
            key=lambda item: (
                item.get("marker_match_score", 0.0),
                item.get("match_score", 0.0),
            ),
            reverse=True,
        )
        marker_rankings[marker_id] = scored
        coverage_counts[marker_id] = len(scored)

    ordered_markers = sorted(
        valid_markers,
        key=lambda item: float(item.get("severity", 0.0) or 0.0),
        reverse=True,
    )
    selected = []
    used_candidate_ids = set()
    reused_count = 0

    for allow_reuse in (False, True):
        for marker in ordered_markers:
            marker_id = str(marker.get("id", "")).strip()
            if any(str(item.get("primary_marker_id", "")).strip() == marker_id for item in selected):
                continue
            for candidate in marker_rankings.get(marker_id, []):
                candidate_id = str(candidate.get("id", "")).strip()
                if not allow_reuse and candidate_id in used_candidate_ids:
                    continue
                chosen = dict(candidate)
                chosen["marker_ids"] = [marker_id]
                chosen["primary_marker_id"] = marker_id
                chosen["issue_type_hint"] = str(marker.get("issue_type", "")).strip()
                chosen["marker_match_score"] = candidate.get("marker_match_score", 0.0)
                selected.append(chosen)
                if allow_reuse and candidate_id in used_candidate_ids:
                    reused_count += 1
                used_candidate_ids.add(candidate_id)
                break

    for candidate in ranked_candidates:
        if len(selected) >= target_count:
            break
        candidate_id = str(candidate.get("id", "")).strip()
        if candidate_id in used_candidate_ids:
            continue
        selected.append(dict(candidate))
        used_candidate_ids.add(candidate_id)
    return selected[:target_count], coverage_counts, reused_count


def _compute_energy_rms_from_wav(wav_bytes: bytes) -> float:
    if not wav_bytes:
        return 0.0
    try:
        with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            frames = wav_file.readframes(wav_file.getnframes())
            if not frames or sample_width <= 0:
                return 0.0
            if channels > 1:
                frames = audioop.tomono(frames, sample_width, 0.5, 0.5)
            rms = audioop.rms(frames, sample_width)
            full_scale = float(2 ** (8 * sample_width - 1))
            return round(min(1.0, rms / full_scale), 4)
    except Exception:
        return 0.0


def _compute_user_energy(audio_bytes: bytes, audio_filename: str) -> float:
    if not audio_bytes:
        return 0.0
    ext = os.path.splitext(audio_filename or "")[1].lower()
    if ext == ".wav":
        return _compute_energy_rms_from_wav(audio_bytes)
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext or ".bin") as src_file:
            src_file.write(audio_bytes)
            src_path = src_file.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as out_file:
            out_path = out_file.name
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            src_path,
            "-ac",
            "1",
            "-ar",
            "16000",
            out_path,
        ]
        proc = subprocess.run(cmd, capture_output=True, check=False)
        if proc.returncode != 0:
            return 0.0
        with open(out_path, "rb") as wav_file:
            return _compute_energy_rms_from_wav(wav_file.read())
    except Exception:
        return 0.0
    finally:
        try:
            if "src_path" in locals() and os.path.exists(src_path):
                os.remove(src_path)
        except Exception:
            pass
        try:
            if "out_path" in locals() and os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass


def _build_user_profile(
    script: str,
    transcript_segments: List[Dict],
    markers: Optional[List[Dict]],
    style_label: str,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
) -> Dict:
    text = " ".join(
        str(seg.get("text", "")).strip()
        for seg in transcript_segments
        if isinstance(seg, dict) and str(seg.get("text", "")).strip()
    )
    words = len(re.findall(r"[A-Za-z0-9']+", text))
    start = min((_to_float(seg.get("start"), 0.0) for seg in transcript_segments), default=0.0)
    end = max((_to_float(seg.get("end"), 0.0) for seg in transcript_segments), default=0.0)
    duration = max(0.1, end - start)
    pause_markers = 0
    if isinstance(markers, list):
        pause_markers = sum(
            1
            for marker in markers
            if str(marker.get("issue_type", "")).strip() in {"pause-too-short", "rhythm-break"}
        )
    punctuation_pauses = len(re.findall(r"[,.!?;:]", text))
    pause_density = (pause_markers + punctuation_pauses) / duration
    energy = _compute_user_energy(audio_bytes=audio_bytes, audio_filename=audio_filename)
    return {
        "style_label": style_label or "general",
        "pace_wps": round(words / duration, 4),
        "pause_density": round(pause_density, 4),
        "energy_rms": round(energy, 4),
        "script": script,
        "transcript": text,
    }




def _clean_line(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _focus_span(text: str, issue_type: str = "") -> str:
    cleaned = _clean_line(text)
    if not cleaned:
        return "this part of the line"
    pieces = re.split(r"[,;:—-]", cleaned)
    candidate = pieces[-1].strip() if pieces else cleaned
    if len(candidate.split()) < 2:
        words = cleaned.split()
        candidate = " ".join(words[-6:])
    words = candidate.split()
    if len(words) > 8:
        candidate = " ".join(words[-8:])
    return candidate.strip() or cleaned


def _coach_watch_text(issue_type: str, demo_span: str) -> str:
    quoted = f'"{demo_span}"' if demo_span and demo_span != "this moment" else "that moment"
    mapping = {
        "pause-too-short": f"Watch what happens just before {quoted}. The comedian gives the turn a little room before landing it.",
        "speed-up": f"Watch how {quoted} stays composed instead of getting rushed.",
        "low-energy": f"Watch which word inside {quoted} quietly becomes the point.",
        "falling-intonation": f"Listen to the finish of {quoted}. The line stays present through the last word.",
        "tone-flat": f"Watch where {quoted} changes shape.",
        "rhythm-break": f"Listen for the steady beat through {quoted}.",
        "unclear-emphasis": f"Watch which word inside {quoted} becomes easiest to hear.",
    }
    return mapping.get(issue_type, f"Watch how the comedian handles {quoted}.")


def _coach_copy_action(issue_type: str, user_span: str) -> str:
    quoted = f'"{user_span}"' if user_span and user_span != "this part of the line" else "that spot"
    mapping = {
        "pause-too-short": f"On your own line, give {quoted} a brief beat of room, then finish cleanly.",
        "speed-up": f"From {quoted} onward, stop trying to get to the end quickly. Let the last thought land in one clean piece.",
        "low-energy": f"Keep the sentence conversational, then let {quoted} carry the point.",
        "falling-intonation": f"Stay with {quoted} through the finish instead of letting the line drop away early.",
        "tone-flat": f"Let the line turn at {quoted} instead of reading straight through it.",
        "rhythm-break": f"Keep one steady beat through {quoted}. Do not brake in the middle and restart.",
        "unclear-emphasis": f"Choose one word inside {quoted} and make that the point the audience hears first.",
    }
    return mapping.get(issue_type, f"Borrow the delivery choice around {quoted}, not the exact wording.")


def _coach_steps(issue_type: str, user_span: str, demo_span: str) -> List[str]:
    uq = f'"{user_span}"' if user_span and user_span != "this part of the line" else "that spot"
    dq = f'"{demo_span}"' if demo_span and demo_span != "this moment" else "that moment"
    if issue_type == "pause-too-short":
        return [
            f"Watch {dq} once and notice the brief pause before the point.",
            f"Now say your line again and give {uq} the same bit of room.",
            "Do three takes and change only the timing into that beat.",
        ]
    if issue_type == "speed-up":
        return [
            f"Listen to how the demo keeps its shape through {dq} instead of hurrying into the finish.",
            f"On your line, begin easing off the gas before {uq} and let the last thought land cleanly.",
            "Do three takes and work only on the last stretch of the line.",
        ]
    if issue_type == "low-energy":
        return [
            f"Watch the demo and find the one word inside {dq} that suddenly matters more than the rest.",
            f"On your line, keep everything easy and let {uq} carry the point.",
            "Do three takes and change only the emphasis, not the volume of the whole sentence.",
        ]
    if issue_type == "falling-intonation":
        return [
            f"Listen to how the demo stays with {dq} all the way through the finish.",
            f"On your line, keep {uq} alive until the sentence is fully done.",
            "Do three takes and work only on the finish of the line.",
        ]
    if issue_type == "tone-flat":
        return [
            f"Watch where the demo line changes shape around {dq}.",
            f"Now say your line again and let {uq} turn instead of reading straight through.",
            "Do three takes and change only the shape of the line.",
        ]
    if issue_type == "rhythm-break":
        return [
            f"Listen for the steady beat through {dq}.",
            f"Now run your line again and keep that same steady beat through {uq}.",
            "Do three takes and work only on keeping the rhythm even.",
        ]
    return [
        f"Watch the demo and find the word inside {dq} that feels like the real point.",
        f"Run your line again and make one word inside {uq} do the work.",
        "Do three takes and change only that one point of emphasis.",
    ]


def _human_reason(issue_type: str) -> str:
    mapping = {
        "pause-too-short": "Rubric focus: Timing and Pacing. This clip is useful because the comic gives the turn a little room before the point lands.",
        "speed-up": "Rubric focus: Timing and Pacing. This clip is useful because the ending stays controlled instead of racing to the finish.",
        "low-energy": "Rubric focus: Vocal Expressiveness. This clip is useful because one word carries the point and the rest supports it.",
        "falling-intonation": "Rubric focus: Confidence and Control. This clip is useful because the finish stays intentional.",
        "tone-flat": "Rubric focus: Vocal Expressiveness. This clip is useful because the line has movement instead of one fixed level.",
        "rhythm-break": "Rubric focus: Timing and Pacing. This clip is useful because the beat stays steady through the thought.",
        "unclear-emphasis": "Rubric focus: Clarity and Articulation. This clip is useful because the key word becomes unmistakable.",
    }
    return mapping.get(issue_type, "Rubric focus: performance control. Use this clip as a cleaner example of the same delivery job.")


def _comparison_line(item: Dict, issue_type: str) -> str:
    if issue_type == "pause-too-short":
        return "The useful thing here is not a huge dramatic pause. It is the small bit of space before the point."
    if issue_type == "speed-up":
        return "Notice how the ending does not get tossed away. The line stays composed right through the finish."
    if issue_type == "low-energy":
        return "Notice that the whole sentence does not get louder. One part sharpens and the rest gives it room."
    if issue_type == "falling-intonation":
        return "Listen to the finish. The line stays present until it is actually over."
    if issue_type == "tone-flat":
        return "What matters here is the change in shape inside the line, not imitation of the exact voice."
    if issue_type == "rhythm-break":
        return "The middle of the line keeps moving in one beat instead of stalling and restarting."
    return "Take this as a cleaner delivery model and borrow the way the point becomes easier to hear."

def _watch_hint(issue_types: Iterable[str]) -> str:
    issue_set = {str(item).strip() for item in issue_types if str(item).strip()}
    if "speed-up" in issue_set or "pause-too-short" in issue_set:
        return "Watch where the comedian lets the line breathe before the point."
    if "low-energy" in issue_set or "unclear-emphasis" in issue_set:
        return "Watch which word suddenly becomes the point of the line."
    if "tone-flat" in issue_set or "falling-intonation" in issue_set:
        return "Listen to how the line changes shape at the end."
    return "Watch how the comedian keeps the thought clear without forcing it."


def _build_reference_record(item: Dict, style_label: str, issue_types: Iterable[str], marker: Optional[Dict] = None) -> Dict:
    primary_issue = str(item.get("issue_type_hint", "")).strip() or str((marker or {}).get("issue_type", "")).strip()
    base_name = os.path.basename(str(item.get("video_path", "")))
    asset_id = int(_to_float(item.get("asset_id"), 0))
    clip_start_sec = max(0.0, _to_float(item.get("start_sec"), 0.0))
    clip_end_sec = max(clip_start_sec + 0.1, _to_float(item.get("end_sec"), clip_start_sec + 0.1))
    candidate_title = str(item.get("title", "")).strip() or str(((item.get("payload", {}) if isinstance(item.get("payload"), dict) else {}) or {}).get("title", "")).strip()
    preview_url = None
    if asset_id > 0:
        preview_url = (
            f"/api/video-dataset/preview?asset_id={asset_id}"
            f"&start_sec={clip_start_sec}&end_sec={clip_end_sec}"
        )
    playable_source_url = (
        f"/api/video-dataset/source-file/{asset_id}?start_sec={clip_start_sec}&end_sec={clip_end_sec}"
        if asset_id > 0
        else str(item.get("source_url", "")).strip()
    )
    watch_url = str(item.get("watch_url", "")).strip()
    if not watch_url and playable_source_url and re.match(r"^https?://", playable_source_url, re.I):
        watch_url = playable_source_url
        playable_source_url = ""
    marker_ids = list(item.get("marker_ids", []) or [])
    primary_marker_id = str(item.get("primary_marker_id", "")).strip() or (marker_ids[0] if marker_ids else None)
    user_span = _focus_span(str((marker or {}).get("demo_text", "")), primary_issue)
    demo_span = _focus_span(str(item.get("transcript_excerpt", "")), primary_issue) if str(item.get("transcript_excerpt", "")).strip() else "this moment"
    steps = _coach_steps(primary_issue, user_span, demo_span)
    transferability = item.get("transferability_summary", {}) if isinstance(item.get("transferability_summary"), dict) else {}
    reason_text = (
        str(item.get("reason", "")).strip()
        or str(transferability.get("why_this_clip", "")).strip()
        or _human_reason(primary_issue)
    )
    watch_text = (
        str(item.get("watch_hint", "")).strip()
        or str(transferability.get("what_to_watch", "")).strip()
        or _coach_watch_text(primary_issue, demo_span)
    )
    copy_text = (
        str(item.get("copy_action", "")).strip()
        or str(transferability.get("adaptation_guidance", "")).strip()
        or _coach_copy_action(primary_issue, user_span)
    )
    title_base = candidate_title or base_name or str(item.get("video_id", "")).strip() or "Stand-up reference"
    return {
        "title": f"{title_base} [{clip_start_sec:.1f}s-{clip_end_sec:.1f}s]",
        "reference_title": candidate_title,
        "video_path": item.get("video_path", ""),
        "asset_id": asset_id,
        "start_sec": clip_start_sec,
        "end_sec": clip_end_sec,
        "preview_url": preview_url,
        "source_url": playable_source_url,
        "watch_url": watch_url,
        "learn_goal": reason_text,
        "reason": reason_text,
        "watch_hint": watch_text,
        "copy_action": copy_text,
        "imitation_steps": steps,
        "rehearsal_drill": steps[-1],
        "comparison": _comparison_line(item, primary_issue),
        "user_focus_span": user_span,
        "demo_focus_span": demo_span,
        "match_score": item.get("match_score", 0.0),
        "style_score": item.get("style_score", 0.0),
        "rhythm_score": item.get("rhythm_score", 0.0),
        "marker_match_score": item.get("marker_match_score", 0.0),
        "performer_name": str(_resolve_candidate_performer(item).get("performer_name", "")).strip(),
        "language": str(item.get("language", "")).strip(),
        "channel": str(item.get("channel", "")).strip(),
        "video_id": str(item.get("video_id", "")).strip(),
        "source_kind": str(item.get("source_kind", "")).strip(),
        "marker_ids": marker_ids,
        "primary_marker_id": primary_marker_id,
        "issue_type_hint": primary_issue,
        "transferability_rationale": str(transferability.get("transferability_rationale", "")).strip(),
        "portability_notes": str(transferability.get("portability_notes", "")).strip(),
    }




def _resolve_candidate_performer(item: Dict) -> Dict:
    return resolve_video_metadata(
        video_path=str(item.get("video_path", "")),
        performer_id=str(item.get("performer_id", "")),
        video_id=str(item.get("video_id", "")),
        title=str(item.get("title", "")),
        channel=str(item.get("channel", "")),
        performer_name=str(item.get("performer_name", "")),
    )


def _metadata_from_match(match) -> Dict:
    if isinstance(match, dict):
        return dict(match.get("metadata", {}) or {})
    metadata = getattr(match, "metadata", None)
    return dict(metadata or {})


def _score_from_match(match) -> float:
    if isinstance(match, dict):
        return _to_float(match.get("score"), 0.0)
    return _to_float(getattr(match, "score", 0.0), 0.0)


def _infer_performer_name_from_path(video_path: str) -> str:
    profile = resolve_video_metadata(video_path=str(video_path or ""))
    return str(profile.get("performer_name", "")).strip()


def _tokenize(text: str) -> set:
    return {
        token
        for token in re.findall(r"[a-z0-9']+", (text or "").lower())
        if len(token) > 1
    }


def _lexical_similarity(query_tokens: set, text: str) -> float:
    if not query_tokens:
        return 0.0
    text_tokens = _tokenize(text)
    if not text_tokens:
        return 0.0
    overlap = query_tokens.intersection(text_tokens)
    union = query_tokens.union(text_tokens)
    if not union:
        return 0.0
    return round(len(overlap) / len(union), 4)


def _load_candidates_from_db(query_text: str, limit: int) -> List[Dict]:
    query_tokens = _tokenize(query_text)
    db = None
    try:
        db = get_session()
        rows = (
            db.query(VideoChunk, VideoAsset)
            .join(VideoAsset, VideoChunk.asset_id == VideoAsset.id)
            .filter(VideoAsset.ingest_status == "ready")
            .order_by(VideoChunk.updated_at.desc())
            .limit(max(30, int(limit) * 8))
            .all()
        )
    except Exception:
        if db is not None:
            db.close()
        logger.exception("load candidates from db failed")
        return []

    try:
        items = []
        for chunk, asset in rows:
            file_path = str(asset.file_path or "").strip()
            if not file_path:
                continue
            excerpt = str(chunk.transcript or "").strip()
            lexical = _lexical_similarity(query_tokens, excerpt)
            semantic_score = max(0.1, min(1.0, lexical + 0.15))
            items.append(
                {
                    "id": f"db-{chunk.id}",
                    "asset_id": int(asset.id or 0),
                    "semantic_score": semantic_score,
                    "style_label": str(chunk.style_label or "general"),
                    "pace_wps": _to_float(chunk.pace_wps, 0.0),
                    "pause_density": _to_float(chunk.pause_density, 0.0),
                    "energy_rms": _to_float(chunk.energy_rms, 0.0),
                    "video_path": file_path,
                    "start_sec": round(_to_float(chunk.start_sec, 0.0), 3),
                    "end_sec": round(_to_float(chunk.end_sec, 0.0), 3),
                    "transcript_excerpt": excerpt,
                    "performer_name": str(resolve_video_metadata(video_path=file_path).get("performer_name", "")).strip(),
                }
            )
        items.sort(key=lambda item: item.get("semantic_score", 0.0), reverse=True)
        logger.info("db fallback candidates loaded: %s", len(items))
        return items[: max(1, int(limit))]
    finally:
        db.close()




def match_comedian_profiles(
    script: str,
    transcript_segments: List[Dict],
    markers: Optional[List[Dict]],
    style_label: str,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
    limit: int = 6,
) -> List[Dict]:
    user_profile = _build_user_profile(
        script=script,
        transcript_segments=transcript_segments,
        markers=markers or [],
        style_label=style_label,
        audio_bytes=audio_bytes,
        audio_filename=audio_filename,
    )
    db = None
    try:
        db = get_session()
        rows = (
            db.query(VideoChunk, VideoAsset)
            .join(VideoAsset, VideoChunk.asset_id == VideoAsset.id)
            .filter(VideoAsset.ingest_status == "ready")
            .all()
        )
    except Exception:
        if db is not None:
            db.close()
        logger.exception("comedian profile load failed")
        return []

    try:
        grouped: Dict[str, List[Dict]] = {}
        for chunk, asset in rows:
            profile = resolve_video_metadata(video_path=str(asset.file_path or ""))
            performer = str(profile.get("performer_name", "")).strip()
            if not performer:
                continue
            grouped.setdefault(performer, []).append({
                "pace_wps": _to_float(chunk.pace_wps, 0.0),
                "pause_density": _to_float(chunk.pause_density, 0.0),
                "energy_rms": _to_float(chunk.energy_rms, 0.0),
                "style_label": str(chunk.style_label or "general").strip() or "general",
            })

        issues = [m for m in (markers or []) if isinstance(m, dict)]
        results = []
        for performer, clips in grouped.items():
            if not clips:
                continue
            avg_pace = sum(c["pace_wps"] for c in clips) / len(clips)
            avg_pause = sum(c["pause_density"] for c in clips) / len(clips)
            avg_energy = sum(c["energy_rms"] for c in clips) / len(clips)
            style_counts: Dict[str, int] = {}
            for clip in clips:
                style_counts[clip["style_label"]] = style_counts.get(clip["style_label"], 0) + 1
            dominant_style = sorted(style_counts.items(), key=lambda item: item[1], reverse=True)[0][0] if style_counts else "general"
            prototype = {
                "pace_wps": avg_pace,
                "pause_density": avg_pause,
                "energy_rms": avg_energy,
                "style_label": dominant_style,
            }
            style_score = _style_similarity(str(user_profile.get("style_label", "")), dominant_style)
            rhythm_score = _rhythm_similarity(user_profile, prototype)
            issue_score = 0.5
            if issues:
                issue_score = sum(_issue_alignment_score(marker, prototype) for marker in issues) / len(issues)
            similarity = round(style_score * 0.25 + rhythm_score * 0.55 + issue_score * 0.20, 4)
            results.append({
                "name": performer,
                "performer_id": performer,
                "title": "",
                "channel": "",
                "similarity": similarity,
                "style_score": round(style_score, 4),
                "rhythm_score": round(rhythm_score, 4),
                "reference_count": len(clips),
                "marker_ids": [str(m.get("id", "")).strip() for m in issues if str(m.get("id", "")).strip()][:3],
                "style_summary": f"Closest on pace, pause shape, and energy contour to {performer}.",
                "ai_note": "This match is based on delivery shape, not on joke topic or wording.",
            })
        results.sort(key=lambda item: item.get("similarity", 0.0), reverse=True)
        return results[: max(1, int(limit))]
    finally:
        db.close()

def match_video_references(
    script: str,
    transcript_segments: List[Dict],
    markers: Optional[List[Dict]],
    style_label: str,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
    issue_types: Optional[List[str]] = None,
    top_k: int = 3,
    initial_top_k: int = 20,
) -> List[Dict]:
    settings = Settings()

    transcript_text = " ".join(
        str(seg.get("text", "")).strip()
        for seg in transcript_segments
        if isinstance(seg, dict)
    ).strip()
    if not transcript_text:
        logger.info("video match skipped: empty transcript")
        return []

    user_profile = _build_user_profile(
        script=script,
        transcript_segments=transcript_segments,
        markers=markers or [],
        style_label=style_label,
        audio_bytes=audio_bytes,
        audio_filename=audio_filename,
    )
    query_text = f"{script}\n{transcript_text}\nstyle:{style_label}"
    result = {}
    if settings.pinecone_api_key:
        try:
            vec = embed_text(query_text)
            pc = ensure_indexes()
            index = pc.Index(settings.pinecone_index_video_clips)
            result = index.query(
                vector=vec,
                top_k=max(1, int(initial_top_k)),
                include_metadata=True,
            )
        except Exception:
            logger.exception("pinecone query failed, fallback to db candidates")
            result = {}

    raw_matches = []
    if isinstance(result, dict):
        raw_matches = result.get("matches", []) or []
    else:
        raw_matches = getattr(result, "matches", []) or []

    candidates = []
    for match in raw_matches:
        meta = _metadata_from_match(match)
        file_path = str(meta.get("file_path", "")).strip()
        if not file_path:
            continue
        asset_id = int(_to_float(meta.get("asset_id"), 0))
        start_sec = round(_to_float(meta.get("start_sec"), 0.0), 3)
        end_sec = round(_to_float(meta.get("end_sec"), start_sec), 3)
        candidates.append(
            {
                "id": str(meta.get("chunk_id", "")) or str(meta.get("asset_id", "")),
                "asset_id": asset_id,
                "semantic_score": _score_from_match(match),
                "style_label": str(meta.get("style_label", "general")),
                "pace_wps": _to_float(meta.get("pace_wps"), 0.0),
                "pause_density": _to_float(meta.get("pause_density"), 0.0),
                "energy_rms": _to_float(meta.get("energy_rms"), 0.0),
                "video_path": file_path,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "transcript_excerpt": str(meta.get("transcript_excerpt", "")).strip(),
                "performer_name": str(resolve_video_metadata(
                    video_path=file_path,
                    performer_id=str(meta.get("performer_id", "")),
                    video_id=str(meta.get("video_id", "")),
                    title=str(meta.get("title", "")),
                    channel=str(meta.get("channel", "")),
                    performer_name=str(meta.get("performer_name", "")),
                ).get("performer_name", "")).strip(),
            }
        )

    needed_candidates = max(int(top_k), int(initial_top_k))
    if len(candidates) < needed_candidates:
        existing_ids = {str(item.get("id", "")) for item in candidates}
        for fallback_item in _load_candidates_from_db(query_text=query_text, limit=needed_candidates):
            fallback_id = str(fallback_item.get("id", ""))
            if fallback_id in existing_ids:
                continue
            candidates.append(fallback_item)
            existing_ids.add(fallback_id)
            if len(candidates) >= needed_candidates:
                break

    if not candidates:
        logger.warning("video match empty: no candidates from pinecone or db fallback")
        return []

    target_count = _reference_target_count(markers=markers, top_k=top_k)
    ranked_pool_size = max(target_count, min(len(candidates), max(int(initial_top_k), target_count * 3)))
    ranked_candidates = rank_video_candidates(
        user_profile=user_profile,
        candidates=candidates,
        top_k=ranked_pool_size,
    )
    assigned_candidates, coverage_counts, reused_count = _assign_candidates_to_markers(
        ranked_candidates=ranked_candidates,
        markers=markers,
        target_count=target_count,
    )
    refs = []
    marker_lookup = {str(marker.get("id", "")).strip(): marker for marker in (markers or []) if isinstance(marker, dict) and str(marker.get("id", "")).strip()}
    for item in assigned_candidates:
        marker_issue_types = []
        if str(item.get("issue_type_hint", "")).strip():
            marker_issue_types.append(str(item.get("issue_type_hint", "")).strip())
        marker = marker_lookup.get(str(item.get("primary_marker_id", "")).strip())
        refs.append(
            _build_reference_record(
                item=item,
                style_label=style_label,
                issue_types=marker_issue_types or issue_types or [],
                marker=marker,
            )
        )
    marker_summary = ", ".join(
        f"{marker_id}:{count}"
        for marker_id, count in sorted(coverage_counts.items())
    )
    logger.info(
        "video match produced refs=%s candidates=%s markers=%s reused=%s style=%s coverage=%s",
        len(refs),
        len(candidates),
        len([item for item in (markers or []) if isinstance(item, dict)]),
        reused_count,
        style_label or "general",
        marker_summary or "-",
    )
    return refs



def _focus_note_query_text(note: Dict, utterance: Dict, local_context: str = "") -> str:
    parts = [
        str(utterance.get("text", "")).strip(),
        str(note.get("comedy_function", "")).strip(),
        str(note.get("focus_type", "")).strip(),
        str(note.get("advice", "")).strip(),
        str(note.get("why", "")).strip(),
        local_context.strip(),
    ]
    return " ".join(part for part in parts if part)



def _function_alignment_score(note: Dict, candidate: Dict) -> float:
    focus_type = str(note.get("focus_type", "")).strip().lower()
    comedy_function = str(note.get("comedy_function", "")).strip().lower()
    excerpt = str(candidate.get("transcript_excerpt", "")).strip().lower()
    if not excerpt:
        return 0.45
    score = 0.45
    if comedy_function == "punch":
        if any(token in excerpt for token in ["then", "so", "suddenly", "just", "and i", "i was like"]):
            score += 0.2
        if any(token in excerpt for token in ["pause", "beat", "laugh", "turn"]):
            score += 0.15
    if comedy_function == "pivot":
        if any(token in excerpt for token in ["but", "actually", "except", "turns out", "instead"]):
            score += 0.25
    if focus_type == "release":
        if any(token in excerpt for token in ["pause", "beat", "reveal", "surprise"]):
            score += 0.15
    if focus_type == "build":
        if any(token in excerpt for token in ["because", "when", "used to", "thought", "always"]):
            score += 0.15
    return round(max(0.0, min(1.0, score)), 4)



def _score_candidate_for_focus_note(note: Dict, utterance: Dict, candidate: Dict) -> float:
    query_tokens = _tokenize(_focus_note_query_text(note, utterance, utterance.get("context_before", "")))
    lexical = _lexical_similarity(query_tokens, str(candidate.get("transcript_excerpt", "")).strip())
    global_score = _to_float(candidate.get("match_score"), 0.0)
    function_score = _function_alignment_score(note, candidate)
    return round(global_score * 0.55 + lexical * 0.2 + function_score * 0.25, 4)



def _focus_issue_hint(note: Dict, utterance: Dict) -> str:
    tags = {
        str(tag).strip()
        for tag in list(note.get("delivery_tags", []) or []) + list(utterance.get("delivery_tags", []) or [])
        if str(tag).strip()
    }
    if "rushed_release" in tags or "rushed_build" in tags:
        return "speed-up"
    if "weak_release" in tags:
        return "pause-too-short"
    if "flat_shape" in tags:
        return "tone-flat"
    if "weak_emphasis" in tags:
        return "unclear-emphasis"
    focus_type = str(note.get("focus_type", "")).strip()
    comedy_function = str(note.get("comedy_function", "")).strip()
    if focus_type == "release" or comedy_function in {"punch", "callback", "tag"}:
        return "pause-too-short"
    if focus_type == "build":
        return "rhythm-break"
    if focus_type == "turn" or comedy_function == "pivot":
        return "rhythm-break"
    if focus_type == "shape":
        return "tone-flat"
    return "unclear-emphasis"


def _comedy_function_similarity(left: str, right: str) -> float:
    lhs = str(left or "").strip().lower()
    rhs = str(right or "").strip().lower()
    if not lhs or not rhs:
        return 0.45
    if lhs == rhs:
        return 1.0
    release_group = {"punch", "callback", "tag"}
    build_group = {"setup", "misdirect", "bridge"}
    if lhs in release_group and rhs in release_group:
        return 0.88
    if lhs == "pivot" and rhs in build_group.union({"pivot"}):
        return 0.8
    if lhs in build_group and rhs in build_group.union({"pivot"}):
        return 0.8
    return 0.32


def _focus_type_similarity(left: str, right: str) -> float:
    lhs = str(left or "").strip().lower()
    rhs = str(right or "").strip().lower()
    if not lhs or not rhs:
        return 0.45
    if lhs == rhs:
        return 1.0
    related = {
        "release": {"release", "tag"},
        "tag": {"tag", "release"},
        "turn": {"turn", "build"},
        "build": {"build", "turn"},
        "shape": {"shape", "release", "turn"},
    }
    if rhs in related.get(lhs, {lhs}):
        return 0.84
    return 0.35


def _structured_delivery_similarity(utterance: Dict, candidate: Dict) -> float:
    audio_features = utterance.get("audio_features", {}) if isinstance(utterance, dict) else {}
    user_pace = _to_float(audio_features.get("words_per_second"), 0.0)
    user_pause = _to_float(audio_features.get("pause_before"), _to_float(utterance.get("gap_before"), 0.0))
    user_energy = _to_float(audio_features.get("rms_level"), 0.0)
    cand_pace = _to_float(candidate.get("pace_wps"), 0.0)
    cand_pause = max(_to_float(candidate.get("pause_before_sec"), 0.0), _to_float(candidate.get("pause_density"), 0.0))
    cand_energy = _to_float(candidate.get("energy_rms"), 0.0)

    pace_diff = min(1.0, abs(user_pace - cand_pace) / 3.0)
    pause_diff = min(1.0, abs(user_pause - cand_pause) / 0.28)
    energy_diff = min(1.0, abs(user_energy - cand_energy) / 0.65)
    return round(max(0.0, 1.0 - (pace_diff * 0.45 + pause_diff * 0.35 + energy_diff * 0.2)), 4)


def _structured_issue_support_score(issue_type: str, candidate: Dict) -> float:
    pause_value = max(_to_float(candidate.get("pause_before_sec"), 0.0), _to_float(candidate.get("pause_density"), 0.0))
    proxy_candidate = {
        "pace_wps": _to_float(candidate.get("pace_wps"), 0.0),
        "pause_density": pause_value,
        "energy_rms": _to_float(candidate.get("energy_rms"), 0.0),
    }
    base = _issue_alignment_score({"issue_type": issue_type}, proxy_candidate)
    if issue_type == "pause-too-short":
        base = max(base, min(1.0, pause_value / 0.22))
    elif issue_type == "speed-up":
        base = max(base, 1.0 - min(1.0, max(0.0, _to_float(candidate.get("pace_wps"), 0.0) - 2.4) / 2.2))
    elif issue_type == "tone-flat":
        base = max(base, min(1.0, _to_float(candidate.get("energy_rms"), 0.0) / 0.35 + 0.1))
    elif issue_type == "unclear-emphasis":
        base = max(base, min(1.0, _to_float(candidate.get("laughter_score"), 0.0) + 0.15))

    tags = {str(tag).strip() for tag in (candidate.get("delivery_tags", []) or []) if str(tag).strip()}
    penalty = 0.0
    if issue_type in {"pause-too-short", "speed-up"} and tags.intersection({"weak_release", "rushed_release", "rushed_build"}):
        penalty = 0.18
    elif issue_type in {"tone-flat", "unclear-emphasis"} and tags.intersection({"flat_shape", "weak_emphasis"}):
        penalty = 0.18
    return round(max(0.0, min(1.0, base - penalty)), 4)


def _structured_note_target(note: Dict, utterance: Dict, style_label: str) -> Dict:
    return {
        "query_text": _focus_note_query_text(note, utterance, utterance.get("context_before", "")),
        "comedy_function": str(note.get("comedy_function", utterance.get("comedy_function", "other"))).strip(),
        "focus_type": str(note.get("focus_type", "")).strip(),
        "issue_hint": _focus_issue_hint(note, utterance),
        "style_label": style_label,
    }


def _retrieval_structuring_model() -> str:
    return str(os.getenv("OPENAI_RETRIEVAL_STRUCTURING_MODEL", "gpt-4o")).strip() or "gpt-4o"


def _retrieval_screening_model() -> str:
    return str(os.getenv("OPENAI_RETRIEVAL_SCREENING_MODEL", "gpt-4o")).strip() or "gpt-4o"


def _retrieval_verification_model() -> str:
    return str(os.getenv("OPENAI_TRANSFERABILITY_MODEL", "gpt-5.2")).strip() or "gpt-5.2"


def _llm_reasoning_enabled() -> bool:
    if str(os.getenv("PYTEST_CURRENT_TEST", "")).strip():
        raw = str(os.getenv("ENABLE_LLM_RETRIEVAL_REASONING", "")).strip().lower()
        return raw in {"1", "true", "yes", "on"}
    return bool(str(Settings().openai_api_key or "").strip())


def _delivery_evidence_summary(note: Dict, utterance: Dict, target: Dict) -> str:
    audio = utterance.get("audio_features", {}) if isinstance(utterance.get("audio_features"), dict) else {}
    parts = [
        f"issue_hint={str(target.get('issue_hint', '')).strip() or 'unknown'}",
        f"comedy_function={str(target.get('comedy_function', '')).strip() or 'other'}",
        f"focus_type={str(target.get('focus_type', '')).strip() or 'shape'}",
        f"pace_wps={_to_float(audio.get('words_per_second'), 0.0):.3f}",
        f"pause_before_sec={_to_float(audio.get('pause_before'), _to_float(utterance.get('gap_before'), 0.0)):.3f}",
        f"energy_rms={_to_float(audio.get('rms_level'), 0.0):.3f}",
        f"laugh_bearing={_to_float(utterance.get('laugh_bearing_score'), 0.0):.3f}",
        f"supporting={_to_float(utterance.get('supporting_score'), 0.0):.3f}",
    ]
    tags = [str(tag).strip() for tag in (note.get("delivery_tags", []) or utterance.get("delivery_tags", []) or []) if str(tag).strip()]
    if tags:
        parts.append(f"delivery_tags={', '.join(tags)}")
    return "; ".join(parts)


def _pedagogical_target_payload(note: Dict, utterance: Dict, target: Dict) -> Dict:
    return {
        "focal_span": str(note.get("quote", "")).strip() or str(utterance.get("text", "")).strip(),
        "context_before": str(utterance.get("context_before", "")).strip(),
        "context_after": str(utterance.get("context_after", "")).strip(),
        "delivery_issue": str(target.get("issue_hint", "")).strip(),
        "bit_function": str(target.get("comedy_function", "")).strip() or str(utterance.get("comedy_function", "")).strip(),
        "delivery_evidence_summary": _delivery_evidence_summary(note, utterance, target),
        "query_text": str(target.get("query_text", "")).strip(),
        "focus_type": str(target.get("focus_type", "")).strip(),
    }


def _fallback_pedagogical_spec(note: Dict, utterance: Dict, target: Dict) -> Dict:
    issue_hint = str(target.get("issue_hint", "")).strip() or "unclear-emphasis"
    function_name = str(target.get("comedy_function", "")).strip() or "other"
    focus_type = str(target.get("focus_type", "")).strip() or "shape"
    positive_delivery = {
        "pause-too-short": ["audible pause before the turn", "clean separation into the reveal", "payoff lands as a distinct beat"],
        "speed-up": ["steady pacing through the final phrase", "controlled release instead of acceleration", "clear ending without rush"],
        "low-energy": ["one word carries more intent", "energy sharpens near the point", "delivery remains legible but more alive"],
        "tone-flat": ["clear vocal shape change", "contrast inside the line", "attitude shifts are easy to hear"],
        "falling-intonation": ["finish stays present", "line remains alive through the last word", "ending does not collapse early"],
        "rhythm-break": ["beat remains stable", "transition feels intentional", "no stall-and-restart in the middle"],
        "unclear-emphasis": ["one keyword becomes easiest to hear", "stress pattern points to the joke", "the point is unmistakable"],
    }.get(issue_hint, ["delivery logic is easy to hear", "performance move is visible", "moment can be studied without topic overlap"])
    negative_constraints = {
        "pause-too-short": ["candidate is funny mainly because of topical wording", "candidate works only because of crowd work", "candidate is another setup instead of a release"],
        "speed-up": ["candidate itself rushes the finish", "candidate depends on shouting instead of control", "candidate hides the point in dense wording"],
        "low-energy": ["candidate relies only on volume", "candidate is funny only because of the topic", "candidate does not show where the point sharpens"],
        "tone-flat": ["candidate has no audible shape change", "candidate depends on persona-specific aggression", "candidate performs the same function but with a flat read"],
        "falling-intonation": ["candidate trails off early", "candidate only works because of room context", "candidate's finish is not clearly demonstrated"],
        "rhythm-break": ["candidate contains a stall in the same place", "candidate resets into a new setup", "candidate cannot be studied as one coherent beat"],
        "unclear-emphasis": ["candidate buries the keyword", "candidate is intelligible only through transcript content", "candidate lacks a clearly teachable emphasis move"],
    }.get(issue_hint, ["candidate lacks a visible teaching moment", "candidate depends too heavily on topic match", "candidate does not solve the same delivery problem"])
    return {
        "moment_function": {
            "label": function_name,
            "confidence": round(max(0.35, _to_float(utterance.get("function_confidence"), 0.55)), 3),
            "justification": f"The current coaching target behaves like a {focus_type or function_name} moment inside the bit.",
        },
        "delivery_failure": {
            "label": issue_hint,
            "mechanism": str(note.get("why", "")).strip() or "The current delivery obscures the local joke function.",
            "why_it_hurts_the_bit": str(note.get("advice", "")).strip() or "The audience can hear the line, but not the intended performance move.",
        },
        "target_demonstration": {
            "core_move": str(note.get("advice", "")).strip() or "Show a clearer reusable delivery move for this moment.",
            "observable_cues": positive_delivery[:3],
            "adaptation_goal": str(note.get("try_next", "")).strip() or "Borrow the performance logic, not the wording.",
        },
        "positive_constraints": {
            "required_functional_properties": [
                f"candidate should serve a compatible {function_name or 'comedy'} function",
                f"candidate should support a {focus_type or 'delivery'} teaching moment",
                "candidate should remain studyable even when the topic differs",
            ],
            "required_delivery_properties": positive_delivery[:3],
            "acceptable_topic_distance": "topic may differ substantially if the same reusable performance logic is visible",
        },
        "negative_constraints": {
            "misleading_if": negative_constraints[:3],
            "reject_even_if_semantically_similar": negative_constraints[:3],
        },
        "semantic_seed_query": str(target.get("query_text", "")).strip(),
        "retrieval_rationale": "Prefer clips whose delivery move is reusable for the same coaching problem, not clips that merely talk about similar subject matter.",
    }


def _fallback_screen_candidate(pedagogical_spec: Dict, target: Dict, utterance: Dict, candidate: Dict) -> Dict:
    function_score = _comedy_function_similarity(target.get("comedy_function", ""), candidate.get("comedy_function", ""))
    focus_score = _focus_type_similarity(target.get("focus_type", ""), candidate.get("focus_type", ""))
    delivery_score = _structured_delivery_similarity(utterance, candidate)
    issue_score = _structured_issue_support_score(target.get("issue_hint", ""), candidate)
    quality_score = _to_float(candidate.get("quality_score"), 0.0)
    laughter_score = _to_float(candidate.get("laughter_score"), 0.0)
    token_count = len(_tokenize(str(candidate.get("transcript_excerpt", "")).strip()))

    functional_pass = function_score >= 0.58 or (function_score >= 0.45 and focus_score >= 0.55)
    demonstration_pass = issue_score >= 0.48 or (issue_score >= 0.4 and delivery_score >= 0.58)
    visibility_pass = token_count >= 3 and max(quality_score, laughter_score) >= 0.2

    tags = {str(tag).strip() for tag in (candidate.get("delivery_tags", []) or []) if str(tag).strip()}
    issue_hint = str(target.get("issue_hint", "")).strip()
    transfer_risk_pass = True
    transfer_reason = "The clip looks portable enough to study."
    if issue_hint in {"pause-too-short", "speed-up"} and tags.intersection({"weak_release", "rushed_release", "rushed_build"}):
        transfer_risk_pass = False
        transfer_reason = "The candidate exhibits a similar weak release pattern, so it risks teaching the same failure."
    if issue_hint in {"tone-flat", "unclear-emphasis"} and tags.intersection({"flat_shape", "weak_emphasis"}):
        transfer_risk_pass = False
        transfer_reason = "The candidate still hides the move that the performer needs to observe."

    decision = "keep" if all((functional_pass, demonstration_pass, visibility_pass, transfer_risk_pass)) else "reject"
    return {
        "candidate_id": str(candidate.get("id", "")).strip(),
        "hard_gates": {
            "functional_alignment": {
                "pass": functional_pass,
                "reason": f"function={function_score:.2f}, focus={focus_score:.2f}",
            },
            "demonstration_alignment": {
                "pass": demonstration_pass,
                "reason": f"issue_support={issue_score:.2f}, delivery={delivery_score:.2f}",
            },
            "pedagogical_visibility": {
                "pass": visibility_pass,
                "reason": f"quality={quality_score:.2f}, laughter={laughter_score:.2f}, tokens={token_count}",
            },
            "transfer_risk": {
                "pass": transfer_risk_pass,
                "reason": transfer_reason,
            },
        },
        "comparative_assessment": {
            "function_match_strength": "strong" if function_score >= 0.8 else ("partial" if function_score >= 0.55 else "weak"),
            "delivery_match_strength": "strong" if issue_score >= 0.75 else ("partial" if issue_score >= 0.5 else "weak"),
            "teachable_move_clarity": "strong" if max(quality_score, laughter_score) >= 0.65 else ("partial" if max(quality_score, laughter_score) >= 0.35 else "weak"),
        },
        "failure_mode_if_used": "" if decision == "keep" else "This clip would likely be studied for the wrong reason or would fail to show the needed move clearly enough.",
        "screening_decision": decision,
        "screening_rationale": str(pedagogical_spec.get("retrieval_rationale", "")).strip() or "Deterministic compatibility screen.",
    }


def _candidate_passes_llm_screening(summary: Dict) -> bool:
    if not isinstance(summary, dict):
        return False
    decision = str(summary.get("screening_decision", "")).strip().lower()
    if decision == "keep":
        return True
    if decision == "reject":
        return False
    hard_gates = summary.get("hard_gates", {}) if isinstance(summary.get("hard_gates"), dict) else {}
    return all(bool(((hard_gates.get(key, {}) if isinstance(hard_gates.get(key), dict) else {}).get("pass"))) for key in (
        "functional_alignment",
        "demonstration_alignment",
        "pedagogical_visibility",
        "transfer_risk",
    ))


def _screen_focus_note_candidates(
    note: Dict,
    utterance: Dict,
    target: Dict,
    pedagogical_spec: Dict,
    candidates: List[Dict],
) -> List[Dict]:
    screened = []
    use_llm = _llm_reasoning_enabled()
    for idx, candidate in enumerate(candidates):
        fallback_summary = _fallback_screen_candidate(pedagogical_spec, target, utterance, candidate)
        if not _candidate_passes_llm_screening(fallback_summary):
            continue
        summary = fallback_summary
        if use_llm and idx < 6:
            try:
                llm_summary = screen_pedagogical_candidate(
                    pedagogical_spec=pedagogical_spec,
                    candidate=candidate,
                    model=_retrieval_screening_model(),
                )
                if _candidate_passes_llm_screening(llm_summary):
                    summary = llm_summary
                elif isinstance(llm_summary, dict) and llm_summary:
                    continue
            except Exception:
                logger.exception("focus-note screening failed for candidate=%s", candidate.get("id"))
        enriched = dict(candidate)
        enriched["screening_summary"] = summary
        screened.append(enriched)
    return screened


def _choose_focus_note_candidates(
    candidates: List[Dict],
    pedagogical_spec: Dict,
    top_k: int,
) -> List[Dict]:
    if not candidates:
        return []
    ordered = list(candidates)
    use_llm = _llm_reasoning_enabled()
    if use_llm:
        try:
            adjudication = adjudicate_transferable_candidate(
                pedagogical_spec=pedagogical_spec,
                candidates=ordered[: max(2, min(len(ordered), max(4, int(top_k) * 3)))],
                model=_retrieval_verification_model(),
            )
        except Exception:
            logger.exception("transferability adjudication failed")
            adjudication = {}
        selected_id = str(adjudication.get("selected_candidate_id", "")).strip()
        if selected_id:
            for idx, candidate in enumerate(ordered):
                if str(candidate.get("id", "")).strip() != selected_id:
                    continue
                chosen = dict(candidate)
                chosen["transferability_summary"] = adjudication
                ordered.pop(idx)
                ordered.insert(0, chosen)
                break
    return ordered[: max(1, int(top_k))]


def _score_structured_candidate_for_focus_note(target: Dict, utterance: Dict, candidate: Dict) -> Dict:
    function_score = _comedy_function_similarity(target.get("comedy_function", ""), candidate.get("comedy_function", ""))
    focus_score = _focus_type_similarity(target.get("focus_type", ""), candidate.get("focus_type", ""))
    style_score = _style_similarity(target.get("style_label", ""), candidate.get("style_label", ""))
    delivery_score = _structured_delivery_similarity(utterance, candidate)
    issue_score = _structured_issue_support_score(target.get("issue_hint", ""), candidate)
    laugh_score = max(_to_float(candidate.get("laughter_score"), 0.0), _to_float(candidate.get("quality_score"), 0.0) * 0.8)
    semantic_score = _lexical_similarity(
        _tokenize(target.get("query_text", "")),
        str(candidate.get("match_text") or candidate.get("transcript_excerpt", "")).strip(),
    )
    quality_score = _to_float(candidate.get("quality_score"), 0.0)

    final_score = round(
        function_score * 0.28
        + focus_score * 0.14
        + style_score * 0.12
        + delivery_score * 0.12
        + issue_score * 0.14
        + laugh_score * 0.10
        + quality_score * 0.05
        + semantic_score * 0.05,
        4,
    )
    if function_score < 0.5 and focus_score < 0.5:
        final_score = round(final_score * 0.45, 4)

    enriched = dict(candidate)
    enriched["semantic_score"] = round(semantic_score, 4)
    enriched["style_score"] = round((style_score + function_score + focus_score) / 3.0, 4)
    enriched["rhythm_score"] = round(delivery_score, 4)
    enriched["marker_match_score"] = round(issue_score, 4)
    enriched["match_score"] = round(final_score, 4)
    enriched["focus_match_score"] = round(final_score, 4)
    enriched["issue_type_hint"] = str(target.get("issue_hint", "")).strip()
    return enriched


def _fetch_reference_candidates(query_text: str, initial_top_k: int) -> List[Dict]:
    settings = Settings()
    result = {}
    if settings.pinecone_api_key:
        try:
            vec = embed_text(query_text)
            pc = ensure_indexes()
            index = pc.Index(settings.pinecone_index_video_clips)
            result = index.query(
                vector=vec,
                top_k=max(1, int(initial_top_k)),
                include_metadata=True,
            )
        except Exception:
            logger.exception("pinecone query failed for focus note, fallback to db candidates")
            result = {}
    raw_matches = []
    if isinstance(result, dict):
        raw_matches = result.get("matches", []) or []
    else:
        raw_matches = getattr(result, "matches", []) or []
    candidates = []
    for match in raw_matches:
        meta = _metadata_from_match(match)
        file_path = str(meta.get("file_path", "")).strip()
        if not file_path:
            continue
        asset_id = int(_to_float(meta.get("asset_id"), 0))
        start_sec = round(_to_float(meta.get("start_sec"), 0.0), 3)
        end_sec = round(_to_float(meta.get("end_sec"), start_sec), 3)
        candidates.append(
            {
                "id": str(meta.get("chunk_id", "")) or str(meta.get("asset_id", "")),
                "asset_id": asset_id,
                "semantic_score": _score_from_match(match),
                "style_label": str(meta.get("style_label", "general")),
                "pace_wps": _to_float(meta.get("pace_wps"), 0.0),
                "pause_density": _to_float(meta.get("pause_density"), 0.0),
                "energy_rms": _to_float(meta.get("energy_rms"), 0.0),
                "video_path": file_path,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "transcript_excerpt": str(meta.get("transcript_excerpt", "")).strip(),
                "performer_name": str(resolve_video_metadata(
                    video_path=file_path,
                    performer_id=str(meta.get("performer_id", "")),
                    video_id=str(meta.get("video_id", "")),
                    title=str(meta.get("title", "")),
                    channel=str(meta.get("channel", "")),
                    performer_name=str(meta.get("performer_name", "")),
                ).get("performer_name", "")).strip(),
            }
        )
    needed = max(1, int(initial_top_k))
    if len(candidates) < needed:
        existing = {str(item.get("id", "")) for item in candidates}
        for fallback_item in _load_candidates_from_db(query_text=query_text, limit=needed):
            fallback_id = str(fallback_item.get("id", ""))
            if fallback_id in existing:
                continue
            candidates.append(fallback_item)
            existing.add(fallback_id)
            if len(candidates) >= needed:
                break
    return candidates



def match_focus_note_videos(
    script: str,
    utterances: List[Dict],
    focus_notes: List[Dict],
    style_label: str,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
    top_k: int = 1,
    initial_top_k: int = 18,
) -> List[Dict]:
    if not focus_notes or not utterances:
        return []
    transcript_segments = []
    for utt in utterances:
        start, end = utt.get("time_range", [0.0, 0.0])
        transcript_segments.append({
            "start": _to_float(start, 0.0),
            "end": _to_float(end, 0.0),
            "text": str(utt.get("text", "")).strip(),
        })
    user_profile = _build_user_profile(
        script=script,
        transcript_segments=transcript_segments,
        markers=[],
        style_label=style_label,
        audio_bytes=audio_bytes,
        audio_filename=audio_filename,
    )
    utterance_lookup = {str(utt.get("id", "")).strip(): utt for utt in utterances}
    refs_by_note = []
    for note in focus_notes:
        utterance = utterance_lookup.get(str(note.get("utterance_id", "")).strip())
        if not utterance:
            continue
        target = _structured_note_target(note, utterance, style_label)
        query_text = str(target.get("query_text", "")).strip()
        dataset_candidates = load_dataset_reference_spans(
            comedy_function=str(target.get("comedy_function", "")).strip(),
            focus_type=str(target.get("focus_type", "")).strip(),
            limit=max(20, int(initial_top_k) * 5),
        )
        rescored = []
        if dataset_candidates:
            for candidate in dataset_candidates:
                rescored.append(_score_structured_candidate_for_focus_note(target, utterance, candidate))
        else:
            structured_candidates = load_structured_video_spans(
                comedy_function=str(target.get("comedy_function", "")).strip(),
                focus_type=str(target.get("focus_type", "")).strip(),
                limit=max(18, int(initial_top_k) * 4),
            )
            if structured_candidates:
                for candidate in structured_candidates:
                    rescored.append(_score_structured_candidate_for_focus_note(target, utterance, candidate))
            else:
                candidates = _fetch_reference_candidates(query_text=query_text, initial_top_k=initial_top_k)
                if not candidates:
                    refs_by_note.append({"note_id": note.get("id"), "utterance_id": utterance.get("id"), "items": []})
                    continue
                ranked_pool = rank_video_candidates(user_profile=user_profile, candidates=candidates, top_k=max(top_k * 4, top_k))
                for item in ranked_pool:
                    enriched = dict(item)
                    enriched["focus_match_score"] = _score_candidate_for_focus_note(note, utterance, item)
                    enriched["issue_type_hint"] = target.get("issue_hint", "")
                    rescored.append(enriched)

        rescored.sort(
            key=lambda item: (
                item.get("focus_match_score", 0.0),
                item.get("match_score", 0.0),
                item.get("quality_score", 0.0),
                item.get("laughter_score", 0.0),
            ),
            reverse=True,
        )
        pedagogical_spec = _fallback_pedagogical_spec(note, utterance, target)
        if _llm_reasoning_enabled():
            try:
                llm_spec = generate_pedagogical_retrieval_spec(
                    target=_pedagogical_target_payload(note, utterance, target),
                    model=_retrieval_structuring_model(),
                )
                if isinstance(llm_spec, dict) and llm_spec:
                    pedagogical_spec = llm_spec
            except Exception:
                logger.exception("pedagogical abstraction failed for note=%s", note.get("id"))
        screened_candidates = _screen_focus_note_candidates(
            note=note,
            utterance=utterance,
            target=target,
            pedagogical_spec=pedagogical_spec,
            candidates=rescored[: max(6, min(len(rescored), max(10, int(initial_top_k))))],
        )
        ranked_candidates = screened_candidates or rescored
        chosen_candidates = _choose_focus_note_candidates(
            candidates=ranked_candidates,
            pedagogical_spec=pedagogical_spec,
            top_k=top_k,
        )
        selected = []
        used_candidate_ids = set()
        issue_hint = str(target.get("issue_hint", "")).strip() or "unclear-emphasis"
        for item in chosen_candidates:
            candidate_id = str(item.get("id", "")).strip()
            if candidate_id and candidate_id in used_candidate_ids:
                continue
            pseudo_marker = {
                "id": f"focus-{note.get('id')}",
                "demo_text": utterance.get("text", ""),
                "issue_type": issue_hint,
            }
            enriched = dict(item)
            enriched["issue_type_hint"] = issue_hint
            record = _build_reference_record(
                item=enriched,
                style_label=style_label,
                issue_types=[issue_hint],
                marker=pseudo_marker,
            )
            record["matched_function"] = str(note.get("comedy_function", "")).strip()
            record["matched_reason"] = (
                f"Supports the {str(note.get('focus_type', '')).strip() or 'delivery'} moment with a stronger {str(item.get('comedy_function', '')).strip() or 'reference'} span and cleaner laugh landing."
            )
            record["supports_advice"] = str(note.get("advice", "")).strip()
            if isinstance(item.get("transferability_summary"), dict):
                transfer = item.get("transferability_summary", {})
                if str(transfer.get("transferability_rationale", "")).strip():
                    record["matched_reason"] = str(transfer.get("transferability_rationale", "")).strip()
            if item.get("payload"):
                payload = item.get("payload", {}) if isinstance(item.get("payload"), dict) else {}
                record["reference_title"] = str(payload.get("title", "")).strip()
                record["reference_why"] = str(payload.get("why", "")).strip()
            selected.append(record)
            if candidate_id:
                used_candidate_ids.add(candidate_id)
            if len(selected) >= max(1, top_k):
                break
        refs_by_note.append(
            {
                "note_id": str(note.get("id", "")).strip(),
                "utterance_id": str(utterance.get("id", "")).strip(),
                "items": selected,
            }
        )
    return refs_by_note
