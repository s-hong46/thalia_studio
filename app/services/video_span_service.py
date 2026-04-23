import csv
import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from app.config import Settings
from app.db import get_session
from app.models import VideoAsset, VideoChunk, VideoSpan
from app.services.rehearsal_service import analyze_rehearsal_take
from app.services.video_catalog_service import extract_video_id, resolve_video_metadata

logger = logging.getLogger(__name__)

_WEAK_DELIVERY_TAGS = {"weak_build", "weak_release", "rushed_build", "rushed_release", "flat_shape", "weak_emphasis"}


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _json_dump(payload: Dict | List) -> str:
    try:
        return json.dumps(payload, ensure_ascii=True)
    except Exception:
        return "{}" if isinstance(payload, dict) else "[]"


def _json_load(payload: str, default):
    try:
        return json.loads(str(payload or "").strip() or _json_dump(default))
    except Exception:
        return default


def _parse_tag_list(raw_value: str) -> List[str]:
    loaded = _json_load(raw_value, [])
    if not isinstance(loaded, list):
        return []
    return [str(item).strip() for item in loaded if str(item).strip()]


def _candidate_label_roots(settings: Settings) -> List[Path]:
    roots: List[Path] = []
    explicit = str(getattr(settings, "video_dataset_label_roots", "") or "").strip()
    if explicit:
        for item in re.split(r"[;,]", explicit):
            value = str(item or "").strip()
            if value:
                roots.append(Path(value).expanduser())
    project_root = Path(settings.project_root)
    dataset_root = Path(settings.video_dataset_root)
    roots.append(project_root / "dataset")
    roots.append(dataset_root.parent / "dataset")
    roots.append(project_root.parent / "dataset")

    unique: List[Path] = []
    seen: set[str] = set()
    for root in roots:
        try:
            key = str(root.resolve())
        except Exception:
            key = str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _label_file_for_video_id(video_id: str, settings: Settings) -> Optional[Path]:
    clean = str(video_id or "").strip()
    if not clean:
        return None
    for root in _candidate_label_roots(settings):
        candidate = root / "Examples_label" / f"{clean}.csv"
        if candidate.is_file():
            return candidate
    return None


def _parse_timestamp_pair(raw_value: str) -> Tuple[float, float]:
    matches = re.findall(r"-?\d+(?:\.\d+)?", str(raw_value or ""))
    if len(matches) < 2:
        return 0.0, 0.0
    start = _to_float(matches[0], 0.0)
    end = _to_float(matches[1], start)
    if end < start:
        start, end = end, start
    return round(start, 3), round(end, 3)


def _merge_intervals(intervals: Sequence[Tuple[float, float]], max_gap_sec: float = 0.35) -> List[Tuple[float, float]]:
    ordered = sorted(
        (
            (max(0.0, _to_float(start, 0.0)), max(0.0, _to_float(end, 0.0)))
            for start, end in intervals
            if _to_float(end, 0.0) > _to_float(start, 0.0)
        ),
        key=lambda item: item[0],
    )
    if not ordered:
        return []
    merged = [list(ordered[0])]
    for start, end in ordered[1:]:
        last = merged[-1]
        if start <= last[1] + max_gap_sec:
            last[1] = max(last[1], end)
            continue
        merged.append([start, end])
    return [(round(item[0], 3), round(item[1], 3)) for item in merged]


@lru_cache(maxsize=2048)
def _load_laughter_intervals_from_csv(path_str: str) -> Tuple[Tuple[float, float], ...]:
    path = Path(path_str)
    if not path.is_file():
        return tuple()
    intervals: List[Tuple[float, float]] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                label = str(row.get("label", "")).strip().upper()
                if not label.startswith("L"):
                    continue
                start_sec, end_sec = _parse_timestamp_pair(row.get("timestamp", ""))
                if end_sec <= start_sec:
                    continue
                intervals.append((start_sec, end_sec))
    except Exception:
        logger.exception("failed to load laughter labels from %s", path_str)
        return tuple()
    return tuple(_merge_intervals(intervals))


def load_laughter_intervals(video_path: str, settings: Optional[Settings] = None) -> List[Tuple[float, float]]:
    effective_settings = settings or Settings()
    video_id = extract_video_id(video_path)
    if not video_id:
        return []
    label_file = _label_file_for_video_id(video_id, effective_settings)
    if label_file is None:
        return []
    return list(_load_laughter_intervals_from_csv(str(label_file)))


def _split_transcript_sentences(transcript: str) -> List[str]:
    cleaned = _normalize_space(transcript)
    if not cleaned:
        return []
    raw_parts = re.split(r"(?<=[.!?])\s+|\n+|(?<=[,;:])\s+", cleaned)
    parts: List[str] = []
    for part in raw_parts:
        item = _normalize_space(part)
        if not item:
            continue
        words = item.split()
        if len(words) <= 18:
            parts.append(item)
            continue
        chunk: List[str] = []
        token_count = 0
        for token in words:
            chunk.append(token)
            token_count += 1
            if token_count >= 12 and re.search(r"[,:;.!?]$|\b(?:but|so|then|because|actually|like)\b", token, re.I):
                parts.append(" ".join(chunk).strip())
                chunk = []
                token_count = 0
        if chunk:
            parts.append(" ".join(chunk).strip())
    return parts or [cleaned]


def build_chunk_transcript_segments(transcript: str, start_sec: float, end_sec: float) -> List[Dict]:
    parts = _split_transcript_sentences(transcript)
    if not parts:
        return []
    start = max(0.0, _to_float(start_sec, 0.0))
    end = max(start + 0.2, _to_float(end_sec, start + 0.2))
    total_duration = max(0.6, end - start)
    weights = [max(1, len(re.findall(r"[A-Za-z0-9']+", part))) for part in parts]
    total_weight = max(1, sum(weights))
    cursor = start
    segments = []
    for idx, part in enumerate(parts):
        duration = total_duration * (weights[idx] / float(total_weight))
        segment_end = end if idx == len(parts) - 1 else min(end, cursor + max(0.55, duration))
        if segment_end <= cursor:
            segment_end = min(end, cursor + 0.55)
        segments.append(
            {
                "start": round(cursor, 3),
                "end": round(segment_end, 3),
                "text": part,
            }
        )
        cursor = segment_end
    return segments


def _focus_type_from_function(function_name: str, joke_role: str) -> str:
    role = str(function_name or "").strip()
    if role in {"setup", "misdirect", "bridge"}:
        return "build"
    if role == "pivot":
        return "turn"
    if role in {"punch", "callback"}:
        return "release"
    if role == "tag":
        return "tag"
    return str(joke_role or "").strip() or "shape"


def _fallback_focus_notes(analysis: Dict) -> List[Dict]:
    utterances = [item for item in (analysis.get("utterances", []) or []) if isinstance(item, dict)]
    candidates: List[Dict] = []
    for utt in utterances:
        if not utt.get("is_focus_span") and float(utt.get("laugh_bearing_score", 0.0) or 0.0) < 0.56:
            continue
        function_name = str(utt.get("comedy_function", "other")).strip()
        focus_type = _focus_type_from_function(function_name, str(utt.get("joke_role", "")).strip())
        candidates.append(
            {
                "id": f"note-{str(utt.get('id', '')).strip() or len(candidates) + 1}",
                "utterance_id": str(utt.get("id", "")).strip(),
                "comedy_function": function_name,
                "focus_type": focus_type,
                "advice": "Study the delivery shape of this moment rather than the topic.",
                "why": "This utterance is carrying the main comedy job inside the chunk.",
                "quote": str(utt.get("text", "")).strip(),
                "delivery_tags": list(utt.get("delivery_tags", []) or []),
            }
        )
    if candidates:
        return candidates
    if not utterances:
        return []
    top = max(
        utterances,
        key=lambda item: (
            float(item.get("laugh_bearing_score", 0.0) or 0.0),
            float(item.get("supporting_score", 0.0) or 0.0),
            -int(item.get("index", 9999) or 9999),
        ),
    )
    return [
        {
            "id": f"note-{str(top.get('id', '')).strip() or 'fallback'}",
            "utterance_id": str(top.get("id", "")).strip(),
            "comedy_function": str(top.get("comedy_function", "other")).strip(),
            "focus_type": _focus_type_from_function(str(top.get("comedy_function", "other")).strip(), str(top.get("joke_role", "")).strip()),
            "advice": "Use this line as the primary delivery reference point inside the chunk.",
            "why": "It is the strongest candidate for the clip's comedy turn.",
            "quote": str(top.get("text", "")).strip(),
            "delivery_tags": list(top.get("delivery_tags", []) or []),
        }
    ]


def _laugh_metrics_for_window(start_sec: float, end_sec: float, intervals: Sequence[Tuple[float, float]]) -> Dict:
    start = max(0.0, _to_float(start_sec, 0.0))
    end = max(start, _to_float(end_sec, start))
    overlap_duration = 0.0
    nearest_delay = None
    selected_interval = None
    total_duration = 0.0

    for laugh_start, laugh_end in intervals:
        if laugh_end < start - 0.35 or laugh_start > end + 1.8:
            continue
        laugh_duration = max(0.0, laugh_end - laugh_start)
        total_duration += laugh_duration
        overlap_duration += max(0.0, min(end, laugh_end) - max(start, laugh_start))
        delay = max(0.0, laugh_start - end)
        if selected_interval is None or delay < nearest_delay:
            selected_interval = (laugh_start, laugh_end)
            nearest_delay = delay

    if selected_interval is None:
        return {
            "score": 0.0,
            "laugh_start_sec": 0.0,
            "laugh_end_sec": 0.0,
            "laugh_delay_sec": 0.0,
            "laugh_duration_sec": 0.0,
        }

    laugh_start, laugh_end = selected_interval
    laugh_delay_sec = max(0.0, _to_float(nearest_delay, 0.0))
    score = 0.0
    if overlap_duration > 0.05:
        score = 0.82
    elif laugh_delay_sec <= 0.35:
        score = 1.0
    elif laugh_delay_sec <= 0.85:
        score = 0.92
    elif laugh_delay_sec <= 1.35:
        score = 0.78
    elif laugh_delay_sec <= 1.8:
        score = 0.62
    duration_bonus = min(0.18, total_duration / 3.0)
    return {
        "score": round(min(1.0, score + duration_bonus), 4),
        "laugh_start_sec": round(laugh_start, 3),
        "laugh_end_sec": round(laugh_end, 3),
        "laugh_delay_sec": round(laugh_delay_sec, 3),
        "laugh_duration_sec": round(max(0.0, laugh_end - laugh_start), 3),
    }


def _related_focus_types(value: str) -> set[str]:
    focus_type = str(value or "").strip().lower()
    if not focus_type:
        return set()
    mapping = {
        "release": {"release", "tag"},
        "tag": {"tag", "release"},
        "turn": {"turn", "build"},
        "build": {"build", "turn"},
        "shape": {"shape", "release", "turn"},
    }
    return mapping.get(focus_type, {focus_type})


def _related_comedy_functions(value: str) -> set[str]:
    function_name = str(value or "").strip().lower()
    if not function_name:
        return set()
    mapping = {
        "punch": {"punch", "callback", "tag"},
        "callback": {"callback", "punch", "tag"},
        "tag": {"tag", "punch", "callback"},
        "pivot": {"pivot", "misdirect", "bridge"},
        "setup": {"setup", "misdirect", "bridge"},
        "misdirect": {"misdirect", "setup", "pivot"},
        "bridge": {"bridge", "setup", "pivot"},
    }
    return mapping.get(function_name, {function_name})


def rebuild_chunk_video_spans(
    *,
    db,
    settings: Settings,
    asset: VideoAsset,
    chunk: VideoChunk,
    transcript: str,
    style_label: str,
    pace_wps: float,
    pause_density: float,
    energy_rms: float,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
) -> List[Dict]:
    db.query(VideoSpan).filter_by(chunk_id=chunk.id).delete()
    cleaned_transcript = _normalize_space(transcript)
    if not cleaned_transcript:
        db.commit()
        return []

    transcript_segments = build_chunk_transcript_segments(
        transcript=cleaned_transcript,
        start_sec=_to_float(chunk.start_sec, 0.0),
        end_sec=_to_float(chunk.end_sec, 0.0),
    )
    if not transcript_segments:
        db.commit()
        return []

    analysis = analyze_rehearsal_take(
        script=cleaned_transcript,
        transcript_segments=transcript_segments,
        style_preset=style_label,
        audio_bytes=audio_bytes,
        audio_filename=audio_filename or f"{Path(str(asset.file_name or 'clip')).stem}-chunk-{int(chunk.chunk_idx or 0)}.wav",
        disable_llm_enrichment=True,
    )

    utterances = [item for item in (analysis.get("utterances", []) or []) if isinstance(item, dict)]
    utterance_lookup = {str(item.get("id", "")).strip(): item for item in utterances if str(item.get("id", "")).strip()}
    focus_notes = [item for item in (analysis.get("focus_notes", []) or []) if isinstance(item, dict)] or _fallback_focus_notes(analysis)
    laugh_intervals = [
        interval
        for interval in load_laughter_intervals(str(asset.file_path or ""), settings=settings)
        if interval[1] >= _to_float(chunk.start_sec, 0.0) - 0.35 and interval[0] <= _to_float(chunk.end_sec, 0.0) + 1.8
    ]

    span_rows: List[Dict] = []
    for span_idx, note in enumerate(focus_notes, start=1):
        utterance = utterance_lookup.get(str(note.get("utterance_id", "")).strip())
        if not utterance:
            continue
        time_range = utterance.get("time_range", [chunk.start_sec, chunk.end_sec])
        start_sec = _to_float(time_range[0], _to_float(chunk.start_sec, 0.0))
        end_sec = _to_float(time_range[1], max(start_sec + 0.2, _to_float(chunk.end_sec, start_sec + 0.2)))
        if end_sec <= start_sec:
            end_sec = start_sec + 0.2

        audio_features = utterance.get("audio_features", {}) or {}
        comedy_function = str(note.get("comedy_function", utterance.get("comedy_function", "other"))).strip() or "other"
        focus_type = str(note.get("focus_type", "")).strip() or _focus_type_from_function(comedy_function, str(utterance.get("joke_role", "")).strip())
        joke_role = str(utterance.get("joke_role", "")).strip() or _focus_type_from_function(comedy_function, "")
        delivery_tags = sorted(
            {
                str(tag).strip()
                for tag in list(note.get("delivery_tags", []) or []) + list(utterance.get("delivery_tags", []) or [])
                if str(tag).strip()
            }
        )
        laugh_metrics = _laugh_metrics_for_window(start_sec, end_sec, laugh_intervals)
        weak_penalty = 0.14 if any(tag in _WEAK_DELIVERY_TAGS for tag in delivery_tags) else 0.0
        function_confidence = _to_float(utterance.get("function_confidence", 0.55), 0.55)
        laugh_bearing = _to_float(utterance.get("laugh_bearing_score", 0.0), 0.0)
        supporting = _to_float(utterance.get("supporting_score", 0.0), 0.0)
        quality_score = max(
            0.16,
            min(
                1.0,
                laugh_metrics["score"] * 0.38
                + function_confidence * 0.22
                + laugh_bearing * 0.18
                + supporting * 0.10
                + min(1.0, _to_float(audio_features.get("rms_level"), energy_rms) / 0.38) * 0.12
                - weak_penalty,
            ),
        )

        transcript_excerpt = _normalize_space(utterance.get("text", cleaned_transcript))
        profile = resolve_video_metadata(video_path=str(asset.file_path or ""))
        payload = {
            "title": str(note.get("title", "")).strip(),
            "advice": str(note.get("advice", "")).strip(),
            "why": str(note.get("why", "")).strip(),
            "try_next": str(note.get("try_next", "")).strip(),
            "quote": str(note.get("quote", transcript_excerpt)).strip(),
            "performer_name": str(profile.get("performer_name", "")).strip(),
        }
        match_text = "\n".join(
            part
            for part in [
                transcript_excerpt,
                str(payload.get("advice", "")).strip(),
                str(payload.get("why", "")).strip(),
                f"function:{comedy_function}",
                f"focus:{focus_type}",
                f"style:{style_label or 'general'}",
                f"tags:{' '.join(delivery_tags)}".strip(),
            ]
            if part
        )

        record = VideoSpan(
            asset_id=asset.id,
            chunk_id=chunk.id,
            span_idx=span_idx,
            start_sec=round(start_sec, 3),
            end_sec=round(end_sec, 3),
            transcript=transcript_excerpt,
            comedy_function=comedy_function,
            focus_type=focus_type,
            joke_role=joke_role,
            function_confidence=round(function_confidence, 4),
            delivery_tags_json=_json_dump(delivery_tags),
            quality_score=round(quality_score, 4),
            laughter_score=round(laugh_metrics["score"], 4),
            laugh_start_sec=round(laugh_metrics["laugh_start_sec"], 3),
            laugh_end_sec=round(laugh_metrics["laugh_end_sec"], 3),
            laugh_delay_sec=round(laugh_metrics["laugh_delay_sec"], 3),
            laugh_duration_sec=round(laugh_metrics["laugh_duration_sec"], 3),
            pace_wps=round(_to_float(audio_features.get("words_per_second"), pace_wps), 4),
            pause_before_sec=round(_to_float(audio_features.get("pause_before"), 0.0), 4),
            pause_density=round(_to_float(pause_density, 0.0), 4),
            energy_rms=round(_to_float(audio_features.get("rms_level"), energy_rms), 4),
            style_label=str(style_label or "general").strip() or "general",
            match_text=match_text[:4000],
            payload_json=_json_dump(payload),
            source_kind="label+heuristic" if laugh_metrics["score"] > 0 else "heuristic",
        )
        db.add(record)
        span_rows.append(
            {
                "span_idx": span_idx,
                "start_sec": round(start_sec, 3),
                "end_sec": round(end_sec, 3),
                "transcript": transcript_excerpt,
                "comedy_function": comedy_function,
                "focus_type": focus_type,
                "joke_role": joke_role,
                "delivery_tags": delivery_tags,
                "function_confidence": round(function_confidence, 4),
                "quality_score": round(quality_score, 4),
                "laughter_score": round(laugh_metrics["score"], 4),
                "laugh_delay_sec": round(laugh_metrics["laugh_delay_sec"], 3),
                "payload": payload,
                "performer_name": str(profile.get("performer_name", "")).strip(),
            }
        )

    db.commit()
    return span_rows


def load_structured_video_spans(
    *,
    comedy_function: str = "",
    focus_type: str = "",
    limit: int = 0,
) -> List[Dict]:
    db = None
    try:
        db = get_session()
        row_limit = max(180, int(limit or 0) * 12) if int(limit or 0) > 0 else 1200
        rows = (
            db.query(VideoSpan, VideoAsset)
            .join(VideoAsset, VideoSpan.asset_id == VideoAsset.id)
            .filter(VideoAsset.ingest_status == "ready")
            .order_by(VideoSpan.quality_score.desc(), VideoSpan.updated_at.desc())
            .limit(row_limit)
            .all()
        )
    except Exception:
        if db is not None:
            db.close()
        logger.exception("structured span load failed")
        return []

    try:
        wanted_functions = _related_comedy_functions(comedy_function)
        wanted_focus = _related_focus_types(focus_type)
        items = []
        for span, asset in rows:
            if wanted_functions and str(span.comedy_function or "").strip().lower() not in wanted_functions:
                continue
            if wanted_focus and str(span.focus_type or "").strip().lower() not in wanted_focus:
                continue
            payload = _json_load(span.payload_json, {})
            if not isinstance(payload, dict):
                payload = {}
            profile = resolve_video_metadata(
                video_path=str(asset.file_path or ""),
                performer_name=str(payload.get("performer_name", "")).strip(),
            )
            items.append(
                {
                    "id": f"span-{span.id}",
                    "span_id": int(span.id or 0),
                    "asset_id": int(asset.id or 0),
                    "chunk_id": int(span.chunk_id or 0),
                    "video_path": str(asset.file_path or ""),
                    "start_sec": round(_to_float(span.start_sec, 0.0), 3),
                    "end_sec": round(_to_float(span.end_sec, 0.0), 3),
                    "transcript_excerpt": str(span.transcript or "").strip(),
                    "comedy_function": str(span.comedy_function or "other").strip(),
                    "focus_type": str(span.focus_type or "shape").strip(),
                    "joke_role": str(span.joke_role or "shape").strip(),
                    "function_confidence": round(_to_float(span.function_confidence, 0.0), 4),
                    "delivery_tags": _parse_tag_list(span.delivery_tags_json),
                    "quality_score": round(_to_float(span.quality_score, 0.0), 4),
                    "laughter_score": round(_to_float(span.laughter_score, 0.0), 4),
                    "laugh_start_sec": round(_to_float(span.laugh_start_sec, 0.0), 3),
                    "laugh_end_sec": round(_to_float(span.laugh_end_sec, 0.0), 3),
                    "laugh_delay_sec": round(_to_float(span.laugh_delay_sec, 0.0), 3),
                    "laugh_duration_sec": round(_to_float(span.laugh_duration_sec, 0.0), 3),
                    "pace_wps": round(_to_float(span.pace_wps, 0.0), 4),
                    "pause_before_sec": round(_to_float(span.pause_before_sec, 0.0), 4),
                    "pause_density": round(_to_float(span.pause_density, 0.0), 4),
                    "energy_rms": round(_to_float(span.energy_rms, 0.0), 4),
                    "style_label": str(span.style_label or "general").strip() or "general",
                    "match_text": str(span.match_text or "").strip(),
                    "payload": payload,
                    "source_kind": str(span.source_kind or "heuristic").strip() or "heuristic",
                    "performer_name": str(profile.get("performer_name", "")).strip(),
                }
            )
        return items
    finally:
        db.close()
