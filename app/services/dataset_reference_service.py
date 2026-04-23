import csv
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from app.config import Settings
from app.db import get_session
from app.models import DatasetReferenceSpan, VideoAsset
from app.services.rehearsal_service import analyze_rehearsal_take
from app.services.video_catalog_service import extract_video_id, load_video_catalog

logger = logging.getLogger(__name__)

_WEAK_DELIVERY_TAGS = {"weak_build", "weak_release", "rushed_build", "rushed_release", "flat_shape", "weak_emphasis"}


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _json_dump(payload) -> str:
    try:
        return json.dumps(payload, ensure_ascii=True)
    except Exception:
        return "[]" if isinstance(payload, list) else "{}"


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
    roots.extend(
        [
            project_root / "dataset",
            dataset_root.parent / "dataset",
            project_root.parent / "dataset",
        ]
    )

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


def list_dataset_label_files(settings: Optional[Settings] = None) -> List[Path]:
    effective_settings = settings or Settings()
    files: List[Path] = []
    for root in _candidate_label_roots(effective_settings):
        label_dir = root / "Examples_label"
        if not label_dir.is_dir():
            continue
        files.extend(sorted(path for path in label_dir.glob("*.csv") if path.is_file()))
    if int(getattr(effective_settings, "video_dataset_max_files_for_test", 0) or 0) > 0:
        return files[: int(effective_settings.video_dataset_max_files_for_test)]
    return files


def _parse_timestamp_pair(raw_value: str) -> Tuple[float, float]:
    matches = re.findall(r"-?\d+(?:\.\d+)?", str(raw_value or ""))
    if len(matches) < 2:
        return 0.0, 0.0
    start = _to_float(matches[0], 0.0)
    end = _to_float(matches[1], start)
    if end < start:
        start, end = end, start
    return round(start, 3), round(end, 3)


def _load_label_tokens(path: Path) -> List[Dict]:
    tokens: List[Dict] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                text = _normalize_space(row.get("text", ""))
                start_sec, end_sec = _parse_timestamp_pair(row.get("timestamp", ""))
                if not text or end_sec <= start_sec:
                    continue
                tokens.append(
                    {
                        "text": text,
                        "start": start_sec,
                        "end": end_sec,
                        "label": str(row.get("label", "")).strip().upper() or "O",
                    }
                )
    except Exception:
        logger.exception("failed to load dataset labels from %s", path)
        return []
    return tokens


def _join_token_texts(tokens: Sequence[Dict]) -> str:
    text = " ".join(str(item.get("text", "")).strip() for item in tokens if str(item.get("text", "")).strip())
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"\s+\.\.\.", "...", text)
    return _normalize_space(text)


def _build_transcript_segments(tokens: Sequence[Dict]) -> List[Dict]:
    if not tokens:
        return []
    segments: List[Dict] = []
    current: List[Dict] = []
    for token in tokens:
        if not current:
            current = [token]
            continue
        previous = current[-1]
        gap = max(0.0, _to_float(token.get("start"), 0.0) - _to_float(previous.get("end"), 0.0))
        duration = _to_float(token.get("end"), 0.0) - _to_float(current[0].get("start"), 0.0)
        should_split = (
            gap >= 1.0
            or len(current) >= 16
            or duration >= 7.5
            or re.search(r"[.!?…]$|\.\.\.$", str(previous.get("text", "")))
        )
        if should_split:
            segments.append(
                {
                    "start": round(_to_float(current[0].get("start"), 0.0), 3),
                    "end": round(_to_float(current[-1].get("end"), 0.0), 3),
                    "text": _join_token_texts(current),
                }
            )
            current = [token]
            continue
        current.append(token)

    if current:
        segments.append(
            {
                "start": round(_to_float(current[0].get("start"), 0.0), 3),
                "end": round(_to_float(current[-1].get("end"), 0.0), 3),
                "text": _join_token_texts(current),
            }
        )
    return [item for item in segments if str(item.get("text", "")).strip()]


def _build_laughter_clusters(tokens: Sequence[Dict], max_gap_sec: float = 1.15) -> List[Dict]:
    laughter_tokens = [item for item in tokens if str(item.get("label", "")).startswith("L")]
    if not laughter_tokens:
        return []

    clusters: List[List[Dict]] = [[laughter_tokens[0]]]
    for token in laughter_tokens[1:]:
        previous = clusters[-1][-1]
        gap = max(0.0, _to_float(token.get("start"), 0.0) - _to_float(previous.get("end"), 0.0))
        if gap <= max_gap_sec:
            clusters[-1].append(token)
            continue
        clusters.append([token])

    results = []
    for cluster in clusters:
        start_sec = min(_to_float(item.get("start"), 0.0) for item in cluster)
        end_sec = max(_to_float(item.get("end"), start_sec) for item in cluster)
        results.append(
            {
                "start_sec": round(start_sec, 3),
                "end_sec": round(end_sec, 3),
                "duration_sec": round(max(0.0, end_sec - start_sec), 3),
                "token_count": len(cluster),
                "text": _join_token_texts(cluster),
            }
        )
    return results


def _select_window_segments(transcript_segments: Sequence[Dict], cluster: Dict) -> List[Dict]:
    if not transcript_segments:
        return []
    start_bound = max(0.0, _to_float(cluster.get("start_sec"), 0.0) - 8.0)
    end_bound = _to_float(cluster.get("end_sec"), 0.0) + 1.8
    selected = [
        segment
        for segment in transcript_segments
        if _to_float(segment.get("end"), 0.0) >= start_bound and _to_float(segment.get("start"), 0.0) <= end_bound
    ]
    if selected:
        return selected[:6]

    ordered = sorted(
        transcript_segments,
        key=lambda item: abs(
            ((_to_float(item.get("start"), 0.0) + _to_float(item.get("end"), 0.0)) / 2.0)
            - ((_to_float(cluster.get("start_sec"), 0.0) + _to_float(cluster.get("end_sec"), 0.0)) / 2.0)
        ),
    )
    fallback = sorted(ordered[:3], key=lambda item: _to_float(item.get("start"), 0.0))
    return fallback


def _window_text(segments: Sequence[Dict]) -> str:
    return _normalize_space(" ".join(str(item.get("text", "")).strip() for item in segments if str(item.get("text", "")).strip()))


def _fallback_note_candidates(analysis: Dict) -> List[Dict]:
    notes = [item for item in (analysis.get("focus_notes", []) or []) if isinstance(item, dict)]
    if notes:
        return notes
    utterances = [item for item in (analysis.get("utterances", []) or []) if isinstance(item, dict)]
    fallback = []
    for utterance in utterances:
        if not utterance.get("is_focus_span"):
            continue
        fallback.append(
            {
                "id": f"fallback-{utterance.get('id', len(fallback) + 1)}",
                "utterance_id": str(utterance.get("id", "")).strip(),
                "comedy_function": str(utterance.get("comedy_function", "other")).strip(),
                "focus_type": {
                    "pivot": "turn",
                    "punch": "release",
                    "tag": "tag",
                    "callback": "release",
                }.get(str(utterance.get("comedy_function", "other")).strip(), "shape"),
                "advice": "Study the delivery shape of this laugh-bearing moment.",
                "why": "This utterance is the strongest stand-up function in the laughter window.",
                "quote": str(utterance.get("text", "")).strip(),
                "delivery_tags": list(utterance.get("delivery_tags", []) or []),
            }
        )
    return fallback


def _note_cluster_relevance(note: Dict, utterance: Dict, cluster: Dict) -> float:
    note_start, note_end = utterance.get("time_range", [0.0, 0.0])
    note_start = _to_float(note_start, 0.0)
    note_end = max(note_start, _to_float(note_end, note_start))
    cluster_start = _to_float(cluster.get("start_sec"), 0.0)
    cluster_end = max(cluster_start, _to_float(cluster.get("end_sec"), cluster_start))
    overlap = max(0.0, min(note_end, cluster_end) - max(note_start, cluster_start))
    delay = max(0.0, cluster_start - note_end)
    center_distance = abs(((note_start + note_end) / 2.0) - ((cluster_start + cluster_end) / 2.0))
    laugh_score = _to_float(utterance.get("laugh_bearing_score"), 0.0)
    return round(overlap * 2.4 + laugh_score * 0.8 - min(1.5, delay) * 0.45 - min(2.0, center_distance) * 0.18, 4)


def _laughter_score(delay_sec: float, duration_sec: float, overlap_sec: float = 0.0) -> float:
    if overlap_sec > 0.05:
        base = 0.92
    elif delay_sec <= 0.25:
        base = 1.0
    elif delay_sec <= 0.75:
        base = 0.9
    elif delay_sec <= 1.3:
        base = 0.76
    elif delay_sec <= 1.9:
        base = 0.58
    else:
        base = 0.36
    duration_bonus = min(0.18, max(0.0, duration_sec) / 3.2)
    return round(min(1.0, base + duration_bonus), 4)


def _issue_hint_from_tags(note: Dict, utterance: Dict) -> str:
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
    if focus_type == "release":
        return "pause-too-short"
    if focus_type in {"turn", "build"}:
        return "rhythm-break"
    return "unclear-emphasis"


def _build_reference_rows_for_file(label_path: Path, catalog_entry: Dict) -> List[Dict]:
    tokens = _load_label_tokens(label_path)
    if not tokens:
        return []
    transcript_segments = _build_transcript_segments(tokens)
    laughter_clusters = _build_laughter_clusters(tokens)
    if not transcript_segments or not laughter_clusters:
        return []

    rows: List[Dict] = []
    video_id = extract_video_id(label_path.stem) or label_path.stem
    title = _normalize_space(catalog_entry.get("title", ""))
    performer_name = _normalize_space(catalog_entry.get("performer_name", ""))
    channel = _normalize_space(catalog_entry.get("channel", ""))
    language = _normalize_space(catalog_entry.get("language", ""))
    source_url = _normalize_space(catalog_entry.get("url", ""))
    label_mtime = _to_float(label_path.stat().st_mtime, 0.0)

    for cluster_idx, cluster in enumerate(laughter_clusters, start=1):
        window_segments = _select_window_segments(transcript_segments, cluster)
        window_script = _window_text(window_segments)
        if not window_script:
            continue
        analysis = analyze_rehearsal_take(
            script=window_script,
            transcript_segments=[dict(item) for item in window_segments],
            style_preset="general",
            disable_llm_enrichment=True,
        )
        utterances = [item for item in (analysis.get("utterances", []) or []) if isinstance(item, dict)]
        utterance_lookup = {str(item.get("id", "")).strip(): item for item in utterances if str(item.get("id", "")).strip()}
        note_candidates = _fallback_note_candidates(analysis)
        if not note_candidates and utterances:
            top_utterance = max(
                utterances,
                key=lambda item: (
                    float(item.get("laugh_bearing_score", 0.0) or 0.0),
                    float(item.get("supporting_score", 0.0) or 0.0),
                ),
            )
            note_candidates = [
                {
                    "id": f"fallback-{cluster_idx}",
                    "utterance_id": str(top_utterance.get("id", "")).strip(),
                    "comedy_function": str(top_utterance.get("comedy_function", "other")).strip(),
                    "focus_type": {
                        "pivot": "turn",
                        "punch": "release",
                        "tag": "tag",
                        "callback": "release",
                    }.get(str(top_utterance.get("comedy_function", "other")).strip(), "shape"),
                    "advice": "Study the delivery shape of this laugh-bearing moment.",
                    "why": "This utterance is the strongest stand-up function in the laughter window.",
                    "quote": str(top_utterance.get("text", "")).strip(),
                    "delivery_tags": list(top_utterance.get("delivery_tags", []) or []),
                }
            ]

        ranked_candidates: List[Tuple[float, Dict, Dict]] = []
        for note in note_candidates:
            utterance = utterance_lookup.get(str(note.get("utterance_id", "")).strip())
            if utterance is None:
                continue
            ranked_candidates.append((_note_cluster_relevance(note, utterance, cluster), note, utterance))

        ranked_candidates.sort(key=lambda item: item[0], reverse=True)
        kept = ranked_candidates[:2] if ranked_candidates else []
        for offset, (_, note, utterance) in enumerate(kept, start=1):
            start_sec, end_sec = utterance.get("time_range", [0.0, 0.0])
            start_sec = _to_float(start_sec, _to_float(cluster.get("start_sec"), 0.0))
            end_sec = max(start_sec + 0.1, _to_float(end_sec, start_sec + 0.1))
            overlap_sec = max(0.0, min(end_sec, _to_float(cluster.get("end_sec"), end_sec)) - max(start_sec, _to_float(cluster.get("start_sec"), start_sec)))
            laugh_delay_sec = max(0.0, _to_float(cluster.get("start_sec"), 0.0) - end_sec)
            laugh_duration_sec = max(0.0, _to_float(cluster.get("duration_sec"), 0.0))
            laughter_score = _laughter_score(laugh_delay_sec, laugh_duration_sec, overlap_sec=overlap_sec)
            comedy_function = str(note.get("comedy_function", utterance.get("comedy_function", "other"))).strip() or "other"
            focus_type = str(note.get("focus_type", "")).strip() or {
                "pivot": "turn",
                "punch": "release",
                "tag": "tag",
                "callback": "release",
            }.get(comedy_function, "shape")
            joke_role = str(utterance.get("joke_role", focus_type)).strip() or focus_type
            function_confidence = _to_float(utterance.get("function_confidence"), 0.55)
            laugh_bearing = _to_float(utterance.get("laugh_bearing_score"), 0.0)
            supporting = _to_float(utterance.get("supporting_score"), 0.0)
            pause_before_sec = _to_float(utterance.get("gap_before"), 0.0)
            transcript_excerpt = _normalize_space(utterance.get("text", note.get("quote", window_script)))
            duration = max(0.1, end_sec - start_sec)
            token_count = len(re.findall(r"[A-Za-z0-9']+", transcript_excerpt))
            pace_wps = round(token_count / duration, 4)
            pause_density = round(len(re.findall(r"[,.!?;:]", transcript_excerpt)) / duration, 4)
            delivery_tags = sorted(
                {
                    str(tag).strip()
                    for tag in list(note.get("delivery_tags", []) or []) + list(utterance.get("delivery_tags", []) or [])
                    if str(tag).strip()
                }
            )
            weak_penalty = 0.14 if any(tag in _WEAK_DELIVERY_TAGS for tag in delivery_tags) else 0.0
            quality_score = round(
                max(
                    0.18,
                    min(
                        1.0,
                        laughter_score * 0.42
                        + function_confidence * 0.22
                        + laugh_bearing * 0.18
                        + supporting * 0.1
                        + min(1.0, pause_before_sec / 0.25) * 0.08
                        - weak_penalty,
                    ),
                ),
                4,
            )
            payload = {
                "title": title,
                "advice": str(note.get("advice", "")).strip(),
                "why": str(note.get("why", "")).strip(),
                "try_next": str(note.get("try_next", "")).strip(),
                "quote": str(note.get("quote", transcript_excerpt)).strip(),
                "channel": channel,
                "language": language,
                "source_url": source_url,
                "issue_hint": _issue_hint_from_tags(note, utterance),
                "cluster_text": _normalize_space(cluster.get("text", "")),
            }
            match_text = "\n".join(
                part
                for part in [
                    transcript_excerpt,
                    str(payload.get("quote", "")).strip(),
                    str(payload.get("advice", "")).strip(),
                    str(payload.get("why", "")).strip(),
                    f"function:{comedy_function}",
                    f"focus:{focus_type}",
                    f"performer:{performer_name}",
                    f"title:{title}",
                    f"language:{language}",
                    f"tags:{' '.join(delivery_tags)}".strip(),
                ]
                if part
            )
            rows.append(
                {
                    "video_id": video_id,
                    "span_idx": cluster_idx * 10 + offset,
                    "label_file": str(label_path),
                    "label_mtime": label_mtime,
                    "source_url": source_url,
                    "title": title,
                    "channel": channel,
                    "performer_name": performer_name,
                    "language": language,
                    "start_sec": round(start_sec, 3),
                    "end_sec": round(end_sec, 3),
                    "transcript": transcript_excerpt,
                    "comedy_function": comedy_function,
                    "focus_type": focus_type,
                    "joke_role": joke_role,
                    "function_confidence": round(function_confidence, 4),
                    "delivery_tags_json": _json_dump(delivery_tags),
                    "quality_score": quality_score,
                    "laughter_score": round(laughter_score, 4),
                    "laugh_start_sec": round(_to_float(cluster.get("start_sec"), 0.0), 3),
                    "laugh_end_sec": round(_to_float(cluster.get("end_sec"), 0.0), 3),
                    "laugh_delay_sec": round(laugh_delay_sec, 3),
                    "laugh_duration_sec": round(laugh_duration_sec, 3),
                    "token_count": int(token_count),
                    "laughter_token_count": int(_to_float(cluster.get("token_count"), 0.0)),
                    "pace_wps": round(pace_wps, 4),
                    "pause_before_sec": round(pause_before_sec, 4),
                    "pause_density": round(pause_density, 4),
                    "energy_rms": 0.0,
                    "style_label": "general",
                    "match_text": match_text[:4000],
                    "payload_json": _json_dump(payload),
                    "source_kind": "dataset-label+heuristic",
                }
            )
    return rows


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


def rebuild_dataset_reference_index(settings: Optional[Settings] = None, force: bool = False) -> Dict:
    effective_settings = settings or Settings()
    label_files = list_dataset_label_files(effective_settings)
    db = None
    try:
        db = get_session()
        if not label_files:
            return {
                "status": "error",
                "processed_files": 0,
                "failed_files": 0,
                "reference_spans": 0,
                "last_error": "no dataset label files found",
            }

        existing_label_files = {str(item[0]) for item in db.query(DatasetReferenceSpan.label_file).distinct().all()}
        force_reindex = force or str(os.getenv("FORCE_VIDEO_REFERENCE_REINDEX", "")).strip().lower() in {"1", "true", "yes", "on"}
        if not force_reindex and existing_label_files and all(str(path) in existing_label_files for path in label_files):
            return {
                "status": "ready",
                "processed_files": db.query(DatasetReferenceSpan.video_id).distinct().count(),
                "failed_files": 0,
                "reference_spans": db.query(DatasetReferenceSpan).count(),
                "last_error": "",
                "reused": True,
            }

        load_video_catalog.cache_clear()
        catalog = load_video_catalog()
        db.query(DatasetReferenceSpan).delete()
        db.commit()

        processed_files = 0
        failed_files = 0
        total_spans = 0
        last_error = ""

        for label_path in label_files:
            video_id = extract_video_id(label_path.stem) or label_path.stem
            catalog_entry = dict(catalog.get(video_id, {}) or {})
            try:
                rows = _build_reference_rows_for_file(label_path, catalog_entry)
                for row in rows:
                    db.add(DatasetReferenceSpan(**row))
                db.commit()
                processed_files += 1
                total_spans += len(rows)
            except Exception as err:
                db.rollback()
                failed_files += 1
                last_error = str(err)
                logger.exception("dataset reference indexing failed for %s", label_path)

        status = "ready" if total_spans > 0 else "error"
        return {
            "status": status,
            "processed_files": processed_files,
            "failed_files": failed_files,
            "reference_spans": total_spans,
            "last_error": last_error,
            "reused": False,
        }
    except Exception as err:
        logger.exception("dataset reference index rebuild failed")
        return {
            "status": "error",
            "processed_files": 0,
            "failed_files": 0,
            "reference_spans": 0,
            "last_error": str(err),
        }
    finally:
        if db is not None:
            db.close()


def _local_asset_lookup(db) -> Dict[str, VideoAsset]:
    items: Dict[str, VideoAsset] = {}
    rows = db.query(VideoAsset).filter(VideoAsset.ingest_status.in_(("ready", "scanning", "pending"))).all()
    for asset in rows:
        video_id = (
            extract_video_id(str(asset.file_path or ""))
            or extract_video_id(str(asset.file_name or ""))
        )
        if video_id and video_id not in items:
            items[video_id] = asset
    return items


def load_dataset_reference_spans(
    *,
    comedy_function: str = "",
    focus_type: str = "",
    limit: int = 0,
) -> List[Dict]:
    db = None
    try:
        db = get_session()
        row_limit = max(240, int(limit or 0) * 14) if int(limit or 0) > 0 else 1400
        rows = (
            db.query(DatasetReferenceSpan)
            .order_by(DatasetReferenceSpan.quality_score.desc(), DatasetReferenceSpan.updated_at.desc())
            .limit(row_limit)
            .all()
        )
        asset_lookup = _local_asset_lookup(db)
    except Exception:
        if db is not None:
            db.close()
        logger.exception("dataset reference span load failed")
        return []

    try:
        wanted_functions = _related_comedy_functions(comedy_function)
        wanted_focus = _related_focus_types(focus_type)
        items = []
        for row in rows:
            if wanted_functions and str(row.comedy_function or "").strip().lower() not in wanted_functions:
                continue
            if wanted_focus and str(row.focus_type or "").strip().lower() not in wanted_focus:
                continue
            payload = _json_load(row.payload_json, {})
            if not isinstance(payload, dict):
                payload = {}
            asset = asset_lookup.get(str(row.video_id or "").strip())
            items.append(
                {
                    "id": f"dataset-ref-{row.id}",
                    "reference_id": int(row.id or 0),
                    "video_id": str(row.video_id or "").strip(),
                    "asset_id": int(getattr(asset, "id", 0) or 0),
                    "video_path": str(getattr(asset, "file_path", "") or ""),
                    "watch_url": str(row.source_url or "").strip(),
                    "source_url": "",
                    "title": str(row.title or "").strip(),
                    "channel": str(row.channel or "").strip(),
                    "performer_name": str(row.performer_name or "").strip(),
                    "language": str(row.language or "").strip(),
                    "start_sec": round(_to_float(row.start_sec, 0.0), 3),
                    "end_sec": round(_to_float(row.end_sec, 0.0), 3),
                    "transcript_excerpt": str(row.transcript or "").strip(),
                    "comedy_function": str(row.comedy_function or "other").strip(),
                    "focus_type": str(row.focus_type or "shape").strip(),
                    "joke_role": str(row.joke_role or "shape").strip(),
                    "function_confidence": round(_to_float(row.function_confidence, 0.0), 4),
                    "delivery_tags": _parse_tag_list(row.delivery_tags_json),
                    "quality_score": round(_to_float(row.quality_score, 0.0), 4),
                    "laughter_score": round(_to_float(row.laughter_score, 0.0), 4),
                    "laugh_start_sec": round(_to_float(row.laugh_start_sec, 0.0), 3),
                    "laugh_end_sec": round(_to_float(row.laugh_end_sec, 0.0), 3),
                    "laugh_delay_sec": round(_to_float(row.laugh_delay_sec, 0.0), 3),
                    "laugh_duration_sec": round(_to_float(row.laugh_duration_sec, 0.0), 3),
                    "pace_wps": round(_to_float(row.pace_wps, 0.0), 4),
                    "pause_before_sec": round(_to_float(row.pause_before_sec, 0.0), 4),
                    "pause_density": round(_to_float(row.pause_density, 0.0), 4),
                    "energy_rms": round(_to_float(row.energy_rms, 0.0), 4),
                    "style_label": str(row.style_label or "general").strip() or "general",
                    "match_text": str(row.match_text or "").strip(),
                    "payload": payload,
                    "source_kind": str(row.source_kind or "dataset-label").strip() or "dataset-label",
                }
            )
        return items
    finally:
        db.close()
