import csv
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

_VIDEO_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")


def _strict_video_id_hint(text: str) -> bool:
    clean = str(text or "").strip()
    return bool(re.search(r"[0-9_]", clean) or re.search(r"[A-Z]", clean))


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def candidate_catalog_paths() -> List[Path]:
    root = _project_root()
    candidates = [
        root / "dataset" / "CSV_clean" / "StandUp4AI_v1.csv",
        root / "external" / "standup4ai-dataset" / "CSV_clean" / "StandUp4AI_v1.csv",
    ]
    explicit_roots = str(os.getenv("VIDEO_DATASET_LABEL_ROOTS", "") or "").strip()
    if explicit_roots:
        for item in re.split(r"[;,]", explicit_roots):
            value = str(item or "").strip()
            if not value:
                continue
            path = Path(value).expanduser()
            if path.is_file() and path.suffix.lower() == ".csv":
                candidates.append(path)
            else:
                candidates.append(path / "CSV_clean" / "StandUp4AI_v1.csv")
    unique: List[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def looks_like_video_id(text: str) -> bool:
    clean = str(text or "").strip()
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", clean)) and _strict_video_id_hint(clean)


def extract_video_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(?:v=|youtu\.be/|/)([A-Za-z0-9_-]{11})(?:[?&#/]|$)", text)
    if match:
        return match.group(1)
    cleaned = text.replace("\\", "/")
    candidates = [text] + [part for part in cleaned.split("/") if part]
    path = Path(text)
    candidates.extend([path.name, path.stem, path.parent.name])
    for candidate in candidates:
        candidate = str(candidate or "").strip()
        if not candidate:
            continue
        if looks_like_video_id(candidate):
            return candidate
        match = _VIDEO_ID_RE.search(candidate)
        if match and looks_like_video_id(match.group(0)):
            return match.group(0)
    return ""


def parse_performer_from_title(title: str) -> str:
    text = _normalize_space(title)
    if not text:
        return ""
    tokens = [
        token.strip(" \t\n\r\"“”'")
        for token in re.split(r"\s+[\-–—]\s+", text)
        if token.strip()
    ]
    if len(tokens) >= 2:
        tail = tokens[-1].lower()
        if "stand-up" in tail or "stand up" in tail or "standup" in tail:
            candidate = _normalize_space(tokens[-2])
            if candidate and not looks_like_video_id(candidate):
                return candidate
    match = re.search(
        r"[\-–—]\s*([^\-–—]+?)\s*[\-–—]\s*stand\s*[- ]?up",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        candidate = _normalize_space(match.group(1))
        if candidate and not looks_like_video_id(candidate):
            return candidate
    return ""


@lru_cache(maxsize=1)
def load_video_catalog() -> Dict[str, Dict]:
    catalog: Dict[str, Dict] = {}
    for csv_path in candidate_catalog_paths():
        if not csv_path.exists():
            continue
        try:
            with csv_path.open("r", encoding="utf-8", errors="ignore") as file_obj:
                reader = csv.DictReader(file_obj)
                for row in reader:
                    url = _normalize_space(row.get("url", ""))
                    video_id = extract_video_id(url)
                    if not video_id:
                        continue
                    title = _normalize_space(row.get("title", ""))
                    channel = _normalize_space(row.get("channel", ""))
                    catalog[video_id] = {
                        "video_id": video_id,
                        "title": title,
                        "channel": channel,
                        "performer_name": parse_performer_from_title(title),
                        "url": url,
                        "language": _normalize_space(row.get("lang", "")),
                        "region": _normalize_space(row.get("region", "")),
                    }
        except Exception:
            continue
    return catalog


def resolve_video_metadata(
    *,
    video_path: str = "",
    performer_id: str = "",
    video_id: str = "",
    title: str = "",
    channel: str = "",
    performer_name: str = "",
) -> Dict:
    resolved_video_id = (
        extract_video_id(video_id)
        or extract_video_id(performer_id)
        or extract_video_id(video_path)
        or extract_video_id(title)
    )
    catalog_entry = load_video_catalog().get(resolved_video_id, {}) if resolved_video_id else {}

    raw_performer = _normalize_space(performer_name)
    if looks_like_video_id(raw_performer):
        raw_performer = ""

    resolved_title = _normalize_space(title) or _normalize_space(catalog_entry.get("title", ""))
    resolved_channel = _normalize_space(channel) or _normalize_space(catalog_entry.get("channel", ""))
    resolved_language = _normalize_space(catalog_entry.get("language", ""))
    parent_name = _normalize_space(Path(str(video_path or "")).parent.name)
    if looks_like_video_id(parent_name):
        parent_name = ""

    resolved_performer = (
        raw_performer
        or _normalize_space(catalog_entry.get("performer_name", ""))
        or parse_performer_from_title(resolved_title)
        or parent_name
    )

    return {
        "video_id": resolved_video_id,
        "title": resolved_title,
        "channel": resolved_channel,
        "language": resolved_language,
        "performer_name": resolved_performer,
    }
