import hashlib
import io
import json
import logging
import os
import re
import subprocess
import threading
import time
import wave
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from shutil import which

from app.config import Settings
from app.db import get_session
from app.models import DatasetReferenceSpan, VideoAsset, VideoChunk, VideoSpan
from app.services.audio_compat import audioop
from app.services.asr_service import transcribe_audio_file
from app.services.dataset_reference_service import rebuild_dataset_reference_index
from app.services.embedding_service import classify_style_label, embed_text
from app.services.pinecone_client import ensure_indexes
from app.services.video_span_service import rebuild_chunk_video_spans

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov"}
FEATURE_VERSION = "v1"
logger = logging.getLogger(__name__)

_status_lock = threading.Lock()
_worker_lock = threading.Lock()
_foreground_lock = threading.Lock()
_worker: Optional[threading.Thread] = None
_foreground_requests = 0
_status = {
    "status": "scanning",
    "processed_files": 0,
    "pending_files": 0,
    "failed_files": 0,
    "reference_status": "scanning",
    "reference_files": 0,
    "reference_spans": 0,
    "reference_last_error": "",
    "degraded": False,
    "last_error": "",
    "dataset_root": "",
    "updated_at": 0.0,
}


def auto_video_dataset_ingest_enabled() -> bool:
    raw = str(os.getenv("AUTO_VIDEO_DATASET_INGEST", "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _set_status(**kwargs):
    with _status_lock:
        _status.update(kwargs)
        _status["updated_at"] = time.time()


def get_video_dataset_status_payload() -> Dict:
    with _status_lock:
        return dict(_status)


def initialize_video_dataset_status(settings: Settings):
    default_error = "reference index missing; run python scripts/reindex_dataset_references.py"
    auto_ingest = auto_video_dataset_ingest_enabled()
    payload = {
        "status": "scanning" if auto_ingest else "error",
        "processed_files": 0,
        "pending_files": 0,
        "failed_files": 0,
        "reference_status": "scanning" if auto_ingest else "error",
        "reference_files": 0,
        "reference_spans": 0,
        "reference_last_error": "" if auto_ingest else default_error,
        "degraded": False,
        "last_error": "" if auto_ingest else default_error,
        "dataset_root": str(settings.video_dataset_root),
    }

    if not str(settings.mysql_url or "").strip():
        payload["status"] = "error"
        payload["reference_status"] = "error"
        payload["reference_last_error"] = "MYSQL_URL is not configured"
        payload["last_error"] = "MYSQL_URL is not configured"
        _set_status(**payload)
        return

    db = None
    try:
        db = get_session()
        reference_spans = int(db.query(DatasetReferenceSpan).count() or 0)
        reference_files = int(db.query(DatasetReferenceSpan.label_file).distinct().count() or 0)
        ready_assets = int(db.query(VideoAsset).filter_by(ingest_status="ready").count() or 0)
        failed_assets = int(db.query(VideoAsset).filter_by(ingest_status="error").count() or 0)
        total_assets = int(db.query(VideoAsset).count() or 0)
        pending_assets = max(0, total_assets - ready_assets - failed_assets)

        payload.update(
            {
                "processed_files": ready_assets,
                "pending_files": pending_assets,
                "failed_files": failed_assets,
                "reference_files": reference_files,
                "reference_spans": reference_spans,
            }
        )
        if reference_spans > 0:
            payload["reference_status"] = "ready"
            payload["reference_last_error"] = ""
            payload["last_error"] = ""
        if reference_spans > 0 or ready_assets > 0:
            payload["status"] = "ready" if pending_assets <= 0 else "indexing"
            payload["degraded"] = reference_spans > 0 and ready_assets <= 0
        elif not auto_ingest:
            payload["status"] = "error"
            payload["reference_status"] = "error"
            payload["reference_last_error"] = default_error
            payload["last_error"] = default_error
    except Exception as err:
        logger.exception("video dataset status bootstrap failed")
        payload["status"] = "error"
        payload["reference_status"] = "error"
        payload["reference_last_error"] = str(err)
        payload["last_error"] = str(err)
    finally:
        if db is not None:
            db.close()
    _set_status(**payload)


def begin_foreground_analysis():
    global _foreground_requests
    with _foreground_lock:
        _foreground_requests += 1


def end_foreground_analysis():
    global _foreground_requests
    with _foreground_lock:
        _foreground_requests = max(0, _foreground_requests - 1)


def foreground_analysis_active() -> bool:
    with _foreground_lock:
        return _foreground_requests > 0


def wait_for_foreground_idle(poll_interval_sec: float = 0.25, max_wait_sec: float = 1.0):
    waited = 0.0
    interval = max(0.05, float(poll_interval_sec))
    max_wait = max(0.0, float(max_wait_sec))
    while foreground_analysis_active() and waited < max_wait:
        time.sleep(interval)
        waited += interval


def build_chunk_windows(
    duration_sec: float, chunk_len: float = 30.0, overlap: float = 5.0
) -> List[Tuple[float, float]]:
    duration_sec = max(0.0, float(duration_sec))
    chunk_len = max(1.0, float(chunk_len))
    overlap = max(0.0, min(float(overlap), chunk_len - 0.5))
    if duration_sec <= 0.0:
        return []
    step = max(0.5, chunk_len - overlap)
    windows: List[Tuple[float, float]] = []
    start = 0.0
    while start < duration_sec:
        end = min(duration_sec, start + chunk_len)
        windows.append((round(start, 3), round(end, 3)))
        if end >= duration_sec:
            break
        start += step
    return windows


def _list_video_files(root: str, limit: int = 0) -> List[Path]:
    base = Path(root)
    if not base.exists():
        return []
    files = []
    for path in base.rglob("*"):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            files.append(path)
    files = sorted(files)
    if int(limit or 0) > 0:
        return files[: int(limit)]
    return files




def _candidate_roots_from_settings(settings: Settings) -> List[Path]:
    roots: List[Path] = []
    raw = str(getattr(settings, "video_dataset_root", "") or "").strip()
    if raw:
        roots.append(Path(raw))
    project_root = Path(settings.project_root)
    roots.append(project_root / "movies")
    roots.append(project_root / "dataset")
    seen: set[str] = set()
    unique: List[Path] = []
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


def _find_video_file_by_name(file_name: str, settings: Settings) -> Optional[Path]:
    clean = str(file_name or "").strip()
    if not clean:
        return None
    for root in _candidate_roots_from_settings(settings):
        try:
            if not root.exists():
                continue
            for path in root.rglob(clean):
                if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
                    return path.resolve()
        except Exception:
            continue
    return None


def resolve_asset_file_path(asset: VideoAsset, settings: Settings, db=None) -> Optional[Path]:
    file_path = Path(str(asset.file_path or "").strip()) if str(asset.file_path or "").strip() else None
    if file_path and file_path.exists():
        return file_path
    recovered = _find_video_file_by_name(str(asset.file_name or "").strip(), settings)
    if recovered is None:
        return None
    if db is not None:
        try:
            asset.file_path = str(recovered)
            asset.file_name = recovered.name
            db.commit()
        except Exception:
            db.rollback()
    return recovered

def _probe_duration(path: Path) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            return 0.0
        return max(0.0, float(proc.stdout.strip() or 0.0))
    except Exception:
        return 0.0


def _extract_audio_segment_bytes(path: Path, start_sec: float, end_sec: float) -> bytes:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        str(max(0.0, start_sec)),
        "-to",
        str(max(start_sec, end_sec)),
        "-i",
        str(path),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-f",
        "wav",
        "pipe:1",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, check=False)
        if proc.returncode != 0:
            return b""
        return proc.stdout or b""
    except Exception:
        return b""


def _extract_words(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", text or ""))


def _compute_pause_density(text: str, duration: float) -> float:
    duration = max(0.1, duration)
    pauses = len(re.findall(r"[,.!?;:]", text or ""))
    return round(pauses / duration, 4)


def _compute_energy_rms(wav_bytes: bytes) -> float:
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


def _cache_key(path: Path, mtime: float, chunk_idx: int) -> str:
    raw = f"{path.as_posix()}|{mtime}|{chunk_idx}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _cache_path(settings: Settings, key: str) -> Path:
    directory = Path(settings.video_dataset_cache_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{key}.json"


def _read_cache(path: Path) -> Optional[Dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_cache(path: Path, payload: Dict):
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _build_embedding_text(transcript: str, style_label: str, pace: float, pause: float, energy: float) -> str:
    return (
        f"{transcript}\n"
        f"style:{style_label}\n"
        f"pace_wps:{pace:.3f}\n"
        f"pause_density:{pause:.3f}\n"
        f"energy_rms:{energy:.3f}\n"
        f"feature_version:{FEATURE_VERSION}"
    )


def _sync_video_asset(db, path: Path, file_size: int, mtime: float, duration_sec: float) -> VideoAsset:
    asset = db.query(VideoAsset).filter_by(file_path=str(path)).first()
    if not asset:
        asset = VideoAsset(
            file_path=str(path),
            file_name=path.name,
            file_size=file_size,
            mtime=mtime,
            duration_sec=duration_sec,
            ingest_status="pending",
            last_error="",
        )
        db.add(asset)
        db.commit()
        db.refresh(asset)
        return asset
    changed = (
        int(asset.file_size or 0) != int(file_size)
        or abs(float(asset.mtime or 0.0) - float(mtime)) > 0.0001
    )
    if changed:
        asset.file_size = int(file_size)
        asset.mtime = float(mtime)
        asset.duration_sec = float(duration_sec)
        asset.ingest_status = "pending"
        asset.last_error = ""
        chunk_ids = [row[0] for row in db.query(VideoChunk.id).filter_by(asset_id=asset.id).all()]
        if chunk_ids:
            db.query(VideoSpan).filter(VideoSpan.chunk_id.in_(chunk_ids)).delete(synchronize_session=False)
        db.query(VideoChunk).filter_by(asset_id=asset.id).delete()
        db.commit()
    return asset


def _ingest_video_asset(db, settings: Settings, pc, asset: VideoAsset):
    path = Path(asset.file_path)
    if not path.exists():
        asset.ingest_status = "error"
        asset.last_error = "file missing"
        db.commit()
        return False

    windows = build_chunk_windows(
        duration_sec=float(asset.duration_sec or 0.0),
        chunk_len=float(settings.video_dataset_chunk_len_sec),
        overlap=float(settings.video_dataset_overlap_sec),
    )
    if not windows:
        asset.ingest_status = "error"
        asset.last_error = "empty duration"
        db.commit()
        return False

    index = None
    if pc is not None:
        try:
            index = pc.Index(settings.pinecone_index_video_clips)
        except Exception:
            index = None

    ok_chunks = 0
    for idx, (start_sec, end_sec) in enumerate(windows):
        wait_for_foreground_idle()
        chunk = (
            db.query(VideoChunk)
            .filter_by(asset_id=asset.id, chunk_idx=idx)
            .first()
        )
        if not chunk:
            chunk = VideoChunk(
                asset_id=asset.id,
                chunk_idx=idx,
                start_sec=start_sec,
                end_sec=end_sec,
                transcript="",
                style_label="general",
            )
            db.add(chunk)
            db.commit()
            db.refresh(chunk)

        cache_file = _cache_path(settings, _cache_key(path, float(asset.mtime or 0.0), idx))
        cached = _read_cache(cache_file)
        transcript = str((cached or {}).get("transcript", "")).strip()
        style_label = str((cached or {}).get("style_label", "general")).strip() or "general"
        pace_wps = float((cached or {}).get("pace_wps", 0.0))
        pause_density = float((cached or {}).get("pause_density", 0.0))
        energy_rms = float((cached or {}).get("energy_rms", 0.0))

        wav_bytes = b""
        if not transcript:
            wait_for_foreground_idle()
            wav_bytes = _extract_audio_segment_bytes(path, start_sec, end_sec)
            if not wav_bytes:
                continue
            try:
                transcript = transcribe_audio_file(
                    io.BytesIO(wav_bytes),
                    f"{path.stem}-chunk-{idx}.wav",
                ).strip()
            except Exception:
                transcript = ""
            if not transcript:
                continue
            duration = max(0.1, end_sec - start_sec)
            words = _extract_words(transcript)
            pace_wps = round(words / duration, 4)
            pause_density = _compute_pause_density(transcript, duration)
            energy_rms = _compute_energy_rms(wav_bytes)
            try:
                style_label, _ = classify_style_label(transcript)
            except Exception:
                style_label = "general"
            _write_cache(
                cache_file,
                {
                    "transcript": transcript,
                    "style_label": style_label,
                    "pace_wps": pace_wps,
                    "pause_density": pause_density,
                    "energy_rms": energy_rms,
                },
            )

        chunk.start_sec = float(start_sec)
        chunk.end_sec = float(end_sec)
        chunk.transcript = transcript
        chunk.style_label = style_label
        chunk.pace_wps = float(pace_wps)
        chunk.pause_density = float(pause_density)
        chunk.energy_rms = float(energy_rms)
        chunk.embedding_ready = 0
        db.commit()

        if index is not None:
            try:
                wait_for_foreground_idle()
                vec = embed_text(
                    _build_embedding_text(
                        transcript=transcript,
                        style_label=style_label,
                        pace=float(pace_wps),
                        pause=float(pause_density),
                        energy=float(energy_rms),
                    )
                )
                index.upsert(
                    vectors=[
                        {
                            "id": f"video-{asset.id}-{chunk.id}",
                            "values": vec,
                            "metadata": {
                                "asset_id": asset.id,
                                "chunk_id": chunk.id,
                                "file_path": asset.file_path,
                                "start_sec": float(start_sec),
                                "end_sec": float(end_sec),
                                "style_label": style_label,
                                "pace_wps": float(pace_wps),
                                "pause_density": float(pause_density),
                                "energy_rms": float(energy_rms),
                                "transcript_excerpt": transcript[:220],
                                "feature_version": FEATURE_VERSION,
                            },
                        }
                    ]
                )
                chunk.embedding_ready = 1
                db.commit()
            except Exception:
                pass

        try:
            rebuild_chunk_video_spans(
                db=db,
                settings=settings,
                asset=asset,
                chunk=chunk,
                transcript=transcript,
                style_label=style_label,
                pace_wps=float(pace_wps),
                pause_density=float(pause_density),
                energy_rms=float(energy_rms),
                audio_bytes=wav_bytes,
                audio_filename=f"{path.stem}-chunk-{idx}.wav",
            )
        except Exception:
            logger.exception("video span rebuild failed: asset=%s chunk=%s", asset.id, chunk.id)
        ok_chunks += 1

    if ok_chunks <= 0:
        asset.ingest_status = "error"
        asset.last_error = "no chunks processed"
        db.commit()
        return False
    asset.ingest_status = "ready"
    asset.last_error = ""
    db.commit()
    return True


def _run_ingest(settings: Settings, force_reference_reindex: bool = False):
    reference_summary = rebuild_dataset_reference_index(settings=settings, force=force_reference_reindex)
    _set_status(
        reference_status=str(reference_summary.get("status", "error")).strip() or "error",
        reference_files=int(reference_summary.get("processed_files", 0) or 0),
        reference_spans=int(reference_summary.get("reference_spans", 0) or 0),
        reference_last_error=str(reference_summary.get("last_error", "")).strip(),
    )
    files = _list_video_files(settings.video_dataset_root, getattr(settings, "video_dataset_max_files_for_test", 0))
    logger.info("video ingest start: root=%s files=%s", settings.video_dataset_root, len(files))
    _set_status(
        status="scanning",
        processed_files=0,
        pending_files=len(files),
        failed_files=0,
        degraded=False,
        last_error="",
        dataset_root=str(settings.video_dataset_root),
    )
    if not files:
        logger.warning("video ingest found no files: root=%s", settings.video_dataset_root)
        if int(reference_summary.get("reference_spans", 0) or 0) > 0:
            _set_status(
                status="ready",
                pending_files=0,
                degraded=True,
                last_error="",
            )
            return
        _set_status(
            status="error",
            last_error=f"dataset folder empty: {settings.video_dataset_root}",
            pending_files=0,
        )
        return

    try:
        pc = ensure_indexes() if settings.pinecone_api_key else None
    except Exception as err:
        pc = None
        logger.exception("video ingest pinecone unavailable")
        _set_status(degraded=True, last_error=f"pinecone unavailable: {err}")

    db = get_session()
    processed = 0
    failed = 0
    try:
        for path in files:
            wait_for_foreground_idle()
            stat = path.stat()
            duration = _probe_duration(path)
            asset = _sync_video_asset(
                db=db,
                path=path,
                file_size=int(stat.st_size),
                mtime=float(stat.st_mtime),
                duration_sec=duration,
            )
            if (
                asset.ingest_status == "ready"
                and int(asset.file_size) == int(stat.st_size)
                and abs(float(asset.mtime) - float(stat.st_mtime)) <= 0.0001
            ):
                processed += 1
                _set_status(processed_files=processed, pending_files=max(0, len(files) - processed - failed))
                continue

            asset.ingest_status = "scanning"
            db.commit()
            ok = _ingest_video_asset(db, settings, pc, asset)
            if ok:
                processed += 1
                logger.info("video ingest asset ready: %s", path.name)
            else:
                failed += 1
                logger.warning("video ingest asset failed: %s error=%s", path.name, asset.last_error)
            _set_status(
                status="indexing",
                processed_files=processed,
                failed_files=failed,
                pending_files=max(0, len(files) - processed - failed),
            )
    finally:
        db.close()

    final_status = "ready"
    if processed == 0:
        final_status = "ready" if int(reference_summary.get("reference_spans", 0) or 0) > 0 else "error"
    previous = get_video_dataset_status_payload()
    _set_status(
        status=final_status,
        degraded=bool(previous.get("degraded")) or (failed > 0 and processed > 0) or (processed == 0 and int(reference_summary.get("reference_spans", 0) or 0) > 0),
    )
    logger.info(
        "video ingest done: status=%s processed=%s failed=%s pending=%s",
        final_status,
        processed,
        failed,
        max(0, len(files) - processed - failed),
    )


def start_video_dataset_ingest(settings: Settings):
    global _worker
    disable_flag = os.getenv("DISABLE_VIDEO_DATASET_INGEST", "").strip().lower()
    if disable_flag in {"1", "true", "yes", "on"}:
        return
    if not auto_video_dataset_ingest_enabled():
        return
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return
        _worker = threading.Thread(
            target=_run_ingest,
            args=(settings, False),
            daemon=True,
            name="video-dataset-ingest",
        )
        _worker.start()


def run_video_dataset_ingest_now(settings: Optional[Settings] = None, force_reference_reindex: bool = False):
    effective_settings = settings or Settings()
    _run_ingest(effective_settings, force_reference_reindex=force_reference_reindex)
    return get_video_dataset_status_payload()


def _preview_filename(asset_id: int, start_sec: float, end_sec: float) -> str:
    return f"asset-{int(asset_id)}-{int(max(0.0, start_sec) * 1000)}-{int(max(start_sec + 0.1, end_sec) * 1000)}.mp4"


def _normalize_preview_window(settings: Settings, start_sec: float, end_sec: float) -> Tuple[float, float]:
    start = max(0.0, float(start_sec))
    end = max(start + 0.1, float(end_sec))
    preview_len = max(5.0, float(getattr(settings, "video_dataset_preview_clip_len_sec", 30.0) or 30.0))
    if end - start > preview_len:
        end = start + preview_len
    return round(start, 3), round(end, 3)


def _preview_route_url(filename: str) -> str:
    return f"/api/video-dataset/preview-file/{filename}"


def _remove_preview_file(path: Path):
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _preview_file_is_valid(path: Path) -> Tuple[bool, str]:
    if not path.exists():
        return False, "preview file missing"
    size = int(path.stat().st_size or 0)
    if size < 20 * 1024:
        return False, f"preview file too small: {size} bytes"
    if which("ffprobe"):
        try:
            proc = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                return False, "ffprobe could not read preview file"
            duration = float((proc.stdout or "0").strip() or 0.0)
            if duration <= 0.05:
                return False, "preview duration is too short"
        except Exception:
            return False, "ffprobe validation failed"
    return True, ""


def build_video_preview_clip_result(asset_id: int, start_sec: float, end_sec: float) -> Dict:
    settings = Settings()
    db = get_session()
    try:
        asset = db.query(VideoAsset).filter_by(id=int(asset_id)).first()
        if not asset:
            return {"ok": False, "error": "preview asset not found"}
        source = resolve_asset_file_path(asset, settings, db=db)
        if source is None or not source.exists():
            return {"ok": False, "error": "preview source file missing"}
        output_dir = Path(settings.video_dataset_preview_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        start_sec, end_sec = _normalize_preview_window(settings, start_sec, end_sec)
        name = _preview_filename(asset.id, start_sec, end_sec)
        output_path = output_dir / name

        valid, reason = _preview_file_is_valid(output_path)
        if output_path.exists() and not valid:
            _remove_preview_file(output_path)

        if not output_path.exists():
            if not which("ffmpeg"):
                return {"ok": False, "error": "ffmpeg is not installed or not on PATH"}
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                str(start_sec),
                "-to",
                str(end_sec),
                "-i",
                str(source),
                "-vf",
                "scale='min(854,iw)':-2",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-crf",
                "30",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
            proc = subprocess.run(cmd, capture_output=True, check=False)
            if proc.returncode != 0:
                return {"ok": False, "error": "ffmpeg clip generation failed"}

        valid, reason = _preview_file_is_valid(output_path)
        if not valid:
            _remove_preview_file(output_path)
            return {"ok": False, "error": reason}

        return {
            "ok": True,
            "preview_url": _preview_route_url(name),
            "preview_file": name,
        }
    finally:
        db.close()


def build_video_preview_clip(asset_id: int, start_sec: float, end_sec: float) -> Optional[str]:
    result = build_video_preview_clip_result(asset_id=asset_id, start_sec=start_sec, end_sec=end_sec)
    if not result.get("ok"):
        return None
    return str(result.get("preview_url", "")).strip() or None
