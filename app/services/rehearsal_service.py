import io
import logging
import os
import re
import subprocess
import tempfile
import uuid
from statistics import median
import wave
from difflib import SequenceMatcher
from typing import Callable, Dict, List, Optional, Tuple

from app.services.llm_service import (
    generate_rehearsal_markers,
    generate_comedy_utterance_annotations,
    generate_focus_notes,
)
from app.services.audio_compat import audioop

logger = logging.getLogger(__name__)

ALLOWED_ISSUE_TYPES = {
    "pause-too-short",
    "speed-up",
    "low-energy",
    "falling-intonation",
    "unclear-emphasis",
    "tone-flat",
    "rhythm-break",
}


def _normalize_text(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def segment_script(script: str) -> List[Dict]:
    segments = []
    segment_id = 1
    for match in re.finditer(r"[^.!?\n]+[.!?]?", script):
        raw = match.group(0)
        stripped = raw.strip()
        if not stripped:
            continue
        leading = len(raw) - len(raw.lstrip())
        char_start = match.start() + leading
        char_end = char_start + len(stripped)
        segments.append(
            {
                "segment_id": f"seg-{segment_id}",
                "char_start": char_start,
                "char_end": char_end,
                "text": stripped,
            }
        )
        segment_id += 1
    if not segments and script.strip():
        stripped = script.strip()
        char_start = script.find(stripped)
        segments.append(
            {
                "segment_id": "seg-1",
                "char_start": max(char_start, 0),
                "char_end": max(char_start, 0) + len(stripped),
                "text": stripped,
            }
        )
    return segments


def align_transcript_to_script(
    script: str, transcript_segments: List[Dict], threshold: float = 0.45
) -> Dict:
    script_segments = segment_script(script)
    if not script_segments:
        return {
            "script_segments": [],
            "aligned_segments": [],
            "performed_script_range": None,
        }

    aligned_segments = []
    used_segment_ids = set()

    for transcript_index, transcript in enumerate(transcript_segments or []):
        transcript_text = str(transcript.get("text", "")).strip()
        if not transcript_text:
            continue
        transcript_norm = _normalize_text(transcript_text)
        best = None
        best_score = 0.0
        for segment in script_segments:
            segment_id = segment["segment_id"]
            if segment_id in used_segment_ids:
                continue
            score = SequenceMatcher(
                None, transcript_norm, _normalize_text(segment["text"])
            ).ratio()
            if score > best_score:
                best_score = score
                best = segment
        if best is None or best_score < threshold:
            continue
        used_segment_ids.add(best["segment_id"])
        aligned_segments.append(
            {
                "transcript_index": transcript_index,
                "time_range": [
                    float(transcript.get("start", 0.0)),
                    float(transcript.get("end", 0.0)),
                ],
                "script_range": {
                    "segment_id": best["segment_id"],
                    "char_start": best["char_start"],
                    "char_end": best["char_end"],
                },
                "segment_text": best["text"],
                "transcript_text": transcript_text,
                "confidence": round(best_score, 4),
            }
        )

    if aligned_segments:
        performed_start = min(item["script_range"]["char_start"] for item in aligned_segments)
        performed_end = max(item["script_range"]["char_end"] for item in aligned_segments)
        performed_script_range = {
            "char_start": performed_start,
            "char_end": performed_end,
        }
    else:
        performed_script_range = None

    return {
        "script_segments": script_segments,
        "aligned_segments": aligned_segments,
        "performed_script_range": performed_script_range,
    }


def _is_punchline_candidate(text: str) -> bool:
    if "!" in text or "?" in text:
        return True
    words = re.findall(r"[A-Za-z0-9']+", text)
    return 4 <= len(words) <= 14


def _build_marker_windows(aligned_segments: List[Dict]) -> List[Dict]:
    if not aligned_segments:
        return []
    windows = []
    punchline_windows = []
    previous_end = None
    for item in aligned_segments:
        start, end = item["time_range"]
        gap_before = None
        if previous_end is not None:
            gap_before = max(0.0, start - previous_end)
        previous_end = end
        window = {
            "time_range": [start, end],
            "script_range": item["script_range"],
            "segment_text": item["segment_text"],
            "transcript_text": item["transcript_text"],
            "gap_before": gap_before,
            "window_source": "sentence-boundary",
        }
        windows.append(window)
        if _is_punchline_candidate(item["segment_text"]):
            candidate = dict(window)
            candidate["window_source"] = "punchline-candidate"
            punchline_windows.append(candidate)
    if not punchline_windows:
        punchline_windows = [dict(windows[-1])]
        punchline_windows[0]["window_source"] = "punchline-candidate"
    return punchline_windows + windows


def _build_transcript_fallback_windows(script: str, transcript_segments: List[Dict]) -> List[Dict]:
    valid = [
        seg
        for seg in (transcript_segments or [])
        if isinstance(seg, dict) and str(seg.get("text", "")).strip()
    ]
    if not valid:
        return []
    script_clean = str(script or "")
    script_segments = segment_script(script_clean) if script_clean.strip() else []
    windows = []
    previous_end = None
    for idx, seg in enumerate(valid):
        start = _to_float(seg.get("start"), 0.0)
        end = _to_float(seg.get("end"), start + 0.8)
        if end <= start:
            end = start + 0.8
        gap_before = None if previous_end is None else max(0.0, start - previous_end)
        previous_end = end
        script_range = {}
        segment_text = str(seg.get("text", "")).strip()
        if idx < len(script_segments):
            matched = script_segments[idx]
            script_range = {
                "segment_id": matched["segment_id"],
                "char_start": matched["char_start"],
                "char_end": matched["char_end"],
            }
            segment_text = str(matched.get("text", "")).strip() or segment_text
        windows.append(
            {
                "time_range": [round(start, 3), round(end, 3)],
                "script_range": script_range,
                "segment_text": segment_text,
                "transcript_text": str(seg.get("text", "")).strip(),
                "gap_before": gap_before,
                "window_source": "transcript-fallback",
            }
        )
    return windows


def _default_instruction(issue_type: str, style_preset: str) -> str:
    base = {
        "pause-too-short": "Add a clean pause right before the punchline to increase contrast.",
        "speed-up": "Slow your pace in this line and separate key words with clearer beats.",
        "low-energy": "Lift your vocal energy and punch the final keyword with more intent.",
        "falling-intonation": "Keep the line ending more open and avoid dropping pitch too early.",
        "unclear-emphasis": "Choose one anchor word and stress it clearly on the second half.",
        "tone-flat": "Shape your tone contour more clearly and add emotional contrast.",
        "rhythm-break": "Reset the rhythm by pausing briefly, then re-enter with stable pacing.",
    }.get(issue_type, "Deliver this line with clearer pacing and emphasis.")
    if style_preset:
        return f"{base} Keep the tone in a {style_preset} style."
    return base


def _default_rationale(issue_type: str, duration: float, words_per_second: float) -> str:
    if issue_type == "pause-too-short":
        return "The transition into this beat is too tight, so the setup and punchline blur together."
    if issue_type == "speed-up":
        return (
            "This segment runs faster than a stable stage pace, reducing clarity near the punchline."
        )
    if issue_type == "low-energy":
        return "The delivery energy stays flat, so the comedic intent is harder to register."
    if issue_type == "falling-intonation":
        return "Pitch drops too early at the end, which weakens anticipation for the joke beat."
    if issue_type == "tone-flat":
        return "The vocal contour stays too even, so emotional intent and contrast are weaker."
    if issue_type == "rhythm-break":
        return "Beat spacing changes abruptly, making the comedic timing feel less intentional."
    return (
        f"Timing is readable but emphasis is diffuse (duration={duration:.2f}s, "
        f"speed={words_per_second:.2f} w/s)."
    )


def _build_marker(window: Dict, style_preset: str = "") -> Dict:
    start, end = window["time_range"]
    duration = max(0.2, end - start)
    words = re.findall(r"[A-Za-z0-9']+", window["transcript_text"])
    words_per_second = len(words) / duration
    gap_before = window.get("gap_before")

    issue_type = "unclear-emphasis"
    severity = 0.55
    if window["window_source"] == "punchline-candidate" and gap_before is not None and gap_before < 0.2:
        issue_type = "pause-too-short"
        severity = 0.88
    elif words_per_second > 3.3:
        issue_type = "speed-up"
        severity = min(1.0, 0.7 + (words_per_second - 3.3) * 0.2)
    elif words_per_second < 1.1:
        issue_type = "low-energy"
        severity = 0.72
    elif window["segment_text"].endswith(".") and duration < 1.0:
        issue_type = "falling-intonation"
        severity = 0.67

    return {
        "id": f"mk-{uuid.uuid4().hex[:8]}",
        "time_range": [round(start, 3), round(end, 3)],
        "script_range": window["script_range"],
        "issue_type": issue_type,
        "severity": round(severity, 3),
        "instruction": _default_instruction(issue_type, style_preset),
        "rationale": _default_rationale(issue_type, duration, words_per_second),
        "demo_text": window["segment_text"],
        "window_source": window["window_source"],
    }


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _decode_audio_to_wav_bytes(audio_bytes: bytes, audio_filename: str) -> bytes:
    if not audio_bytes:
        return b""
    ext = os.path.splitext(audio_filename or "")[1].lower()
    if ext == ".wav":
        return audio_bytes
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, f"input{ext or '.bin'}")
            output_path = os.path.join(tmpdir, "decoded.wav")
            with open(source_path, "wb") as handle:
                handle.write(audio_bytes)
            completed = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    source_path,
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    output_path,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if completed.returncode != 0 or not os.path.exists(output_path):
                return b""
            return open(output_path, "rb").read()
    except Exception:
        return b""



def _extract_audio_profiles(
    audio_bytes: bytes,
    audio_filename: str,
    windows: List[Dict],
) -> Dict[int, Dict]:
    if not audio_bytes or not windows:
        return {}
    wav_bytes = _decode_audio_to_wav_bytes(audio_bytes, audio_filename)
    if not wav_bytes:
        return {}
    try:
        with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
            sample_rate = wav_file.getframerate()
            frame_count = wav_file.getnframes()
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            if sample_rate <= 0 or frame_count <= 0 or sample_width <= 0:
                return {}
            audio_duration = frame_count / float(sample_rate)
            full_scale = float(2 ** (8 * sample_width - 1))
            profiles = {}
            for index, window in enumerate(windows):
                base_start, base_end = window.get("time_range", [0.0, 0.0])
                clip_start = max(0.0, min(audio_duration, _to_float(base_start, 0.0)))
                clip_end = max(clip_start, min(audio_duration, _to_float(base_end, clip_start)))
                if clip_end <= clip_start:
                    continue
                start_frame = int(clip_start * sample_rate)
                end_frame = int(clip_end * sample_rate)
                if end_frame <= start_frame:
                    continue
                wav_file.setpos(start_frame)
                frames = wav_file.readframes(max(0, end_frame - start_frame))
                if not frames:
                    continue
                mono_frames = frames
                if channels > 1:
                    mono_frames = audioop.tomono(frames, sample_width, 0.5, 0.5)
                rms = audioop.rms(mono_frames, sample_width)
                peak = audioop.max(mono_frames, sample_width)
                duration = max(0.001, clip_end - clip_start)
                words = re.findall(r"[A-Za-z0-9']+", window.get("transcript_text", ""))
                profiles[index] = {
                    "analysis_time_range": [round(clip_start, 3), round(clip_end, 3)],
                    "rms_level": round(min(1.0, rms / full_scale), 4),
                    "peak_level": round(min(1.0, peak / full_scale), 4),
                    "words_per_second": round(len(words) / duration, 3),
                    "pause_before": _to_float(window.get("gap_before"), 0.0),
                }
            return profiles
    except Exception:
        return {}



def _pick_window_for_marker(raw_marker: Dict, windows: List[Dict]) -> Dict:
    if not windows:
        return {}
    index = raw_marker.get("window_index")
    if isinstance(index, int) and 0 <= index < len(windows):
        return windows[index]

    raw_script_range = raw_marker.get("script_range")
    if isinstance(raw_script_range, dict):
        raw_segment_id = str(raw_script_range.get("segment_id", "")).strip()
        if raw_segment_id:
            for window in windows:
                segment_id = str(
                    window.get("script_range", {}).get("segment_id", "")
                ).strip()
                if segment_id == raw_segment_id:
                    return window

    raw_time = raw_marker.get("time_range")
    if isinstance(raw_time, list) and len(raw_time) == 2:
        raw_start = _to_float(raw_time[0])
        raw_end = _to_float(raw_time[1])
        best_window = windows[0]
        best_overlap = -1.0
        for window in windows:
            overlap = _overlap_ratio(
                [raw_start, raw_end],
                [
                    _to_float(window.get("time_range", [0.0, 0.0])[0]),
                    _to_float(window.get("time_range", [0.0, 0.0])[1]),
                ],
            )
            if overlap > best_overlap:
                best_overlap = overlap
                best_window = window
        return best_window
    return windows[0]


def _normalize_generated_markers(
    raw_markers: List[Dict],
    windows: List[Dict],
    style_preset: str = "",
) -> List[Dict]:
    normalized = []
    for raw_marker in raw_markers:
        if not isinstance(raw_marker, dict):
            continue
        window = _pick_window_for_marker(raw_marker, windows)
        if not window:
            continue
        window_time = window.get("time_range", [0.0, 0.0])
        win_start = _to_float(window_time[0], 0.0)
        win_end = _to_float(window_time[1], win_start)

        raw_time = raw_marker.get("time_range")
        if isinstance(raw_time, list) and len(raw_time) == 2:
            marker_start = _to_float(raw_time[0], win_start)
            marker_end = _to_float(raw_time[1], win_end)
        else:
            marker_start, marker_end = win_start, win_end

        if marker_end < marker_start:
            marker_start, marker_end = marker_end, marker_start
        marker_start = max(win_start, marker_start)
        marker_end = min(win_end, marker_end)
        if marker_end <= marker_start:
            marker_start, marker_end = win_start, win_end

        issue_type = str(raw_marker.get("issue_type", "unclear-emphasis")).strip()
        if issue_type not in ALLOWED_ISSUE_TYPES:
            issue_type = "unclear-emphasis"

        severity = _to_float(raw_marker.get("severity"), 0.6)
        severity = max(0.0, min(1.0, severity))

        instruction = str(raw_marker.get("instruction", "")).strip()
        if not instruction:
            instruction = _default_instruction(issue_type, style_preset)
        rationale = str(raw_marker.get("rationale", "")).strip()
        if not rationale:
            duration = max(0.2, marker_end - marker_start)
            transcript_text = str(window.get("transcript_text", "")).strip()
            words = re.findall(r"[A-Za-z0-9']+", transcript_text)
            rationale = _default_rationale(issue_type, duration, len(words) / duration)

        script_range = raw_marker.get("script_range")
        if not isinstance(script_range, dict):
            script_range = window.get("script_range", {})

        demo_text = str(raw_marker.get("demo_text", "")).strip()
        if not demo_text:
            demo_text = str(window.get("segment_text", "")).strip()

        normalized.append(
            {
                "id": str(raw_marker.get("id", f"mk-{uuid.uuid4().hex[:8]}")).strip(),
                "time_range": [round(marker_start, 3), round(marker_end, 3)],
                "script_range": script_range,
                "issue_type": issue_type,
                "severity": round(severity, 3),
                "instruction": instruction,
                "rationale": rationale,
                "demo_text": demo_text,
                "window_source": str(
                    raw_marker.get("window_source", window.get("window_source", "sentence-boundary"))
                ),
            }
        )
    return normalized


def _overlap_ratio(a: List[float], b: List[float]) -> float:
    start = max(a[0], b[0])
    end = min(a[1], b[1])
    if end <= start:
        return 0.0
    overlap = end - start
    union = max(a[1], b[1]) - min(a[0], b[0])
    if union <= 0:
        return 0.0
    return overlap / union


def select_top_markers(markers: List[Dict], limit: int = 5) -> List[Dict]:
    ranked = sorted(markers, key=lambda item: float(item.get("severity", 0.0)), reverse=True)
    selected = []
    for marker in ranked:
        duplicated = False
        for chosen in selected:
            if marker.get("issue_type") != chosen.get("issue_type"):
                continue
            if _overlap_ratio(marker["time_range"], chosen["time_range"]) >= 0.55:
                duplicated = True
                break
        if duplicated:
            continue
        selected.append(marker)
        if len(selected) >= limit:
            break
    return selected




def _safe_percentile(values: List[float], percentile: float, default: float = 0.0) -> float:
    numeric = sorted(float(v) for v in values if v is not None)
    if not numeric:
        return default
    if len(numeric) == 1:
        return numeric[0]
    percentile = max(0.0, min(1.0, float(percentile)))
    idx = percentile * (len(numeric) - 1)
    lower = int(idx)
    upper = min(len(numeric) - 1, lower + 1)
    frac = idx - lower
    return numeric[lower] * (1.0 - frac) + numeric[upper] * frac



def _normalized_rank(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.5
    return max(0.0, min(1.0, (float(value) - low) / (high - low)))



def _utterance_context_text(utterances: List[Dict], index: int, radius: int = 1) -> Tuple[str, str]:
    before = []
    after = []
    for i in range(max(0, index - radius), index):
        text = str(utterances[i].get("text", "")).strip()
        if text:
            before.append(text)
    for i in range(index + 1, min(len(utterances), index + radius + 1)):
        text = str(utterances[i].get("text", "")).strip()
        if text:
            after.append(text)
    return " ".join(before).strip(), " ".join(after).strip()


_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+")
_WEAK_BOUNDARY_RE = re.compile(r"(?<=[,;:—-])\s+")
_DISCOURSE_MARKER_RE = re.compile(
    r"\b(?:so|but|then|like|yeah|well|anyway|because|actually|instead|that's right|i mean)\b",
    re.IGNORECASE,
)


def _split_text_into_comedy_beats(text: str, max_tokens: int = 14) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []

    parts: List[str] = []
    sentence_like = _SENTENCE_BOUNDARY_RE.split(raw)
    if len(sentence_like) == 1:
        sentence_like = [raw]

    for sentence in sentence_like:
        sentence = sentence.strip()
        if not sentence:
            continue

        weak_chunks = _WEAK_BOUNDARY_RE.split(sentence)
        if len(weak_chunks) == 1:
            weak_chunks = [sentence]

        for chunk in weak_chunks:
            chunk = chunk.strip()
            if not chunk:
                continue

            if len(re.findall(r"[A-Za-z0-9']+", chunk)) <= max_tokens:
                parts.append(chunk)
                continue

            current: List[str] = []
            current_word_count = 0
            words = re.findall(r"\S+", chunk)
            for word in words:
                current.append(word)
                if re.search(r"[A-Za-z0-9']", word):
                    current_word_count += 1

                joined = " ".join(current).strip()
                boundary_hit = bool(_DISCOURSE_MARKER_RE.search(word))
                punctuation_hit = bool(re.search(r"[,:;.!?]$", word))

                if current_word_count >= max_tokens and (boundary_hit or punctuation_hit):
                    parts.append(joined)
                    current = []
                    current_word_count = 0

            if current:
                parts.append(" ".join(current).strip())

    cleaned = [p.strip() for p in parts if p and p.strip()]
    return cleaned or [raw]



def _split_segment_with_time(seg: Dict, max_tokens: int = 14) -> List[Dict]:
    text = str(seg.get("text", "")).strip()
    if not text:
        return []

    time_range = seg.get("time_range", [None, None]) if isinstance(seg, dict) else [None, None]
    start = _to_float(seg.get("start", time_range[0]), 0.0)
    end = _to_float(seg.get("end", time_range[1]), start + 0.8)
    if end <= start:
        end = start + 0.8

    beats = _split_text_into_comedy_beats(text, max_tokens=max_tokens)
    if len(beats) <= 1:
        return [{
            "text": text,
            "start": round(start, 3),
            "end": round(end, 3),
        }]

    word_counts = [max(1, len(re.findall(r"[A-Za-z0-9']+", beat))) for beat in beats]
    total_words = max(1, sum(word_counts))
    total_duration = max(0.8, end - start)

    cursor = start
    out: List[Dict] = []
    for idx, beat in enumerate(beats):
        frac = word_counts[idx] / total_words
        beat_duration = total_duration * frac
        beat_end = end if idx == len(beats) - 1 else min(end, cursor + max(0.45, beat_duration))
        if beat_end <= cursor:
            beat_end = min(end, cursor + 0.45)

        out.append({
            "text": beat,
            "start": round(cursor, 3),
            "end": round(beat_end, 3),
        })
        cursor = beat_end

    return out



def build_utterances_from_transcript(transcript_segments: List[Dict]) -> List[Dict]:
    utterances: List[Dict] = []
    previous_end = None

    for seg in transcript_segments or []:
        split_segments = _split_segment_with_time(seg, max_tokens=14)

        for split_seg in split_segments:
            text = str(split_seg.get("text", "")).strip()
            if not text:
                continue

            start = _to_float(split_seg.get("start"), 0.0)
            end = _to_float(split_seg.get("end"), start + 0.8)
            if end <= start:
                end = start + 0.8

            gap_before = None if previous_end is None else round(max(0.0, start - previous_end), 3)
            previous_end = end

            utterances.append(
                {
                    "id": f"utt-{len(utterances) + 1}",
                    "index": len(utterances),
                    "text": text,
                    "time_range": [round(start, 3), round(end, 3)],
                    "script_range": {},
                    "segment_text": text,
                    "gap_before": gap_before,
                    "alignment_confidence": 0.0,
                }
            )

    for idx, utt in enumerate(utterances):
        before, after = _utterance_context_text(utterances, idx, radius=1)
        utt["context_before"] = before
        utt["context_after"] = after

    return utterances



def attach_alignment_to_utterances(utterances: List[Dict], alignment: Dict) -> List[Dict]:
    aligned_by_index = {}
    for item in (alignment or {}).get("aligned_segments", []) or []:
        try:
            transcript_index = int(item.get("transcript_index"))
        except Exception:
            continue
        aligned_by_index[transcript_index] = item
    updated = []
    for utt in utterances:
        item = dict(utt)
        aligned = aligned_by_index.get(int(item.get("index", -1)))
        if aligned:
            item["script_range"] = aligned.get("script_range", {}) or {}
            item["segment_text"] = str(aligned.get("segment_text", "")).strip() or item.get("segment_text", "")
            item["alignment_confidence"] = round(_to_float(aligned.get("confidence"), 0.0), 4)
        else:
            item.setdefault("script_range", {})
            item["alignment_confidence"] = round(_to_float(item.get("alignment_confidence"), 0.0), 4)
        updated.append(item)
    return updated



def build_utterances_from_alignment(script: str, alignment: Dict, transcript_segments: Optional[List[Dict]] = None) -> List[Dict]:
    utterances = build_utterances_from_transcript(transcript_segments or [])
    if utterances:
        return attach_alignment_to_utterances(utterances, alignment or {})

    fallback_windows = _build_transcript_fallback_windows(
        script=script,
        transcript_segments=transcript_segments or [],
    )
    if not fallback_windows:
        return []
    return attach_alignment_to_utterances(
        build_utterances_from_transcript(fallback_windows),
        alignment or {},
    )



def extract_utterance_audio_profiles(
    audio_bytes: bytes,
    audio_filename: str,
    utterances: List[Dict],
) -> Dict[str, Dict]:
    windows = []
    for utt in utterances:
        windows.append(
            {
                "time_range": utt.get("time_range", [0.0, 0.0]),
                "transcript_text": utt.get("text", ""),
                "gap_before": utt.get("gap_before"),
            }
        )
    base_profiles = _extract_audio_profiles(
        audio_bytes=audio_bytes,
        audio_filename=audio_filename,
        windows=windows,
    )
    mapped: Dict[str, Dict] = {}
    for idx, utt in enumerate(utterances):
        time_range = utt.get("time_range", [0.0, 0.0])
        start = _to_float(time_range[0], 0.0)
        end = _to_float(time_range[1], start)
        duration = max(0.001, end - start)
        words = re.findall(r"[A-Za-z0-9']+", str(utt.get("text", "")))
        profile = dict(base_profiles.get(idx, {}) or {})
        if not profile:
            profile = {
                "analysis_time_range": [round(start, 3), round(end, 3)],
                "rms_level": 0.0,
                "peak_level": 0.0,
                "words_per_second": round(len(words) / duration, 3),
                "pause_before": _to_float(utt.get("gap_before"), 0.0),
            }
        profile.setdefault("analysis_time_range", [round(start, 3), round(end, 3)])
        profile["duration"] = round(duration, 3)
        profile["token_count"] = len(words)
        mapped[str(utt.get("id", idx))] = profile
    return mapped



def _contains_any(text: str, vocabulary: List[str]) -> bool:
    lowered = (text or "").lower()
    return any(str(token).lower() in lowered for token in vocabulary)



def _normalize_llm_utterance_annotations(
    utterances: List[Dict],
    raw_annotations: List[Dict],
) -> List[Dict]:
    by_id = {
        str(item.get("id", "")).strip(): item
        for item in raw_annotations
        if isinstance(item, dict) and str(item.get("id", "")).strip()
    }
    normalized = []
    for utt in utterances:
        item = dict(utt)
        incoming = by_id.get(str(utt.get("id", "")).strip(), {})
        role = str(incoming.get("comedy_function", item.get("comedy_function", "other"))).strip().lower() or "other"
        if role not in {"setup", "misdirect", "pivot", "punch", "tag", "bridge", "callback", "other"}:
            role = "other"
        item["comedy_function"] = role
        try:
            conf = float(incoming.get("function_confidence", item.get("function_confidence", 0.55)))
        except Exception:
            conf = float(item.get("function_confidence", 0.55) or 0.55)
        item["function_confidence"] = round(max(0.0, min(1.0, conf)), 3)
        tags = incoming.get("delivery_tags", item.get("delivery_tags", []))
        if not isinstance(tags, list):
            tags = []
        item["delivery_tags"] = [str(tag).strip() for tag in tags if str(tag).strip()]
        item["is_focus_span"] = bool(incoming.get("is_focus_span", item.get("is_focus_span", False)))
        item["joke_role"] = str(incoming.get("joke_role", item.get("joke_role", "shape"))).strip() or str(item.get("joke_role", "shape")).strip() or "shape"
        item["laugh_bearing_score"] = round(_to_float(incoming.get("laugh_bearing_score"), item.get("laugh_bearing_score", 0.0)), 3)
        item["supporting_score"] = round(_to_float(incoming.get("supporting_score"), item.get("supporting_score", 0.0)), 3)
        normalized.append(item)
    return normalized



def fallback_annotate_comedy_functions(utterances: List[Dict]) -> List[Dict]:
    if not utterances:
        return []
    token_counts = [int((utt.get("audio_features", {}) or {}).get("token_count", 0) or 0) for utt in utterances]
    durations = [float((utt.get("audio_features", {}) or {}).get("duration", 0.0) or 0.0) for utt in utterances]
    pauses = [float((utt.get("audio_features", {}) or {}).get("pause_before", 0.0) or 0.0) for utt in utterances if utt.get("audio_features")]
    energies = [float((utt.get("audio_features", {}) or {}).get("rms_level", 0.0) or 0.0) for utt in utterances if utt.get("audio_features")]
    wps_values = [float((utt.get("audio_features", {}) or {}).get("words_per_second", 0.0) or 0.0) for utt in utterances]
    token_q25 = _safe_percentile(token_counts, 0.25, 0.0)
    token_q75 = _safe_percentile(token_counts, 0.75, 1.0)
    duration_q50 = _safe_percentile(durations, 0.5, 1.0)
    pause_q25 = _safe_percentile(pauses, 0.25, 0.0)
    pause_q75 = _safe_percentile(pauses, 0.75, 0.0)
    energy_q25 = _safe_percentile(energies, 0.25, 0.0)
    wps_q75 = _safe_percentile(wps_values, 0.75, 0.0)

    pivot_terms = ["but", "so", "then", "actually", "instead", "turns out", "except", "until", "suddenly", "and then"]
    callback_terms = ["again", "back", "remember", "still", "as usual", "same thing"]
    misdirect_terms = ["i thought", "i figured", "i assumed", "obviously", "of course"]

    candidate_focus_scores: List[Tuple[float, float, int]] = []
    provisional_roles: List[str] = []
    for idx, utt in enumerate(utterances):
        text = str(utt.get("text", "")).strip()
        feats = utt.get("audio_features", {}) or {}
        token_count = int(feats.get("token_count", 0) or 0)
        duration = float(feats.get("duration", 0.0) or 0.0)
        pause_before = float(feats.get("pause_before", 0.0) or 0.0)
        wps = float(feats.get("words_per_second", 0.0) or 0.0)
        rms = float(feats.get("rms_level", 0.0) or 0.0)
        peak = float(feats.get("peak_level", 0.0) or 0.0)
        position = 0.0 if len(utterances) <= 1 else idx / float(len(utterances) - 1)
        shortness = 1.0 - _normalized_rank(token_count, token_q25, token_q75)
        lengthiness = 1.0 - shortness
        tail_bias = position
        has_pivot = _contains_any(text, pivot_terms)
        has_callback = _contains_any(text, callback_terms)
        has_misdirect = _contains_any(text, misdirect_terms)
        prev_tokens = token_counts[idx - 1] if idx > 0 else token_count
        contrast = max(0.0, float(prev_tokens - token_count))
        contrast_score = _normalized_rank(contrast, 0.0, max(1.0, token_q75))
        pause_score = _normalized_rank(pause_before, pause_q25, max(pause_q75, pause_q25 + 0.01))
        energy_score = _normalized_rank(rms, energy_q25, max(energy_q25 + 0.1, 0.35))

        punch_score = 0.34 * tail_bias + 0.24 * shortness + 0.19 * contrast_score + 0.13 * pause_score + 0.1 * energy_score
        setup_score = 0.45 * (1.0 - tail_bias) + 0.35 * lengthiness + 0.2 * _normalized_rank(duration, duration_q50, max(duration_q50 * 1.5, duration_q50 + 0.1))
        pivot_score = 0.56 * (1.0 if has_pivot else 0.0) + 0.2 * tail_bias + 0.14 * contrast_score + 0.1 * pause_score
        callback_score = 0.65 * (1.0 if has_callback else 0.0) + 0.35 * tail_bias
        misdirect_score = 0.62 * (1.0 if has_misdirect else 0.0) + 0.23 * lengthiness + 0.15 * (1.0 - tail_bias)

        scores = {
            "setup": setup_score,
            "pivot": pivot_score,
            "punch": punch_score,
            "callback": callback_score,
            "misdirect": misdirect_score,
            "other": 0.2,
        }
        role = max(scores.items(), key=lambda item: item[1])[0]
        if idx > 0 and provisional_roles[-1] == "punch" and shortness >= 0.55:
            role = "tag"
        elif role == "other" and idx < max(1, len(utterances) // 2):
            role = "setup"
        provisional_roles.append(role)

        delivery_tags: List[str] = []
        if role in {"pivot", "punch", "tag", "callback"}:
            if pause_before <= pause_q25 and role in {"pivot", "punch"}:
                delivery_tags.append("weak_release" if role in {"punch", "callback"} else "weak_build")
            if wps >= wps_q75 and role in {"pivot", "punch", "tag", "callback"}:
                delivery_tags.append("rushed_release" if role in {"punch", "tag", "callback"} else "rushed_build")
            if rms <= energy_q25:
                delivery_tags.append("flat_shape")
            if peak and (peak - rms) < 0.08:
                delivery_tags.append("weak_emphasis")

        joke_role = {
            "setup": "build",
            "misdirect": "build",
            "bridge": "build",
            "pivot": "turn",
            "punch": "release",
            "callback": "release",
            "tag": "tag",
        }.get(role, "shape")
        laugh_bearing_score = max(
            punch_score,
            callback_score,
            0.82 * pivot_score,
            0.55 * misdirect_score,
            0.45 * (1.0 if role == "tag" else 0.0),
        )
        supporting_score = max(setup_score, 0.85 * misdirect_score, 0.8 * pivot_score)

        utt["comedy_function"] = role
        utt["function_confidence"] = round(min(0.95, max(scores.get(role, 0.55), 0.45)), 3)
        utt["delivery_tags"] = sorted(set(delivery_tags))
        utt["joke_role"] = joke_role
        utt["laugh_bearing_score"] = round(min(0.99, max(0.0, laugh_bearing_score)), 3)
        utt["supporting_score"] = round(min(0.99, max(0.0, supporting_score)), 3)
        utt["is_focus_span"] = role in {"pivot", "punch", "tag", "callback"} or laugh_bearing_score >= 0.58
        candidate_focus_scores.append((
            max(laugh_bearing_score, supporting_score * 0.75),
            position,
            idx,
        ))

    if utterances and not any(bool(utt.get("is_focus_span")) for utt in utterances):
        _, _, focus_idx = max(candidate_focus_scores)
        focus_utt = utterances[focus_idx]
        focus_utt["is_focus_span"] = True
        tags = [str(tag).strip() for tag in (focus_utt.get("delivery_tags", []) or []) if str(tag).strip()]
        if "provisional_focus" not in tags:
            tags.append("provisional_focus")
        focus_utt["delivery_tags"] = sorted(set(tags))
        focus_utt["function_confidence"] = round(max(float(focus_utt.get("function_confidence", 0.55) or 0.55), 0.55), 3)

    return utterances



def annotate_comedy_functions(
    script: str,
    utterances: List[Dict],
    style_preset: str = "",
    disable_llm_enrichment: bool = False,
) -> List[Dict]:
    base = fallback_annotate_comedy_functions([dict(item) for item in utterances])
    llm_annotations = []
    if not disable_llm_enrichment:
        try:
            llm_annotations = generate_comedy_utterance_annotations(
                script=script,
                utterances=base,
                style_preset=style_preset,
            )
        except Exception:
            llm_annotations = []
    if not llm_annotations:
        return base
    merged = _normalize_llm_utterance_annotations(base, llm_annotations)
    for item in merged:
        if item.get("comedy_function") in {"pivot", "punch", "tag", "callback"}:
            item["is_focus_span"] = True
        if not str(item.get("joke_role", "")).strip():
            item["joke_role"] = {
                "setup": "build",
                "misdirect": "build",
                "bridge": "build",
                "pivot": "turn",
                "punch": "release",
                "callback": "release",
                "tag": "tag",
            }.get(str(item.get("comedy_function", "other")).strip(), "shape")
    return merged



def build_joke_units(utterances: List[Dict]) -> List[Dict]:
    joke_units: List[Dict] = []
    if not utterances:
        return joke_units

    release_candidates = []
    for idx, utt in enumerate(utterances):
        role = str(utt.get("joke_role", "")).strip()
        function_name = str(utt.get("comedy_function", "")).strip()
        laugh_score = float(utt.get("laugh_bearing_score", 0.0) or 0.0)
        if role in {"release", "tag"} or function_name in {"punch", "callback"} or laugh_score >= 0.6:
            release_candidates.append((idx, laugh_score))

    if not release_candidates:
        focus_indices = [idx for idx, utt in enumerate(utterances) if utt.get("is_focus_span")]
        if focus_indices:
            release_candidates = [(focus_indices[-1], float(utterances[focus_indices[-1]].get("laugh_bearing_score", 0.5) or 0.5))]

    seen_ranges = []
    for seq, (release_idx, _) in enumerate(release_candidates, start=1):
        start_idx = max(0, release_idx - 4)
        end_idx = min(len(utterances) - 1, release_idx + 2)
        if any(start_idx <= prev_release <= end_idx for _, prev_release in seen_ranges):
            continue

        setup_ids = []
        pivot_ids = []
        tag_ids = []
        punch_ids = [str(utterances[release_idx].get("id"))]

        for idx in range(start_idx, release_idx):
            utt = utterances[idx]
            role = str(utt.get("joke_role", "")).strip()
            function_name = str(utt.get("comedy_function", "")).strip()
            utt_id = str(utt.get("id", "")).strip()
            if role == "turn" or function_name == "pivot":
                pivot_ids.append(utt_id)
            elif role == "build" or function_name in {"setup", "misdirect", "bridge"}:
                setup_ids.append(utt_id)

        for idx in range(release_idx + 1, min(len(utterances), release_idx + 3)):
            utt = utterances[idx]
            role = str(utt.get("joke_role", "")).strip()
            function_name = str(utt.get("comedy_function", "")).strip()
            utt_id = str(utt.get("id", "")).strip()
            if role == "tag" or function_name == "tag":
                tag_ids.append(utt_id)
            elif idx == release_idx + 1 and float(utt.get("laugh_bearing_score", 0.0) or 0.0) >= 0.5:
                tag_ids.append(utt_id)

        joke_units.append(
            {
                "id": f"joke-{len(joke_units) + 1}",
                "setup_ids": setup_ids,
                "pivot_ids": pivot_ids,
                "punch_ids": punch_ids,
                "tag_ids": tag_ids,
            }
        )
        seen_ranges.append((start_idx, release_idx))
    return joke_units



def _focus_title(function_name: str, focus_type: str, delivery_tags: List[str]) -> str:
    if function_name == "punch":
        if "rushed_release" in delivery_tags:
            return "Punch is arriving too quickly"
        if "weak_release" in delivery_tags:
            return "Punch needs a cleaner release"
        if "flat_shape" in delivery_tags:
            return "Punch needs more shape"
        return "Punch delivery can land more clearly"
    if function_name == "pivot":
        return "Turn into the laugh needs more contrast"
    if function_name == "tag":
        return "Tag can follow the laugh more cleanly"
    if function_name == "callback":
        return "Callback needs a clearer cue"
    return "Delivery focus"



def _fallback_note_for_utterance(utterance: Dict, joke_unit_id: str) -> Dict:
    function_name = str(utterance.get("comedy_function", "other")).strip()
    delivery_tags = [str(tag).strip() for tag in (utterance.get("delivery_tags", []) or []) if str(tag).strip()]
    text = str(utterance.get("text", "")).strip()
    context_before = str(utterance.get("context_before", "")).strip()
    pause_before = float((utterance.get("audio_features", {}) or {}).get("pause_before", 0.0) or 0.0)
    focus_type = {
        "setup": "build",
        "pivot": "turn",
        "punch": "release",
        "tag": "tag",
        "callback": "release",
    }.get(function_name, "shape")

    if function_name == "punch":
        advice = (
            "This line reads like the main laugh trigger. Keep the setup moving, then make the release easier to hear in the audio. "
            "Let the turn happen before the final idea lands."
        )
        why = "When the release arrives too flat or too quickly, the audience hears the information but not the payoff."
        try_next = "Run the line again and work only on the last thought. Give the reveal a clearer beat, then let the key word finish the line."
        if "rushed_release" in delivery_tags:
            advice = (
                "This sounds like the main laugh trigger, but the release is rushing by in the audio. Slow the final thought just enough that the audience can register the turn before the reveal lands."
            )
            try_next = "Keep the setup conversational. In the final stretch of the line, stop accelerating and let the reveal arrive in one clean piece."
        elif "weak_release" in delivery_tags:
            advice = (
                "This line already contains the reveal, but the audio moves into it without enough separation. Give the turn a little more room before the payoff."
            )
            try_next = "Repeat the line and change only the moment before the reveal. Add a small beat of space, then finish cleanly."
        elif "flat_shape" in delivery_tags or "weak_emphasis" in delivery_tags:
            advice = (
                "The joke turn is present, but the delivery stays too even for the payoff to stand out. Let one word in the reveal do more of the work."
            )
            try_next = "Keep the whole sentence natural, then make the surprise word easier to hear instead of pushing every word equally."
    elif function_name == "pivot":
        advice = (
            "This line is doing the turn from setup into payoff. Right now the audio does not separate the turn strongly enough from what came before."
        )
        why = "The pivot is where the audience stops hearing explanation and starts hearing the joke mechanism."
        try_next = "Run the setup and this line together. Make this sentence feel like a change in direction, not just the next sentence."
    elif function_name == "tag":
        advice = (
            "This line behaves like a tag. It should ride the energy of the laugh that came just before it, not restart from neutral."
        )
        why = "A tag works best when it feels attached to the previous laugh instead of opening a new setup."
        try_next = "Say the punch and this line back to back. Keep the tag lighter and faster than the punch, but still easy to hear."
    else:
        advice = (
            "This span matters because it shapes how the audience receives the next beat. Use the audio to make the intention clearer, not bigger."
        )
        why = "Stand up works when the audience can hear the job of each line inside the larger joke structure."
        try_next = "Keep the line conversational and focus on making the transition into the next beat easier to follow."

    if context_before and function_name in {"pivot", "punch"}:
        why = f"This line follows '{context_before}'. The audience needs to hear a stronger change in function between that setup material and this line."

    return {
        "id": f"note-{str(utterance.get('id', '')).strip() or uuid.uuid4().hex[:8]}",
        "utterance_id": str(utterance.get("id", "")).strip(),
        "joke_unit_id": joke_unit_id,
        "time_range": utterance.get("time_range", [0.0, 0.0]),
        "script_range": utterance.get("script_range", {}),
        "comedy_function": function_name,
        "focus_type": focus_type,
        "joke_role": str(utterance.get("joke_role", focus_type)).strip() or focus_type,
        "title": _focus_title(function_name, focus_type, delivery_tags),
        "advice": advice,
        "why": why,
        "try_next": try_next,
        "delivery_tags": delivery_tags,
        "quote": text,
        "pause_before": round(pause_before, 3),
    }



def _normalize_focus_notes(
    utterances: List[Dict],
    joke_units: List[Dict],
    raw_notes: List[Dict],
) -> List[Dict]:
    utt_by_id = {str(utt.get("id", "")).strip(): utt for utt in utterances}
    valid_unit_ids = {str(unit.get("id", "")).strip() for unit in joke_units}
    notes = []
    for item in raw_notes:
        if not isinstance(item, dict):
            continue
        utterance_id = str(item.get("utterance_id", "")).strip()
        utterance = utt_by_id.get(utterance_id)
        if not utterance:
            continue
        note = dict(item)
        note["id"] = str(note.get("id", f"note-{utterance_id}")).strip() or f"note-{utterance_id}"
        note["utterance_id"] = utterance_id
        joke_unit_id = str(note.get("joke_unit_id", "")).strip()
        if joke_unit_id not in valid_unit_ids:
            joke_unit_id = next((str(unit.get("id", "")).strip() for unit in joke_units if utterance_id in set((unit.get("setup_ids", []) or []) + (unit.get("pivot_ids", []) or []) + (unit.get("punch_ids", []) or []) + (unit.get("tag_ids", []) or []))), "")
        note["joke_unit_id"] = joke_unit_id
        note.setdefault("comedy_function", utterance.get("comedy_function", "other"))
        note.setdefault("focus_type", {
            "pivot": "turn",
            "punch": "release",
            "tag": "tag",
            "callback": "release",
        }.get(str(utterance.get("comedy_function", "other")), "shape"))
        note.setdefault("title", _focus_title(str(note.get("comedy_function", "other")), str(note.get("focus_type", "shape")), utterance.get("delivery_tags", [])))
        note.setdefault("joke_role", str(utterance.get("joke_role", note.get("focus_type", "shape"))).strip() or str(note.get("focus_type", "shape")).strip() or "shape")
        note.setdefault("advice", "Coach the delivery of this audio span, not the wording.")
        note.setdefault("why", "This line is carrying an important stand up function.")
        note.setdefault("try_next", "Repeat the line and change only the delivery job of this moment.")
        tags = note.get("delivery_tags", utterance.get("delivery_tags", []))
        note["delivery_tags"] = [str(tag).strip() for tag in (tags if isinstance(tags, list) else []) if str(tag).strip()]
        note["time_range"] = utterance.get("time_range", [0.0, 0.0])
        note["script_range"] = utterance.get("script_range", {})
        note["quote"] = str(utterance.get("text", "")).strip()
        notes.append(note)
    return notes



def build_focused_coaching_notes(
    script: str,
    utterances: List[Dict],
    joke_units: List[Dict],
    style_preset: str = "",
    disable_llm_enrichment: bool = False,
) -> List[Dict]:
    baseline: List[Dict] = []
    utterance_by_id = {str(utt.get("id", "")).strip(): utt for utt in utterances}

    if joke_units:
        for unit in joke_units:
            unit_id = str(unit.get("id", "")).strip()
            ordered_ids = (
                list(unit.get("pivot_ids", []) or [])
                + list(unit.get("punch_ids", []) or [])
                + list(unit.get("tag_ids", []) or [])
            )
            for utterance_id in ordered_ids:
                utt = utterance_by_id.get(str(utterance_id).strip())
                if utt is None:
                    continue
                baseline.append(_fallback_note_for_utterance(utt, unit_id))
    else:
        for utt in utterances:
            if utt.get("is_focus_span"):
                baseline.append(_fallback_note_for_utterance(utt, ""))

    llm_notes = []
    if not disable_llm_enrichment:
        try:
            llm_notes = generate_focus_notes(
                script=script,
                utterances=utterances,
                joke_units=joke_units,
                style_preset=style_preset,
            )
        except Exception:
            llm_notes = []

    normalized_llm = _normalize_focus_notes(utterances, joke_units, llm_notes) if llm_notes else []

    baseline_by_utt = {
        str(note.get("utterance_id", "")).strip(): dict(note)
        for note in baseline
        if str(note.get("utterance_id", "")).strip()
    }

    for note in normalized_llm:
        utt_id = str(note.get("utterance_id", "")).strip()
        if not utt_id:
            continue
        if utt_id in baseline_by_utt:
            merged = dict(baseline_by_utt[utt_id])
            merged.update({k: v for k, v in note.items() if v not in (None, "", [], {})})
            baseline_by_utt[utt_id] = merged
        else:
            baseline_by_utt[utt_id] = dict(note)

    deduped = list(baseline_by_utt.values())

    if not deduped and utterances:
        focus_candidates = [utt for utt in utterances if utt.get("is_focus_span")]
        fallback_utt = focus_candidates[-1] if focus_candidates else utterances[-1]
        deduped.append(_fallback_note_for_utterance(fallback_utt, ""))

    def _note_rank(note: Dict) -> tuple:
        utt = utterance_by_id.get(str(note.get("utterance_id", "")).strip(), {})
        return (
            float(utt.get("laugh_bearing_score", 0.0) or 0.0),
            float(utt.get("supporting_score", 0.0) or 0.0),
            -int(utt.get("index", 9999) or 9999),
        )

    deduped.sort(key=_note_rank, reverse=True)
    return deduped



def _legacy_issue_type_from_note(note: Dict) -> str:
    tags = {str(tag).strip() for tag in (note.get("delivery_tags", []) or []) if str(tag).strip()}
    function_name = str(note.get("comedy_function", "")).strip()
    if "rushed_release" in tags or "rushed_build" in tags:
        return "speed-up"
    if "weak_release" in tags or (function_name == "punch" and str(note.get("focus_type", "")) == "release"):
        return "pause-too-short"
    if "flat_shape" in tags:
        return "tone-flat"
    if "weak_emphasis" in tags:
        return "unclear-emphasis"
    if function_name == "pivot":
        return "rhythm-break"
    return "unclear-emphasis"



def build_compatibility_markers_from_focus_notes(focus_notes: List[Dict]) -> List[Dict]:
    markers = []
    for idx, note in enumerate(focus_notes, start=1):
        issue_type = _legacy_issue_type_from_note(note)
        severity = 0.55 + 0.1 * min(3, len(note.get("delivery_tags", []) or []))
        markers.append(
            {
                "id": f"mk-{idx}-{uuid.uuid4().hex[:6]}",
                "time_range": note.get("time_range", [0.0, 0.0]),
                "script_range": note.get("script_range", {}),
                "issue_type": issue_type,
                "severity": round(min(0.95, severity), 3),
                "instruction": str(note.get("advice", "")).strip(),
                "rationale": str(note.get("why", "")).strip(),
                "demo_text": str(note.get("quote", "")).strip(),
                "window_source": "utterance-focus",
                "utterance_id": str(note.get("utterance_id", "")).strip(),
                "focus_type": str(note.get("focus_type", "")).strip(),
                "comedy_function": str(note.get("comedy_function", "")).strip(),
            }
        )
    return markers



def _legacy_markers_from_focus_notes(focus_notes: List[Dict]) -> List[Dict]:
    return build_compatibility_markers_from_focus_notes(focus_notes)



def analyze_rehearsal_take(
    script: str,
    transcript_segments: List[Dict],
    style_preset: str = "",
    marker_generator: Optional[Callable] = None,
    audio_bytes: bytes = b"",
    audio_filename: str = "",
    disable_llm_enrichment: bool = False,
) -> Dict:
    utterances = build_utterances_from_transcript(transcript_segments=transcript_segments)
    alignment = align_transcript_to_script(script=script, transcript_segments=transcript_segments)
    utterances = attach_alignment_to_utterances(utterances, alignment)

    profile_map = extract_utterance_audio_profiles(
        audio_bytes=audio_bytes,
        audio_filename=audio_filename,
        utterances=utterances,
    )
    for utt in utterances:
        utt["audio_features"] = profile_map.get(str(utt.get("id", "")).strip(), {})

    utterances = annotate_comedy_functions(
        script=script,
        utterances=utterances,
        style_preset=style_preset,
        disable_llm_enrichment=disable_llm_enrichment,
    )
    joke_units = build_joke_units(utterances)
    focus_notes = build_focused_coaching_notes(
        script=script,
        utterances=utterances,
        joke_units=joke_units,
        style_preset=style_preset,
        disable_llm_enrichment=disable_llm_enrichment,
    )

    markers = build_compatibility_markers_from_focus_notes(focus_notes)
    if marker_generator:
        try:
            compatibility_windows = [
                {
                    "time_range": utt.get("time_range", [0.0, 0.0]),
                    "script_range": utt.get("script_range", {}),
                    "segment_text": utt.get("segment_text", utt.get("text", "")),
                    "transcript_text": utt.get("text", ""),
                    "gap_before": utt.get("gap_before"),
                    "window_source": "utterance-focus" if utt.get("is_focus_span") else "utterance",
                }
                for utt in utterances
            ]
            raw_markers = marker_generator(
                script,
                compatibility_windows,
                style_preset=style_preset,
                audio_profiles={idx: profile_map.get(str(utt.get("id", "")).strip(), {}) for idx, utt in enumerate(utterances)},
            )
            normalized = _normalize_generated_markers(raw_markers or [], compatibility_windows, style_preset=style_preset)
            if normalized:
                markers = select_top_markers(normalized)
        except Exception:
            logger.exception("compatibility marker generator failed")

    logger.info(
        "rehearsal analyzed: aligned=%s utterances=%s focus_notes=%s markers=%s",
        len(alignment.get("aligned_segments", [])),
        len(utterances),
        len(focus_notes),
        len(markers),
    )
    return {
        "alignment": alignment,
        "utterances": utterances,
        "joke_units": joke_units,
        "focus_notes": focus_notes,
        "markers": markers,
    }


def compute_evidence_clip_range(
    marker_start: float,
    marker_end: float,
    audio_duration: float,
    target_len: float = 5.0,
    min_len: float = 3.0,
    max_len: float = 8.0,
) -> Tuple[float, float]:
    if audio_duration <= 0:
        return 0.0, 0.0
    target_len = max(min_len, min(max_len, target_len))
    midpoint = max(0.0, (marker_start + marker_end) / 2.0)
    half = target_len / 2.0
    start = max(0.0, midpoint - half)
    end = min(audio_duration, midpoint + half)

    duration = end - start
    if duration < target_len:
        shortage = target_len - duration
        shift_left = min(start, shortage / 2.0)
        start -= shift_left
        end = min(audio_duration, end + (shortage - shift_left))
        if end - start < target_len and end >= audio_duration:
            start = max(0.0, audio_duration - target_len)
            end = audio_duration

    if end - start < min_len and audio_duration >= min_len:
        end = min(audio_duration, start + min_len)
        if end - start < min_len:
            start = max(0.0, end - min_len)
    return round(start, 3), round(end, 3)


def _static_url_from_path(path: str) -> Optional[str]:
    static_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "static"))
    try:
        rel = os.path.relpath(path, static_root)
    except ValueError:
        return None
    return f"/static/{rel.replace(os.sep, '/')}"


def build_evidence_clip_url(
    audio_bytes: bytes,
    filename: str,
    marker_time_range: List[float],
    output_dir: str = "app/static/rehearsal/evidence",
) -> Optional[str]:
    if not audio_bytes:
        return None
    os.makedirs(output_dir, exist_ok=True)
    ext = os.path.splitext(filename or "")[1].lower() or ".wav"
    source_name = f"take-{uuid.uuid4().hex}{ext}"
    source_path = os.path.join(output_dir, source_name)
    with open(source_path, "wb") as file_obj:
        file_obj.write(audio_bytes)

    clip_stem = os.path.join(output_dir, f"evidence-{uuid.uuid4().hex}")
    clip_path = f"{clip_stem}.wav"
    try:
        start_hint = _to_float(marker_time_range[0] if isinstance(marker_time_range, list) and len(marker_time_range) >= 1 else 0.0, 0.0)
        end_hint = _to_float(marker_time_range[1] if isinstance(marker_time_range, list) and len(marker_time_range) >= 2 else start_hint, start_hint)
        if ext == ".wav":
            with wave.open(source_path, "rb") as wav_file:
                sample_rate = wav_file.getframerate()
                frame_count = wav_file.getnframes()
                channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                if sample_rate <= 0:
                    return _static_url_from_path(source_path)
                audio_duration = frame_count / float(sample_rate)
                clip_start, clip_end = compute_evidence_clip_range(
                    marker_start=start_hint,
                    marker_end=end_hint,
                    audio_duration=audio_duration,
                )
                if clip_end <= clip_start:
                    return _static_url_from_path(source_path)
                start_frame = int(clip_start * sample_rate)
                end_frame = int(clip_end * sample_rate)
                wav_file.setpos(start_frame)
                frames = wav_file.readframes(max(0, end_frame - start_frame))

            with wave.open(clip_path, "wb") as clip_file:
                clip_file.setnchannels(channels)
                clip_file.setsampwidth(sample_width)
                clip_file.setframerate(sample_rate)
                clip_file.writeframes(frames)
            return _static_url_from_path(clip_path)

        duration_completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                source_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
        audio_duration = _to_float(duration_completed.stdout.strip(), 0.0)
        if audio_duration <= 0:
            audio_duration = max(end_hint + 2.0, 5.0)
        clip_start, clip_end = compute_evidence_clip_range(
            marker_start=start_hint,
            marker_end=end_hint,
            audio_duration=audio_duration,
        )
        completed = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(max(0.0, clip_start)),
                "-to",
                str(max(clip_start, clip_end)),
                "-i",
                source_path,
                "-ac",
                "1",
                "-ar",
                "16000",
                clip_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode == 0 and os.path.exists(clip_path):
            return _static_url_from_path(clip_path)
        return _static_url_from_path(source_path)
    except Exception:
        return _static_url_from_path(source_path)
