import io

from app.config import Settings
from app.services.openai_client import get_openai_client


def _is_unsupported_response_format_error(err: Exception) -> bool:
    message = str(err).lower()
    return "response_format" in message and (
        "unsupported_value" in message
        or "not compatible" in message
        or "not support" in message
    )


def _is_invalid_language_error(err: Exception) -> bool:
    message = str(err).lower()
    return "language" in message and (
        "invalid_value" in message
        or "not recognized" in message
        or "not supported" in message
    )


def _make_audio_payload(audio_bytes: bytes, filename: str):
    payload = io.BytesIO(audio_bytes)
    payload.name = filename or "audio.webm"
    return payload


def _build_transcription_kwargs(
    settings: Settings,
    audio_bytes: bytes,
    filename: str,
    response_format: str,
    include_language: bool = True,
):
    kwargs = {
        "model": settings.asr_model,
        "file": _make_audio_payload(audio_bytes, filename),
        "response_format": response_format,
    }
    if include_language and settings.asr_language:
        kwargs["language"] = settings.asr_language
    return kwargs


def _request_transcription(
    client,
    settings: Settings,
    audio_bytes: bytes,
    filename: str,
    response_format: str,
):
    kwargs = _build_transcription_kwargs(
        settings=settings,
        audio_bytes=audio_bytes,
        filename=filename,
        response_format=response_format,
        include_language=True,
    )
    try:
        return client.audio.transcriptions.create(**kwargs)
    except Exception as err:
        if not (settings.asr_language and _is_invalid_language_error(err)):
            raise
        retry_kwargs = _build_transcription_kwargs(
            settings=settings,
            audio_bytes=audio_bytes,
            filename=filename,
            response_format=response_format,
            include_language=False,
        )
        return client.audio.transcriptions.create(**retry_kwargs)


def transcribe_audio_file(file_obj, filename: str):
    settings = Settings()
    if not settings.openai_api_key:
        raise RuntimeError(settings.missing_openai_key_message())
    audio_bytes = file_obj.read()
    if not audio_bytes:
        return ""
    client = get_openai_client()
    transcript = _request_transcription(
        client=client,
        settings=settings,
        audio_bytes=audio_bytes,
        filename=filename,
        response_format="text",
    )
    return str(transcript).strip()


def transcribe_audio_segments(file_obj, filename: str):
    settings = Settings()
    if not settings.openai_api_key:
        raise RuntimeError(settings.missing_openai_key_message())
    audio_bytes = file_obj.read()
    if not audio_bytes:
        return []
    client = get_openai_client()
    try:
        transcript = _request_transcription(
            client=client,
            settings=settings,
            audio_bytes=audio_bytes,
            filename=filename,
            response_format="verbose_json",
        )
    except Exception as err:
        if not _is_unsupported_response_format_error(err):
            raise
        transcript = _request_transcription(
            client=client,
            settings=settings,
            audio_bytes=audio_bytes,
            filename=filename,
            response_format="json",
        )

    segments = getattr(transcript, "segments", None)
    if segments is None and isinstance(transcript, dict):
        segments = transcript.get("segments")

    if not segments:
        if isinstance(transcript, dict):
            text = str(transcript.get("text", "")).strip()
        else:
            text = str(getattr(transcript, "text", "")).strip()
        if not text:
            text = str(transcript).strip()
        if not text:
            return []
        approx_duration = max(1.0, len(text.split()) / 2.8)
        return [{"start": 0.0, "end": round(approx_duration, 3), "text": text}]

    normalized = []
    for segment in segments:
        if isinstance(segment, dict):
            start = float(segment.get("start", 0.0))
            end = float(segment.get("end", start))
            text = str(segment.get("text", "")).strip()
        else:
            start = float(getattr(segment, "start", 0.0))
            end = float(getattr(segment, "end", start))
            text = str(getattr(segment, "text", "")).strip()
        if not text:
            continue
        normalized.append({"start": start, "end": end, "text": text})
    return normalized
