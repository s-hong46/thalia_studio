import os
import uuid

from app.config import Settings
from app.services.openai_client import get_openai_client


def generate_speech(text: str):
    if not text or not text.strip():
        return None
    settings = Settings()
    if not settings.openai_api_key:
        return None
    output_dir = settings.tts_output_dir or "app/static/tts"
    os.makedirs(output_dir, exist_ok=True)
    ext = settings.tts_format or "mp3"
    filename = f"review-{uuid.uuid4().hex}.{ext}"
    path = os.path.join(output_dir, filename)
    client = get_openai_client(timeout_sec=min(float(settings.openai_timeout_sec or 45.0), 20.0))
    response_format = settings.tts_format or "mp3"
    try:
        speech = client.audio.speech.create(
            model=settings.tts_model,
            voice=settings.tts_voice,
            input=text,
            response_format=response_format,
        )
        speech.write_to_file(path)
    except Exception:
        return None
    static_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "static")
    )
    try:
        rel = os.path.relpath(path, static_root)
        rel = rel.replace(os.sep, "/")
        return f"/static/{rel}"
    except ValueError:
        return None
