import io

from app import create_app


def test_asr_transcribe_rejects_short_transcript(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(api_module, "transcribe_audio_file", lambda stream, filename: "hello")
    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/asr/transcribe",
        data={"audio": (io.BytesIO(b"fake"), "take.wav")},
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert "too short" in payload["error"]


def test_asr_transcribe_returns_text_when_long_enough(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_file",
        lambda stream, filename: "I hate alarm clocks every single morning",
    )
    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/asr/transcribe",
        data={"audio": (io.BytesIO(b"fake"), "take.wav")},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert "alarm clocks" in payload["text"]



def test_asr_transcribe_uses_fallback_text_when_key_missing(monkeypatch):
    import app.routes.api as api_module

    def _raise_missing_key(stream, filename):
        raise RuntimeError("OPENAI_API_KEY is not set")

    monkeypatch.setattr(api_module, "transcribe_audio_file", _raise_missing_key)
    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/asr/transcribe",
        data={
            "audio": (io.BytesIO(b"fake"), "take.wav"),
            "fallback_text": "I hate alarm clocks every single morning",
        },
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["source"] == "browser-fallback"
    assert "alarm clocks" in payload["text"]
