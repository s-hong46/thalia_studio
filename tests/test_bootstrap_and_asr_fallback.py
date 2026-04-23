import io

from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool


def test_create_app_bootstraps_tables(monkeypatch):
    import app.db as db_module
    from app import create_app

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    monkeypatch.setenv("MYSQL_URL", "sqlite://")
    db_module._engine = engine
    db_module._session_factory = None
    db_module._schema_ready = False
    db_module._schema_url = ""
    monkeypatch.setattr(db_module, "get_engine", lambda: engine)

    create_app()
    table_names = set(inspect(engine).get_table_names())

    assert "users" in table_names
    assert "style_presets" in table_names
    assert "video_assets" in table_names
    assert "video_chunks" in table_names
    assert "video_spans" in table_names
    assert "dataset_reference_spans" in table_names


def test_transcribe_audio_segments_fallbacks_to_json(monkeypatch):
    import app.services.asr_service as asr_service

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    requested_formats = []

    class FakeTranscriptions:
        def create(self, **kwargs):
            requested_formats.append(kwargs.get("response_format"))
            if kwargs.get("response_format") == "verbose_json":
                raise RuntimeError(
                    "unsupported_value: response_format 'verbose_json' is not compatible "
                    "with model 'gpt-4o-mini-transcribe-api-ev3'"
                )
            return {"text": "alarm clocks betray me"}

    class FakeAudio:
        transcriptions = FakeTranscriptions()

    class FakeClient:
        audio = FakeAudio()

    monkeypatch.setattr(asr_service, "get_openai_client", lambda: FakeClient())

    segments = asr_service.transcribe_audio_segments(io.BytesIO(b"fake"), "take.wav")

    assert requested_formats == ["verbose_json", "json"]
    assert len(segments) == 1
    assert segments[0]["text"] == "alarm clocks betray me"


def test_transcribe_audio_segments_retries_without_language(monkeypatch):
    import app.services.asr_service as asr_service

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_ASR_LANGUAGE", "English")
    language_values = []

    class FakeTranscriptions:
        def create(self, **kwargs):
            language_values.append(kwargs.get("language"))
            if kwargs.get("language"):
                raise RuntimeError(
                    "invalid_value: Language code 'English' is not recognized. "
                    "param: 'language'"
                )
            return {"text": "this one works"}

    class FakeAudio:
        transcriptions = FakeTranscriptions()

    class FakeClient:
        audio = FakeAudio()

    monkeypatch.setattr(asr_service, "get_openai_client", lambda: FakeClient())

    segments = asr_service.transcribe_audio_segments(io.BytesIO(b"fake"), "take.wav")

    assert language_values == ["English", None]
    assert len(segments) == 1
    assert segments[0]["text"] == "this one works"
