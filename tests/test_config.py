from app.config import Settings
import os


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    monkeypatch.setenv("PINECONE_API_KEY", "y")
    monkeypatch.setenv(
        "MYSQL_URL",
        "mysql+pymysql://user:password@127.0.0.1:3306/Talkshow?charset=utf8mb4",
    )
    s = Settings()
    assert s.openai_api_key == "x"
    assert s.pinecone_api_key == "y"
    assert "Talkshow" in s.mysql_url
    assert os.path.isabs(s.video_dataset_root)


def test_settings_supports_database_url_alias(monkeypatch):
    monkeypatch.delenv("MYSQL_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///artifacts/dev.db")
    s = Settings()
    assert s.mysql_url == "sqlite:///artifacts/dev.db"


def test_missing_openai_message_mentions_env_location(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("COMEDYCOACH_ENV_FILE", raising=False)
    s = Settings()
    message = s.missing_openai_key_message()
    assert "OPENAI_API_KEY is not set" in message
    assert ".env" in message
    assert s.project_root in message


def test_settings_exposes_config_diagnostics(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    s = Settings()
    diagnostics = s.config_diagnostics()
    assert diagnostics["project_root"] == s.project_root
    assert diagnostics["openai_api_key_present"] is True
    assert isinstance(diagnostics["checked_env_paths"], list)
