from app import create_app
from app.db import Base
import app.db as db_module


def _setup_in_memory(monkeypatch):
    monkeypatch.setenv("MYSQL_URL", "sqlite:///:memory:")
    db_module._engine = None
    Base.metadata.create_all(bind=db_module.get_engine())
    app = create_app()
    return app.test_client()


def test_style_presets_require_nickname(monkeypatch):
    client = _setup_in_memory(monkeypatch)
    response = client.get("/api/style-presets")
    assert response.status_code == 400


def test_style_presets_create_and_list(monkeypatch):
    client = _setup_in_memory(monkeypatch)

    create_res = client.post(
        "/api/style-presets",
        json={"nickname": "alex", "name": "dry observational"},
    )
    assert create_res.status_code == 200
    payload = create_res.get_json()
    assert payload["item"]["name"] == "dry observational"

    list_res = client.get("/api/style-presets?nickname=alex")
    assert list_res.status_code == 200
    list_payload = list_res.get_json()
    names = [item["name"] for item in list_payload["items"]]
    assert "dry observational" in names


def test_style_presets_isolated_by_user(monkeypatch):
    client = _setup_in_memory(monkeypatch)
    client.post(
        "/api/style-presets",
        json={"nickname": "alex", "name": "dry observational"},
    )
    client.post(
        "/api/style-presets",
        json={"nickname": "sam", "name": "storytelling"},
    )

    alex_res = client.get("/api/style-presets?nickname=alex")
    sam_res = client.get("/api/style-presets?nickname=sam")
    assert alex_res.status_code == 200
    assert sam_res.status_code == 200
    alex_names = [item["name"] for item in alex_res.get_json()["items"]]
    sam_names = [item["name"] for item in sam_res.get_json()["items"]]
    assert "dry observational" in alex_names
    assert "storytelling" not in alex_names
    assert "storytelling" in sam_names
