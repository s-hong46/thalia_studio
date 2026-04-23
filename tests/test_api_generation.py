from app import create_app
import app.routes.api as api_module


def test_punchlines_route(monkeypatch):
    monkeypatch.setattr(api_module, "generate_text", lambda prompt: "line one\nline two")
    app = create_app()
    client = app.test_client()
    r = client.post("/api/punchlines", json={"topic": "airports"})
    assert r.status_code == 200
