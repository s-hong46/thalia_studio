from app import create_app
from app.db import get_engine, Base


def test_draft_list_route(monkeypatch):
    monkeypatch.setenv("MYSQL_URL", "sqlite:///:memory:")
    Base.metadata.create_all(bind=get_engine())
    app = create_app()
    client = app.test_client()
    r = client.get("/api/drafts?nickname=alex")
    assert r.status_code == 200
