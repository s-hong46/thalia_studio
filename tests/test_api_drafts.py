from app import create_app
from app.db import get_engine, Base


def test_create_draft_route(monkeypatch):
    monkeypatch.setenv("MYSQL_URL", "sqlite:///:memory:")
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    app = create_app()
    client = app.test_client()
    r = client.post("/api/drafts", json={"nickname": "alex"})
    assert r.status_code == 200
