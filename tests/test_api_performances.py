from app import create_app
from app.db import Base
from app.models import User, Draft
import app.db as db_module
import app.routes.api as api_module


def test_performance_start(monkeypatch):
    monkeypatch.setenv("MYSQL_URL", "sqlite:///:memory:")
    db_module._engine = None
    Base.metadata.create_all(bind=db_module.get_engine())
    session = db_module.get_session()
    user = User(nickname="alex")
    session.add(user)
    session.commit()
    session.refresh(user)
    draft = Draft(user_id=user.id, title="test", content="hello")
    session.add(draft)
    session.commit()
    session.refresh(draft)
    session.close()

    monkeypatch.setattr(api_module, "generate_text", lambda prompt: "ok")
    app = create_app()
    client = app.test_client()
    r = client.post(
        "/api/performance/start", json={"draft_id": draft.id, "text": "hello"}
    )
    assert r.status_code == 200
