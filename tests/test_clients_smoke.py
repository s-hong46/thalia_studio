from app.services.openai_client import get_openai_client
from app.services.pinecone_client import get_pinecone, ensure_indexes


def test_clients_exist(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    monkeypatch.setenv("PINECONE_API_KEY", "y")
    assert get_openai_client() is not None
    assert get_pinecone() is not None
    assert callable(ensure_indexes)
