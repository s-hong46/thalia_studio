from app.services.embedding_service import classify_style_label


def test_style_label_returns_label(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    label, conf = classify_style_label("A dry observational line about traffic.")
    assert isinstance(label, str)
    assert 0 <= conf <= 1
