from app.services.embedding_service import classify_style_label


def test_style_label_returns_label(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    label, conf = classify_style_label("A clean observational line.")
    assert isinstance(label, str)
    assert 0 <= conf <= 1


def test_style_label_uses_llm_when_key_set(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    import app.services.llm_service as llm_service

    monkeypatch.setattr(
        llm_service, "classify_style_label_llm", lambda text: ("precise", 0.9)
    )
    label, conf = classify_style_label("Any text.")
    assert label == "precise"
    assert conf == 0.9
