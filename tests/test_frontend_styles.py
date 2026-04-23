from pathlib import Path


def test_layout_style_hooks_exist():
    css = Path("app/static/styles.css").read_text(encoding="utf-8")
    assert ".review-cta" in css
    assert ".assistant-panel" in css
    assert ".asr-primary" in css
    assert ".asr-meta" in css
    assert ".asr-hint" in css
    assert ".rehearsal-tools" in css
    assert ".marker-timeline" in css
    assert ".analysis-script" in css
    assert ".marker-item" in css
    assert ".video-reference-list" in css
    assert ".detected-style" in css
