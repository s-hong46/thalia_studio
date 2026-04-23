from app import create_app


def test_index_page():
    app = create_app()
    client = app.test_client()
    r = client.get("/")
    assert r.status_code == 200


def test_index_layout_sections():
    app = create_app()
    client = app.test_client()
    r = client.get("/")
    html = r.get_data(as_text=True)
    assert 'data-section="writing-area"' in html
    assert 'data-section="performance-review"' in html
    assert 'data-section="feedback-panel"' in html
    assert 'data-section="study-videos"' in html
    assert 'data-section="asr-cta"' in html
    assert 'data-section="rehearsal-analysis"' in html
    assert 'data-section="focused-note"' in html


def test_index_layout_order():
    app = create_app()
    client = app.test_client()
    r = client.get("/")
    html = r.get_data(as_text=True)
    left_idx = html.find('data-column="left"')
    center_idx = html.find('data-column="center"')
    right_idx = html.find('data-column="right"')
    assert left_idx < center_idx < right_idx
    writing_idx = html.find('data-section="writing-area"')
    review_idx = html.find('data-section="performance-review"')
    assert writing_idx < review_idx


def test_rehearsal_controls_exist():
    app = create_app()
    client = app.test_client()
    r = client.get("/")
    html = r.get_data(as_text=True)
    assert 'id="analyzeRehearsalBtn"' in html
    assert 'id="rehearsalAudioInput"' in html
    assert 'id="transcriptView"' in html
    assert 'id="focusedNoteCard"' in html
    assert 'id="saveDraftBtn"' in html
    assert 'id="includeVideoRef"' in html
    assert 'id="videoReferenceList"' in html
    assert 'id="stylePresetSelect"' in html
    assert 'id="saveStylePresetBtn"' in html
    assert "Who to Watch" in html
