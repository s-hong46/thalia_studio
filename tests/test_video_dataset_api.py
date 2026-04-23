import io

from app import create_app


def test_video_dataset_status_endpoint(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "get_video_dataset_status_payload",
        lambda: {
            "status": "ready",
            "processed_files": 12,
            "pending_files": 0,
            "failed_files": 0,
            "last_error": "",
        },
    )
    app = create_app()
    client = app.test_client()
    response = client.get("/api/video-dataset/status")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ready"
    assert payload["processed_files"] == 12


def test_video_dataset_preview_requires_params():
    app = create_app()
    client = app.test_client()
    response = client.get("/api/video-dataset/preview")
    assert response.status_code == 400


def test_video_dataset_preview_returns_url(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "build_video_preview_clip",
        lambda asset_id, start_sec, end_sec: "/static/rehearsal/video_preview/demo.mp4",
    )
    app = create_app()
    client = app.test_client()
    response = client.get("/api/video-dataset/preview?asset_id=1&start_sec=0&end_sec=5")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["preview_url"] == "/static/rehearsal/video_preview/demo.mp4"


def test_rehearsal_analyze_accepts_include_video_dataset_alias(monkeypatch):
    import app.routes.api as api_module

    called = {"count": 0}
    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [{"start": 0.0, "end": 2.0, "text": "airport line and timing"}],
    )
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda script, transcript_segments, style_preset="", **kwargs: {
            "alignment": {
                "performed_script_range": {"char_start": 0, "char_end": 10},
                "script_segments": [],
                "aligned_segments": [],
            },
            "markers": [],
        },
    )
    monkeypatch.setattr(api_module, "generate_speech", lambda text: None)
    monkeypatch.setattr(api_module, "classify_style_label", lambda text: ("observational", 0.8))
    monkeypatch.setattr(
        api_module,
        "match_video_references",
        lambda **kwargs: called.update({"count": called["count"] + 1}) or [],
    )
    monkeypatch.setattr(
        api_module,
        "get_video_dataset_status_payload",
        lambda: {"status": "ready", "processed_files": 1, "pending_files": 0, "failed_files": 0, "last_error": ""},
    )

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate airports",
            "include_video_dataset": "0",
            "audio": (io.BytesIO(b"fake"), "take.wav"),
        },
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["video_dataset_enabled"] is False
    assert called["count"] == 0
