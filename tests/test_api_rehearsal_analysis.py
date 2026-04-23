import io
import json

from app import create_app


def test_rehearsal_analyze_requires_audio_and_script():
    app = create_app()
    client = app.test_client()
    response = client.post("/api/rehearsal/analyze", data={})
    assert response.status_code == 400


def test_rehearsal_analyze_returns_alignment_and_markers(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [
            {"start": 0.0, "end": 2.0, "text": "I hate alarm clocks"}
        ],
    )
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda script, transcript_segments, style_preset="", **kwargs: {
            "alignment": {
                "performed_script_range": {"char_start": 0, "char_end": 20},
                "script_segments": [],
                "aligned_segments": [],
            },
            "markers": [
                {
                    "id": "m-1",
                    "time_range": [0.0, 2.0],
                    "script_range": {"segment_id": "seg-1", "char_start": 0, "char_end": 20},
                    "issue_type": "speed-up",
                    "severity": 0.8,
                    "instruction": "Slow down before the punchline.",
                    "rationale": "Your pacing spikes near the end.",
                    "demo_text": "I hate alarm clocks.",
                }
            ],
        },
    )
    monkeypatch.setattr(api_module, "generate_speech", lambda text: "/static/tts/demo.mp3")
    monkeypatch.setattr(api_module, "build_evidence_url", lambda *args, **kwargs: "/static/rehearsal/evidence.wav")
    monkeypatch.setattr(
        api_module,
        "match_video_references",
        lambda **kwargs: [
            {
                "title": "Airport Bits",
                "preview_url": "/api/video-dataset/preview?asset_id=1&start_sec=0&end_sec=5",
                "video_path": "movies/i0w0q-eu2Hk/sample.mp4",
                "asset_id": 1,
                "start_sec": 0.0,
                "end_sec": 5.0,
                "reason": "style and rhythm match",
                "watch_hint": "watch the pause before punchline",
                "match_score": 0.86,
                "style_score": 0.8,
                "rhythm_score": 0.79,
                "performer_name": "i0w0q-eu2Hk",
            }
        ],
    )

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
            "style_preset": "dry observational",
        },
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert "alignment" in payload
    assert "markers" in payload
    assert payload["markers"][0]["evidence_audio_url"] == "/static/rehearsal/evidence.wav"
    assert payload["markers"][0]["demo_audio_url"] == "/static/tts/demo.mp3"
    assert payload["video_references"][0]["marker_ids"] == ["m-1"]
    assert payload["video_references"][0]["preview_url"].startswith("/api/video-dataset/preview")
    assert payload["video_references"][0]["preview_video_url"] == ""
    assert payload["feedback"]["items"][0]["marker_id"] == "m-1"
    assert "full_text" in payload["feedback"]
    assert payload["feedback"]["items"][0]["paragraph"]
    assert payload["process_map"]["status"] == "ready"
    assert payload["process_map"]["title"] == "Which Comedian Are You Most Like?"
    assert payload["process_map"]["style_description"]
    assert payload["process_map"]["performers"]
    assert payload["process_map"]["performers"][0]["name"]


def test_rehearsal_analyze_publishes_sse_when_draft_id_provided(monkeypatch):
    import app.routes.api as api_module

    events = []

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [
            {"start": 0.0, "end": 2.0, "text": "I hate alarm clocks"}
        ],
    )
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda script, transcript_segments, style_preset="", **kwargs: {
            "alignment": {
                "performed_script_range": {"char_start": 0, "char_end": 20},
                "script_segments": [],
                "aligned_segments": [],
            },
            "markers": [
                {
                    "id": "m-1",
                    "time_range": [0.0, 2.0],
                    "script_range": {
                        "segment_id": "seg-1",
                        "char_start": 0,
                        "char_end": 20,
                    },
                    "issue_type": "speed-up",
                    "severity": 0.8,
                    "instruction": "Slow down before the punchline.",
                    "rationale": "Your pacing spikes near the end.",
                    "demo_text": "I hate alarm clocks.",
                }
            ],
        },
    )
    monkeypatch.setattr(
        api_module, "generate_speech", lambda text: "/static/tts/demo.mp3"
    )
    monkeypatch.setattr(
        api_module,
        "build_evidence_url",
        lambda *args, **kwargs: "/static/rehearsal/evidence.wav",
    )
    monkeypatch.setattr(
        api_module,
        "publish_event",
        lambda draft_id, event, data: events.append((draft_id, event, data)),
    )
    monkeypatch.setattr(
        api_module,
        "match_video_references",
        lambda **kwargs: [
            {
                "title": "Clip A",
                "preview_url": "/api/video-dataset/preview?asset_id=1&start_sec=0&end_sec=5",
                "video_path": "movies/i0w0q-eu2Hk/sample.mp4",
                "asset_id": 1,
                "start_sec": 0.0,
                "end_sec": 5.0,
                "reason": "style and rhythm match",
                "watch_hint": "watch opening minute",
                "match_score": 0.8,
                "style_score": 0.7,
                "rhythm_score": 0.75,
                "performer_name": "i0w0q-eu2Hk",
            }
        ],
    )

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "draft_id": "1",
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
            "style_preset": "dry observational",
        },
    )
    assert response.status_code == 200
    assert len(events) == 1
    draft_id, event, payload = events[0]
    assert draft_id == "1"
    assert event == "rehearsal_analysis"
    parsed = json.loads(payload)
    assert parsed["script"] == "I hate alarm clocks."
    assert parsed["markers"][0]["demo_audio_url"] == "/static/tts/demo.mp3"
    assert parsed["feedback"]["items"][0]["marker_id"] == "m-1"
    assert parsed["feedback"]["full_text"]
    assert parsed["process_map"]["status"] in {"ready", "empty"}


def test_rehearsal_analyze_rejects_too_short_transcript(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [{"start": 0.0, "end": 0.6, "text": "hello"}],
    )

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
        },
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert "too short" in payload["error"]


def test_rehearsal_analyze_degrades_when_tts_fails(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [
            {"start": 0.0, "end": 2.0, "text": "I hate alarm clocks"}
        ],
    )
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda script, transcript_segments, style_preset="", **kwargs: {
            "alignment": {
                "performed_script_range": {"char_start": 0, "char_end": 20},
                "script_segments": [],
                "aligned_segments": [],
            },
            "markers": [
                {
                    "id": "m-1",
                    "time_range": [0.0, 2.0],
                    "script_range": {"segment_id": "seg-1", "char_start": 0, "char_end": 20},
                    "issue_type": "speed-up",
                    "severity": 0.8,
                    "instruction": "Slow down before the punchline.",
                    "rationale": "Your pacing spikes near the end.",
                    "demo_text": "I hate alarm clocks.",
                }
            ],
        },
    )
    monkeypatch.setattr(
        api_module,
        "generate_speech",
        lambda text: (_ for _ in ()).throw(RuntimeError("tts timeout")),
    )
    monkeypatch.setattr(
        api_module,
        "build_evidence_url",
        lambda *args, **kwargs: "/static/rehearsal/evidence.wav",
    )
    monkeypatch.setattr(api_module, "match_video_references", lambda **kwargs: [])

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
        },
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["markers"][0]["demo_audio_url"] is None


def test_rehearsal_analyze_video_reference_toggle(monkeypatch):
    import app.routes.api as api_module

    called = {"count": 0}

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda stream, filename: [
            {"start": 0.0, "end": 2.0, "text": "I hate alarm clocks and airport security lines"}
        ],
    )
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda script, transcript_segments, style_preset="", **kwargs: {
            "alignment": {
                "performed_script_range": {"char_start": 0, "char_end": 40},
                "script_segments": [],
                "aligned_segments": [],
            },
            "markers": [
                {
                    "id": "m-1",
                    "time_range": [0.0, 2.0],
                    "script_range": {"segment_id": "seg-1", "char_start": 0, "char_end": 20},
                    "issue_type": "speed-up",
                    "severity": 0.8,
                    "instruction": "Slow down before the punchline.",
                    "rationale": "Your pacing spikes near the end.",
                    "demo_text": "I hate alarm clocks.",
                }
            ],
        },
    )
    monkeypatch.setattr(
        api_module, "classify_style_label", lambda text: ("observational", 0.82)
    )
    monkeypatch.setattr(api_module, "generate_speech", lambda text: "/static/tts/demo.mp3")
    monkeypatch.setattr(api_module, "build_evidence_url", lambda *args, **kwargs: "/static/rehearsal/evidence.wav")
    monkeypatch.setattr(
        api_module,
        "match_video_references",
        lambda **kwargs: called.update({"count": called["count"] + 1}) or [
            {
                "title": "Airport Bits",
                "preview_url": "/api/video-dataset/preview?asset_id=1&start_sec=0&end_sec=5",
                "reason": "topic overlap: airport",
                "watch_hint": "watch opening minute",
                "style_label": "observational",
                "score": 2.3,
            }
        ],
    )

    app = create_app()
    client = app.test_client()

    off_response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
            "include_video_reference": "0",
        },
    )
    assert off_response.status_code == 200
    off_payload = off_response.get_json()
    assert off_payload["video_reference_enabled"] is False
    assert off_payload["video_references"] == []
    assert called["count"] == 0

    on_response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks.",
            "audio": (io.BytesIO(b"fake audio"), "take.wav"),
            "include_video_reference": "1",
        },
    )
    assert on_response.status_code == 200
    on_payload = on_response.get_json()
    assert on_payload["video_reference_enabled"] is True
    assert on_payload["style_detection"]["label"] == "observational"
    assert on_payload["video_references"][0]["preview_url"].startswith("/api/video-dataset/preview")
    assert called["count"] == 1



def test_rehearsal_analysis_uses_browser_transcript_when_asr_key_missing(monkeypatch):
    import app.routes.api as api_module

    monkeypatch.setattr(
        api_module,
        "transcribe_audio_segments",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("OPENAI_API_KEY is not set")),
    )
    monkeypatch.setattr(api_module, "classify_style_label", lambda text: ("general", 0.5))
    monkeypatch.setattr(
        api_module,
        "analyze_rehearsal_take",
        lambda **kwargs: {
            "alignment": {"performed_script_range": {"char_start": 0, "char_end": len(kwargs.get("script", ""))}},
            "markers": [
                {
                    "id": "mk-1",
                    "time_range": [0.0, 1.0],
                    "script_range": {"segment_id": "fallback-1", "char_start": 0, "char_end": 10},
                    "issue_type": "unclear-emphasis",
                    "severity": 0.6,
                    "instruction": "Stress the key word more clearly.",
                    "rationale": "Fallback analysis.",
                    "demo_text": "demo",
                }
            ],
        },
    )
    monkeypatch.setattr(api_module, "get_video_dataset_status_payload", lambda: {"status": "ready"})
    monkeypatch.setattr(api_module, "match_video_references", lambda **kwargs: [])
    monkeypatch.setattr(api_module, "link_references_to_markers", lambda markers, video_references: video_references)
    monkeypatch.setattr(api_module, "_hydrate_video_preview_urls", lambda refs: refs)
    monkeypatch.setattr(api_module, "match_comedian_profiles", lambda **kwargs: [])
    monkeypatch.setattr(api_module, "build_text_only_feedback", lambda **kwargs: {"summary": "ok", "items": []})
    monkeypatch.setattr(api_module, "build_similarity_process_map", lambda **kwargs: {"status": "ok", "nodes": [], "edges": []})
    monkeypatch.setattr(api_module, "_safe_generate_marker_demo_audio", lambda text: "")
    monkeypatch.setattr(api_module, "build_evidence_url", lambda **kwargs: "")

    app = create_app()
    client = app.test_client()
    response = client.post(
        "/api/rehearsal/analyze",
        data={
            "script": "I hate alarm clocks every single morning. They always betray me.",
            "transcript_text": "I hate alarm clocks every single morning. They always betray me.",
            "audio": (io.BytesIO(b"fake"), "take.wav"),
        },
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["transcript_source"] == "browser-fallback"
    assert payload["markers"][0]["id"] == "mk-1"


def test_split_fallback_transcript_text_breaks_long_run_on_weak_boundaries():
    import app.routes.api as api_module

    text = "So my real advice to everyone is to find yourself more loser friends, that's right, that's right, that's right, I see some of you guys clapping, the ones that are not, you're probably the loser friend."
    parts = api_module._split_fallback_transcript_text(text)

    assert len(parts) >= 3
    assert any("loser friends" in part for part in parts)
    assert any("clapping" in part for part in parts)
