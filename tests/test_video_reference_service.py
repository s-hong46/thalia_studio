import app.services.video_reference_service as video_reference_service


def test_recommend_video_references_delegates_to_video_match_service(monkeypatch):
    called = {"count": 0}

    def _fake_match(**kwargs):
        called["count"] += 1
        return [{"title": "clip", "video_path": "movies/a.mp4"}]

    monkeypatch.setattr(video_reference_service, "match_video_references", _fake_match)

    refs = video_reference_service.recommend_video_references(
        script="airport security line is chaos",
        transcript_segments=[{"text": "airport line chaos"}],
        style_label="observational",
        issue_types=["speed-up"],
        top_k=2,
    )
    assert called["count"] == 1
    assert refs[0]["video_path"] == "movies/a.mp4"
