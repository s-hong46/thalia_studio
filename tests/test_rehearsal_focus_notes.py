from app.services.rehearsal_service import analyze_rehearsal_take


def test_analyze_rehearsal_take_returns_utterances_and_focus_notes():
    script = (
        "I thought moving to Virginia would make me an adult. "
        "Then I realized adulthood is just paying to park near your own apartment."
    )
    transcript_segments = [
        {"start": 0.0, "end": 2.4, "text": "I thought moving to Virginia would make me an adult."},
        {"start": 2.5, "end": 5.7, "text": "Then I realized adulthood is just paying to park near your own apartment."},
    ]
    result = analyze_rehearsal_take(
        script=script,
        transcript_segments=transcript_segments,
        style_preset="observational",
        audio_bytes=b"",
        audio_filename="sample.webm",
    )
    assert isinstance(result.get("utterances"), list)
    assert len(result["utterances"]) == 2
    assert all("time_range" in item for item in result["utterances"])
    assert isinstance(result.get("focus_notes"), list)
    assert result["focus_notes"]
    assert all(item.get("utterance_id") for item in result["focus_notes"])
    assert all(item.get("advice") for item in result["focus_notes"])
