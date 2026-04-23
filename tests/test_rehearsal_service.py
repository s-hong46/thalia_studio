from app.services import rehearsal_service


def test_compute_evidence_clip_range_prefers_five_seconds():
    start, end = rehearsal_service.compute_evidence_clip_range(
        marker_start=10.0, marker_end=11.0, audio_duration=30.0
    )
    assert round(end - start, 2) == 5.0
    assert round(start, 2) == 8.0
    assert round(end, 2) == 13.0


def test_compute_evidence_clip_range_clamped_by_audio_bounds():
    start, end = rehearsal_service.compute_evidence_clip_range(
        marker_start=0.1, marker_end=0.2, audio_duration=2.4
    )
    assert start == 0.0
    assert round(end, 2) == 2.4


def test_select_top_markers_dedup_and_cap():
    markers = [
        {"id": "1", "issue_type": "speed-up", "time_range": [10.0, 12.0], "severity": 0.9},
        {"id": "2", "issue_type": "speed-up", "time_range": [10.5, 12.5], "severity": 0.85},
        {"id": "3", "issue_type": "low-energy", "time_range": [15.0, 16.0], "severity": 0.8},
        {"id": "4", "issue_type": "falling-intonation", "time_range": [20.0, 21.0], "severity": 0.7},
        {"id": "5", "issue_type": "pause-too-short", "time_range": [25.0, 26.0], "severity": 0.6},
        {"id": "6", "issue_type": "unclear-emphasis", "time_range": [30.0, 31.0], "severity": 0.5},
        {"id": "7", "issue_type": "speed-up", "time_range": [40.0, 41.0], "severity": 0.4},
    ]
    top = rehearsal_service.select_top_markers(markers, limit=5)
    ids = [item["id"] for item in top]
    assert len(top) == 5
    assert "1" in ids
    assert "2" not in ids


def test_align_transcript_to_script_sentence_level():
    script = "I hate alarm clocks. They are tiny daily betrayals."
    transcript_segments = [
        {"start": 0.0, "end": 1.8, "text": "I really hate alarm clocks"},
        {"start": 2.0, "end": 4.5, "text": "they are tiny daily betrayal"},
    ]
    result = rehearsal_service.align_transcript_to_script(
        script=script,
        transcript_segments=transcript_segments,
    )
    assert len(result["script_segments"]) == 2
    assert len(result["aligned_segments"]) >= 1
    assert result["performed_script_range"]["char_end"] > result["performed_script_range"]["char_start"]


def test_marker_windows_use_combined_strategy():
    aligned_segments = [
        {
            "time_range": [0.0, 1.2],
            "script_range": {"segment_id": "seg-1", "char_start": 0, "char_end": 18},
            "segment_text": "I hate alarm clocks.",
            "transcript_text": "I hate alarm clocks",
        },
        {
            "time_range": [1.4, 3.0],
            "script_range": {"segment_id": "seg-2", "char_start": 19, "char_end": 40},
            "segment_text": "They betray me daily.",
            "transcript_text": "They betray me daily",
        },
    ]
    windows = rehearsal_service._build_marker_windows(aligned_segments)
    sources = {item["window_source"] for item in windows}
    assert "punchline-candidate" in sources
    assert "sentence-boundary" in sources
    assert len(windows) >= len(aligned_segments)


def test_analyze_rehearsal_take_prefers_marker_generator():
    script = "I hate alarm clocks. They betray me daily."
    transcript_segments = [
        {"start": 0.0, "end": 1.8, "text": "I hate alarm clocks"},
        {"start": 2.0, "end": 4.2, "text": "They betray me daily"},
    ]

    def fake_marker_generator(script, windows, style_preset="", audio_profiles=None):
        assert script
        assert windows
        return [
            {
                "time_range": windows[0]["time_range"],
                "script_range": windows[0]["script_range"],
                "issue_type": "low-energy",
                "severity": 0.91,
                "instruction": "Lift the emotional contrast.",
                "rationale": "Energy stays flat near the beat.",
                "demo_text": windows[0]["segment_text"],
            }
        ]

    result = rehearsal_service.analyze_rehearsal_take(
        script=script,
        transcript_segments=transcript_segments,
        style_preset="dry",
        marker_generator=fake_marker_generator,
    )
    assert result["markers"]
    assert result["markers"][0]["issue_type"] == "low-energy"


def test_analyze_rehearsal_take_falls_back_when_generator_empty():
    script = "I hate alarm clocks. They betray me daily."
    transcript_segments = [
        {"start": 0.0, "end": 1.8, "text": "I hate alarm clocks"},
    ]

    result = rehearsal_service.analyze_rehearsal_take(
        script=script,
        transcript_segments=transcript_segments,
        marker_generator=lambda *args, **kwargs: [],
    )
    assert result["markers"]
    assert result["markers"][0]["issue_type"] in {
        "pause-too-short",
        "speed-up",
        "low-energy",
        "falling-intonation",
        "unclear-emphasis",
    }


def test_analyze_rehearsal_take_generates_markers_when_alignment_empty():
    script = "I hate alarm clocks. They betray me daily."
    transcript_segments = [
        {"start": 0.0, "end": 2.0, "text": "completely different words here"},
    ]

    result = rehearsal_service.analyze_rehearsal_take(
        script=script,
        transcript_segments=transcript_segments,
        marker_generator=lambda *args, **kwargs: [],
    )
    assert result["alignment"]["aligned_segments"] == []
    assert result["markers"]


def test_build_utterances_from_transcript_splits_long_segment_into_multiple_beats():
    transcript_segments = [
        {
            "start": 0.0,
            "end": 9.0,
            "text": "How are they got successful family? They don't have time to hang out with mediocre ass. So my real advice to everyone is to find yourself more loser friends. That's right, that's right, that's right. I see some of you guys clapping.",
        }
    ]

    utterances = rehearsal_service.build_utterances_from_transcript(transcript_segments)

    assert len(utterances) >= 4
    assert utterances[0]["time_range"][0] == 0.0
    assert utterances[-1]["time_range"][1] == 9.0
    assert all(utterances[i]["time_range"][1] <= utterances[i + 1]["time_range"][0] for i in range(len(utterances) - 1))


def test_build_focused_coaching_notes_preserves_baseline_when_llm_returns_single_note(monkeypatch):
    utterances = [
        {
            "id": "utt-1",
            "index": 0,
            "text": "setup line",
            "time_range": [0.0, 1.0],
            "script_range": {},
            "comedy_function": "pivot",
            "delivery_tags": [],
            "context_before": "",
            "context_after": "punch line",
            "audio_features": {},
            "laugh_bearing_score": 0.6,
            "supporting_score": 0.5,
        },
        {
            "id": "utt-2",
            "index": 1,
            "text": "punch line",
            "time_range": [1.0, 2.0],
            "script_range": {},
            "comedy_function": "punch",
            "delivery_tags": ["weak_release"],
            "context_before": "setup line",
            "context_after": "",
            "audio_features": {},
            "laugh_bearing_score": 0.95,
            "supporting_score": 0.7,
        },
    ]
    joke_units = [{"id": "joke-1", "setup_ids": [], "pivot_ids": ["utt-1"], "punch_ids": ["utt-2"], "tag_ids": []}]

    monkeypatch.setattr(
        rehearsal_service,
        "generate_focus_notes",
        lambda **kwargs: [
            {
                "utterance_id": "utt-2",
                "title": "Custom punch note",
                "advice": "Sharpen the release.",
            }
        ],
    )

    notes = rehearsal_service.build_focused_coaching_notes(
        script="setup line. punch line.",
        utterances=utterances,
        joke_units=joke_units,
        style_preset="",
    )

    ids = [note["utterance_id"] for note in notes]
    assert ids == ["utt-2", "utt-1"]
    assert notes[0]["title"] == "Custom punch note"
