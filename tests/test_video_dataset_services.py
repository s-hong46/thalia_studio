import io
import struct
import wave

from app.services.video_dataset_ingest_service import (
    auto_video_dataset_ingest_enabled,
    begin_foreground_analysis,
    build_chunk_windows,
    end_foreground_analysis,
    foreground_analysis_active,
    get_video_dataset_status_payload,
)
from app.services.video_match_service import rank_video_candidates
import app.services.video_match_service as video_match_service


def test_build_chunk_windows_with_overlap():
    windows = build_chunk_windows(duration_sec=65.0, chunk_len=30.0, overlap=5.0)
    assert windows
    assert windows[0] == (0.0, 30.0)
    assert windows[1][0] == 25.0
    assert windows[-1][1] == 65.0


def test_rank_video_candidates_prefers_style_and_rhythm_match():
    user = {"style_label": "observational", "pace_wps": 2.2, "pause_density": 0.12}
    candidates = [
        {
            "id": "a",
            "semantic_score": 0.82,
            "style_label": "observational",
            "pace_wps": 2.1,
            "pause_density": 0.11,
        },
        {
            "id": "b",
            "semantic_score": 0.9,
            "style_label": "dark",
            "pace_wps": 3.8,
            "pause_density": 0.01,
        },
    ]
    ranked = rank_video_candidates(user_profile=user, candidates=candidates, top_k=2)
    assert ranked[0]["id"] == "a"
    assert ranked[0]["match_score"] >= ranked[1]["match_score"]


def test_video_dataset_status_uses_strict_enum():
    payload = get_video_dataset_status_payload()
    assert payload["status"] in {"scanning", "indexing", "ready", "error"}


def test_auto_video_dataset_ingest_defaults_off(monkeypatch):
    monkeypatch.delenv("AUTO_VIDEO_DATASET_INGEST", raising=False)
    assert auto_video_dataset_ingest_enabled() is False


def test_auto_video_dataset_ingest_respects_explicit_flag(monkeypatch):
    monkeypatch.setenv("AUTO_VIDEO_DATASET_INGEST", "1")
    assert auto_video_dataset_ingest_enabled() is True


def test_foreground_analysis_flag_balances():
    begin_foreground_analysis()
    assert foreground_analysis_active() is True
    end_foreground_analysis()
    assert foreground_analysis_active() is False


def test_match_video_references_uses_real_audio_energy(monkeypatch):
    samples = []
    for i in range(16000):
        val = 30000 if (i % 2 == 0) else -30000
        samples.append(struct.pack("<h", val))
    wav_buf = io.BytesIO()
    with wave.open(wav_buf, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(b"".join(samples))
    audio_bytes = wav_buf.getvalue()

    monkeypatch.setenv("PINECONE_API_KEY", "x")
    monkeypatch.setattr(video_match_service, "embed_text", lambda text: [0.1, 0.2, 0.3])

    class FakeIndex:
        def query(self, **kwargs):
            return {
                "matches": [
                    {
                        "score": 0.8,
                        "metadata": {
                            "asset_id": 1,
                            "chunk_id": 1,
                            "file_path": "movies/a.mp4",
                            "start_sec": 0.0,
                            "end_sec": 5.0,
                            "style_label": "observational",
                            "pace_wps": 2.0,
                            "pause_density": 0.1,
                            "energy_rms": 0.9,
                        },
                    },
                    {
                        "score": 0.8,
                        "metadata": {
                            "asset_id": 2,
                            "chunk_id": 2,
                            "file_path": "movies/b.mp4",
                            "start_sec": 0.0,
                            "end_sec": 5.0,
                            "style_label": "observational",
                            "pace_wps": 2.0,
                            "pause_density": 0.1,
                            "energy_rms": 0.1,
                        },
                    },
                ]
            }

    class FakePc:
        def Index(self, name):
            return FakeIndex()

    monkeypatch.setattr(video_match_service, "ensure_indexes", lambda: FakePc())

    refs = video_match_service.match_video_references(
        script="airport timing",
        transcript_segments=[{"start": 0.0, "end": 2.0, "text": "airport timing"}],
        markers=[],
        style_label="observational",
        audio_bytes=audio_bytes,
        audio_filename="take.wav",
        top_k=2,
        initial_top_k=2,
    )
    assert refs
    assert refs[0]["video_path"] == "movies/a.mp4"


def test_match_video_references_covers_each_marker_when_candidates_are_available(monkeypatch):
    monkeypatch.setenv("PINECONE_API_KEY", "x")
    monkeypatch.setattr(video_match_service, "embed_text", lambda text: [0.1, 0.2, 0.3])

    class FakeIndex:
        def query(self, **kwargs):
            return {
                "matches": [
                    {
                        "score": 0.91,
                        "metadata": {
                            "asset_id": 1,
                            "chunk_id": 1,
                            "file_path": "movies/a.mp4",
                            "start_sec": 0.0,
                            "end_sec": 5.0,
                            "style_label": "observational",
                            "pace_wps": 1.9,
                            "pause_density": 0.21,
                            "energy_rms": 0.55,
                            "transcript_excerpt": "pause before the punchline and let the beat sit",
                        },
                    },
                    {
                        "score": 0.88,
                        "metadata": {
                            "asset_id": 2,
                            "chunk_id": 2,
                            "file_path": "movies/b.mp4",
                            "start_sec": 10.0,
                            "end_sec": 15.0,
                            "style_label": "observational",
                            "pace_wps": 2.2,
                            "pause_density": 0.12,
                            "energy_rms": 0.62,
                            "transcript_excerpt": "lift the second clause with more amusement and smile in the voice",
                        },
                    },
                    {
                        "score": 0.87,
                        "metadata": {
                            "asset_id": 3,
                            "chunk_id": 3,
                            "file_path": "movies/c.mp4",
                            "start_sec": 20.0,
                            "end_sec": 25.0,
                            "style_label": "observational",
                            "pace_wps": 1.8,
                            "pause_density": 0.09,
                            "energy_rms": 0.84,
                            "transcript_excerpt": "push the final keyword harder and add more energy",
                        },
                    },
                    {
                        "score": 0.7,
                        "metadata": {
                            "asset_id": 4,
                            "chunk_id": 4,
                            "file_path": "movies/d.mp4",
                            "start_sec": 30.0,
                            "end_sec": 35.0,
                            "style_label": "observational",
                            "pace_wps": 2.5,
                            "pause_density": 0.08,
                            "energy_rms": 0.4,
                            "transcript_excerpt": "general delivery example",
                        },
                    },
                ]
            }

    class FakePc:
        def Index(self, name):
            return FakeIndex()

    monkeypatch.setattr(video_match_service, "ensure_indexes", lambda: FakePc())

    markers = [
        {
            "id": "m-1",
            "issue_type": "pause-too-short",
            "severity": 0.95,
            "instruction": "Hold the silence before the payoff.",
            "rationale": "The beat disappears.",
            "demo_text": "pause before the punchline",
        },
        {
            "id": "m-2",
            "issue_type": "tone-flat",
            "severity": 0.82,
            "instruction": "Lift the second clause with more amusement.",
            "rationale": "The comparison needs more shape.",
            "demo_text": "smile in the voice",
        },
        {
            "id": "m-3",
            "issue_type": "low-energy",
            "severity": 0.8,
            "instruction": "Push the final keyword harder.",
            "rationale": "The line needs more lift.",
            "demo_text": "add more energy",
        },
    ]

    refs = video_match_service.match_video_references(
        script="setup and payoff",
        transcript_segments=[{"start": 0.0, "end": 4.0, "text": "setup and payoff"}],
        markers=markers,
        style_label="observational",
        top_k=2,
        initial_top_k=4,
    )

    assert len(refs) >= 3
    covered_marker_ids = {ref["primary_marker_id"] for ref in refs if ref.get("primary_marker_id")}
    assert covered_marker_ids == {"m-1", "m-2", "m-3"}


def test_match_video_references_reuses_candidate_when_pool_is_too_small(monkeypatch):
    monkeypatch.setenv("PINECONE_API_KEY", "x")
    monkeypatch.setattr(video_match_service, "embed_text", lambda text: [0.1, 0.2, 0.3])

    class FakeIndex:
        def query(self, **kwargs):
            return {
                "matches": [
                    {
                        "score": 0.92,
                        "metadata": {
                            "asset_id": 1,
                            "chunk_id": 1,
                            "file_path": "movies/a.mp4",
                            "start_sec": 0.0,
                            "end_sec": 5.0,
                            "style_label": "observational",
                            "pace_wps": 2.0,
                            "pause_density": 0.2,
                            "energy_rms": 0.7,
                            "transcript_excerpt": "strong general example for both markers",
                        },
                    }
                ]
            }

    class FakePc:
        def Index(self, name):
            return FakeIndex()

    monkeypatch.setattr(video_match_service, "ensure_indexes", lambda: FakePc())

    refs = video_match_service.match_video_references(
        script="same example reused",
        transcript_segments=[{"start": 0.0, "end": 4.0, "text": "same example reused"}],
        markers=[
            {"id": "m-1", "issue_type": "pause-too-short", "severity": 0.9, "instruction": "pause", "demo_text": "pause"},
            {"id": "m-2", "issue_type": "low-energy", "severity": 0.8, "instruction": "energy", "demo_text": "energy"},
        ],
        style_label="observational",
        top_k=1,
        initial_top_k=1,
    )

    assert len(refs) == 2
    assert {ref["primary_marker_id"] for ref in refs} == {"m-1", "m-2"}
    assert {ref["video_path"] for ref in refs} == {"movies/a.mp4"}


def test_match_focus_note_videos_prefers_dataset_reference_index(monkeypatch):
    structured_calls = {"count": 0}

    monkeypatch.setattr(
        video_match_service,
        "load_dataset_reference_spans",
        lambda **kwargs: [
            {
                "id": "dataset-ref-1",
                "video_id": "Yp9LaRbLyLo",
                "asset_id": 0,
                "video_path": "",
                "watch_url": "https://www.youtube.com/watch?v=Yp9LaRbLyLo",
                "source_url": "",
                "title": "Parking Spot - Test Comic - Stand-Up Featuring",
                "channel": "Comedy Central Stand-Up",
                "performer_name": "Test Comic",
                "language": "en",
                "start_sec": 2.2,
                "end_sec": 3.6,
                "transcript_excerpt": "wanted my parking spot",
                "comedy_function": "punch",
                "focus_type": "release",
                "joke_role": "release",
                "function_confidence": 0.82,
                "delivery_tags": [],
                "quality_score": 0.91,
                "laughter_score": 0.96,
                "laugh_start_sec": 2.9,
                "laugh_end_sec": 3.6,
                "laugh_delay_sec": 0.0,
                "laugh_duration_sec": 0.7,
                "pace_wps": 2.1,
                "pause_before_sec": 0.18,
                "pause_density": 0.12,
                "energy_rms": 0.0,
                "style_label": "general",
                "match_text": "wanted my parking spot function:punch focus:release",
                "payload": {
                    "title": "Parking Spot - Test Comic - Stand-Up Featuring",
                    "why": "This laugh window lands on the reveal.",
                },
                "source_kind": "dataset-label+heuristic",
            }
        ],
    )
    monkeypatch.setattr(
        video_match_service,
        "load_structured_video_spans",
        lambda **kwargs: structured_calls.update({"count": structured_calls["count"] + 1}) or [],
    )

    groups = video_match_service.match_focus_note_videos(
        script="My dad said he'd die for me. Turns out he wanted my parking spot.",
        utterances=[
            {
                "id": "utt-1",
                "text": "Turns out he wanted my parking spot.",
                "time_range": [0.0, 2.0],
                "delivery_tags": ["weak_release"],
                "comedy_function": "punch",
                "joke_role": "release",
                "audio_features": {
                    "words_per_second": 2.0,
                    "pause_before": 0.08,
                    "rms_level": 0.12,
                },
                "context_before": "My dad said he'd die for me.",
            }
        ],
        focus_notes=[
            {
                "id": "note-1",
                "utterance_id": "utt-1",
                "comedy_function": "punch",
                "focus_type": "release",
                "advice": "Pause before the reveal.",
                "delivery_tags": ["weak_release"],
            }
        ],
        style_label="observational",
        top_k=1,
        initial_top_k=4,
    )

    assert groups
    assert groups[0]["items"]
    ref = groups[0]["items"][0]
    assert ref["watch_url"] == "https://www.youtube.com/watch?v=Yp9LaRbLyLo"
    assert ref["source_url"] == ""
    assert "Parking Spot - Test Comic - Stand-Up Featuring" in ref["title"]
    assert structured_calls["count"] == 0


def test_match_focus_note_videos_uses_transferability_adjudication(monkeypatch):
    monkeypatch.setattr(video_match_service, "_llm_reasoning_enabled", lambda: True)
    monkeypatch.setattr(
        video_match_service,
        "generate_pedagogical_retrieval_spec",
        lambda **kwargs: {
            "semantic_seed_query": "pause before the reveal",
            "retrieval_rationale": "Prefer reusable release examples.",
        },
    )
    monkeypatch.setattr(
        video_match_service,
        "screen_pedagogical_candidate",
        lambda pedagogical_spec, candidate, model="gpt-4o": {
            "candidate_id": candidate["id"],
            "hard_gates": {
                "functional_alignment": {"pass": True, "reason": "ok"},
                "demonstration_alignment": {"pass": True, "reason": "ok"},
                "pedagogical_visibility": {"pass": True, "reason": "ok"},
                "transfer_risk": {"pass": True, "reason": "ok"},
            },
            "screening_decision": "keep",
        },
    )
    monkeypatch.setattr(
        video_match_service,
        "adjudicate_transferable_candidate",
        lambda pedagogical_spec, candidates, model="gpt-5.2": {
            "selected_candidate_id": "dataset-ref-b",
            "why_this_clip": "This clip shows a cleaner delayed release.",
            "what_to_watch": "Watch the pause before the final turn.",
            "adaptation_guidance": "Borrow the separation, not the wording.",
            "transferability_rationale": "The performance problem is the same and the move is clearer here.",
            "portability_notes": "Different topic, same release logic.",
        },
    )
    monkeypatch.setattr(
        video_match_service,
        "load_dataset_reference_spans",
        lambda **kwargs: [
            {
                "id": "dataset-ref-a",
                "video_id": "A",
                "asset_id": 0,
                "video_path": "",
                "watch_url": "https://example.com/a",
                "source_url": "",
                "title": "Candidate A",
                "channel": "Test",
                "performer_name": "Comic A",
                "language": "en",
                "start_sec": 1.0,
                "end_sec": 3.0,
                "transcript_excerpt": "the reveal lands quickly",
                "comedy_function": "punch",
                "focus_type": "release",
                "joke_role": "release",
                "function_confidence": 0.82,
                "delivery_tags": [],
                "quality_score": 0.95,
                "laughter_score": 0.92,
                "laugh_start_sec": 2.2,
                "laugh_end_sec": 2.9,
                "laugh_delay_sec": 0.1,
                "laugh_duration_sec": 0.7,
                "pace_wps": 2.0,
                "pause_before_sec": 0.05,
                "pause_density": 0.08,
                "energy_rms": 0.15,
                "style_label": "observational",
                "match_text": "fast release example",
                "payload": {"title": "Candidate A", "why": "quick release"},
                "source_kind": "dataset-label+heuristic",
            },
            {
                "id": "dataset-ref-b",
                "video_id": "B",
                "asset_id": 0,
                "video_path": "",
                "watch_url": "https://example.com/b",
                "source_url": "",
                "title": "Candidate B",
                "channel": "Test",
                "performer_name": "Comic B",
                "language": "en",
                "start_sec": 4.0,
                "end_sec": 6.0,
                "transcript_excerpt": "the pause makes the reveal feel separate",
                "comedy_function": "punch",
                "focus_type": "release",
                "joke_role": "release",
                "function_confidence": 0.8,
                "delivery_tags": [],
                "quality_score": 0.72,
                "laughter_score": 0.8,
                "laugh_start_sec": 5.1,
                "laugh_end_sec": 5.8,
                "laugh_delay_sec": 0.2,
                "laugh_duration_sec": 0.7,
                "pace_wps": 1.8,
                "pause_before_sec": 0.2,
                "pause_density": 0.13,
                "energy_rms": 0.18,
                "style_label": "observational",
                "match_text": "delayed release example",
                "payload": {"title": "Candidate B", "why": "delayed release"},
                "source_kind": "dataset-label+heuristic",
            },
        ],
    )

    groups = video_match_service.match_focus_note_videos(
        script="He said he'd support me. Then he took my parking spot.",
        utterances=[
            {
                "id": "utt-1",
                "text": "Then he took my parking spot.",
                "time_range": [0.0, 2.0],
                "delivery_tags": ["weak_release"],
                "comedy_function": "punch",
                "joke_role": "release",
                "audio_features": {
                    "words_per_second": 2.2,
                    "pause_before": 0.04,
                    "rms_level": 0.14,
                },
                "context_before": "He said he'd support me.",
                "context_after": "",
            }
        ],
        focus_notes=[
            {
                "id": "note-1",
                "utterance_id": "utt-1",
                "quote": "Then he took my parking spot.",
                "comedy_function": "punch",
                "focus_type": "release",
                "advice": "Pause before the reveal.",
                "why": "The reveal arrives too attached to the setup.",
                "delivery_tags": ["weak_release"],
            }
        ],
        style_label="observational",
        top_k=1,
        initial_top_k=4,
    )

    assert groups
    assert groups[0]["items"]
    ref = groups[0]["items"][0]
    assert ref["watch_url"] == "https://example.com/b"
    assert ref["reason"] == "This clip shows a cleaner delayed release."
    assert ref["watch_hint"] == "Watch the pause before the final turn."
    assert ref["copy_action"] == "Borrow the separation, not the wording."
