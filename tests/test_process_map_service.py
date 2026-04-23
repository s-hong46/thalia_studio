from app.services.process_map import (
    build_marker_feedback,
    build_similarity_process_map,
    link_references_to_markers,
)


def _sample_markers():
    return [
        {
            "id": "m-1",
            "time_range": [0.0, 2.0],
            "issue_type": "speed-up",
            "severity": 0.9,
            "instruction": "Slow down.",
            "rationale": "Pacing spikes.",
        },
        {
            "id": "m-2",
            "time_range": [2.0, 4.0],
            "issue_type": "tone-flat",
            "severity": 0.6,
            "instruction": "Add tone variation.",
            "rationale": "Tone stays flat.",
        },
    ]


def _sample_refs():
    return [
        {
            "title": "Clip A",
            "video_path": "movies/performer-a/a.mp4",
            "match_score": 0.82,
            "style_score": 0.76,
            "rhythm_score": 0.7,
        },
        {
            "title": "Clip B",
            "video_path": "movies/performer-b/b.mp4",
            "match_score": 0.78,
            "style_score": 0.74,
            "rhythm_score": 0.69,
        },
        {
            "title": "Clip C",
            "video_path": "movies/performer-c/c.mp4",
            "match_score": 0.71,
            "style_score": 0.7,
            "rhythm_score": 0.65,
        },
        {
            "title": "Clip D",
            "video_path": "movies/performer-d/d.mp4",
            "match_score": 0.66,
            "style_score": 0.62,
            "rhythm_score": 0.6,
        },
    ]


def test_link_references_to_markers_assigns_primary_marker():
    linked = link_references_to_markers(_sample_markers(), _sample_refs())
    assert linked
    assert linked[0]["primary_marker_id"] == "m-1"
    assert linked[0]["marker_ids"]
    assert linked[0]["performer_name"] == "performer-a"


def test_build_marker_feedback_includes_linked_reference_titles():
    linked = link_references_to_markers(_sample_markers(), _sample_refs())
    feedback = build_marker_feedback("observational", _sample_markers(), linked)
    assert "Detected style" in feedback["summary"]
    assert feedback["full_text"]
    assert feedback["items"]
    assert feedback["items"][0]["marker_id"] == "m-1"
    assert feedback["items"][0]["reference_titles"]
    assert feedback["items"][0]["paragraph"]


def test_build_similarity_process_map_returns_performer_nodes():
    linked = link_references_to_markers(_sample_markers(), _sample_refs())
    process_map = build_similarity_process_map("observational", _sample_markers(), linked)
    assert process_map["status"] == "ready"
    assert process_map["title"] == "Which Comedian Are You Most Like?"
    assert process_map["style_description"]
    assert process_map["top_performer"] in {"performer-a", "performer-b"}
    assert len(process_map["top_performers"]) == 3
    assert process_map["ai_summary"]
    assert process_map["performers"][0]["ai_note"]
    assert process_map["performers"][0]["style_summary"]
    assert any(node["type"] == "performer" for node in process_map["nodes"])
    assert process_map["edges"]


def test_link_references_to_markers_resolves_dataset_performer_name():
    refs = [
        {
            "title": "Clip From Dataset",
            "video_path": "movies/i0w0q-eu2Hk/sample.mp4",
            "match_score": 0.8,
            "style_score": 0.8,
            "rhythm_score": 0.8,
        }
    ]
    linked = link_references_to_markers(_sample_markers(), refs)
    assert linked[0]["performer_id"] == "i0w0q-eu2Hk"
