from app.services.llm_service import (
    build_audience_prompt,
    build_critic_prompt,
    build_feedback_prompt,
    build_functional_screening_prompt,
    build_pedagogical_abstraction_prompt,
    build_performer_prompt,
    build_punchline_prompt,
    build_review_prompt,
    build_transferability_adjudication_prompt,
)


def test_prompt_includes_topic():
    p = build_punchline_prompt("airports")
    assert "airports" in p


def test_feedback_prompt_includes_examples():
    p = build_feedback_prompt("text", ["bad pattern"])
    assert "bad pattern" in p


def test_performance_prompts():
    assert "Performer" in build_performer_prompt("x")
    assert "Critic" in build_critic_prompt("x", "y")
    assert "Audience" in build_audience_prompt("x", "y")


def test_review_prompt_requests_sections():
    prompt = build_review_prompt("draft", "performance", "critic", "audience", 8.5)
    assert "Sentence Connections" in prompt
    assert "Performance Demo" in prompt
    assert "Actionable Feedback" in prompt
    assert "Style" in prompt


def test_pedagogical_abstraction_prompt_includes_target_fields():
    prompt = build_pedagogical_abstraction_prompt(
        {
            "focal_span": "but not you as a person",
            "context_before": "Gen Z will respect your pronouns,",
            "context_after": "That is the tension.",
            "delivery_issue": "pause-too-short",
            "bit_function": "punch",
            "delivery_evidence_summary": "pace_wps=2.4; pause_before_sec=0.02",
        }
    )
    assert "Focal span" in prompt
    assert "pause-too-short" in prompt
    assert "pedagogical" in prompt.lower()


def test_screening_and_adjudication_prompts_reference_transferability():
    screening_prompt = build_functional_screening_prompt(
        {"semantic_seed_query": "pause before the reveal"},
        {
            "id": "cand-1",
            "transcript_excerpt": "and then I just stopped",
            "comedy_function": "punch",
            "focus_type": "release",
            "joke_role": "release",
            "pace_wps": 2.0,
            "pause_before_sec": 0.18,
            "pause_density": 0.1,
            "energy_rms": 0.2,
            "delivery_tags": [],
            "laughter_score": 0.8,
            "laugh_delay_sec": 0.1,
            "laugh_duration_sec": 0.6,
        },
    )
    adjudication_prompt = build_transferability_adjudication_prompt(
        {"semantic_seed_query": "pause before the reveal"},
        [
            {
                "id": "cand-1",
                "transcript_excerpt": "and then I just stopped",
                "comedy_function": "punch",
                "focus_type": "release",
                "joke_role": "release",
                "pace_wps": 2.0,
                "pause_before_sec": 0.18,
                "pause_density": 0.1,
                "energy_rms": 0.2,
                "delivery_tags": [],
                "laughter_score": 0.8,
                "laugh_delay_sec": 0.1,
                "laugh_duration_sec": 0.6,
            }
        ],
    )
    assert "pedagogical compatibility" in screening_prompt.lower()
    assert "transferability" in adjudication_prompt.lower()
