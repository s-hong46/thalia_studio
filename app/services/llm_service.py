from typing import Any, Dict, List, Optional
import os
import json
import re
from app.config import Settings
from app.services.openai_client import get_openai_client


def build_punchline_prompt(topic: str) -> str:
    return (
        "Generate 8-12 punchline ideas for a stand-up set about the topic below. "
        "Return plain text only, one idea per line. No markdown, no bullets, no bold.\n\n"
        f"Topic: {topic}"
    )


def build_suggestion_prompt(draft: str) -> str:
    return (
        "You are a stand-up writing coach. Continue the draft with one next sentence that improves "
        "stage effectiveness, not just grammar. Prioritize setup-to-payoff contrast, specificity, "
        "and comic intent. Return a single plain-text sentence only. No markdown, no bullets, no bold.\n\n"
        f"Draft:\n{draft}"
    )


def build_feedback_prompt(draft: str, anti_examples: List[str]) -> str:
    joined = "\n".join(f"- {x}" for x in anti_examples)
    return (
        "You are a stand-up writing and delivery coach. "
        "Given the draft and similar failed patterns, provide concrete fixes to improve live performance. "
        "Focus on joke framing, timing beats, escalation, and punchline clarity. "
        "Avoid generic pronunciation correction unless it blocks meaning. "
        "Return plain text only, no markdown, no bold markers, no asterisks.\n\n"
        f"Draft:\n{draft}\n\nSimilar failed patterns:\n{joined}\n"
    )


def build_performer_prompt(draft: str) -> str:
    return (
        "You are the Performer in a stand-up show. Based on the draft, perform a short set "
        "(2-4 sentences). Return plain text only, no markdown.\n\n"
        f"Draft:\n{draft}"
    )


def build_critic_prompt(draft: str, performer_text: str) -> str:
    return (
        "You are the Critic. Comment on the performance in 2-3 sentences "
        "with one concrete improvement. Return plain text only.\n\n"
        f"Draft:\n{draft}\n\nPerformance:\n{performer_text}"
    )


def build_audience_prompt(draft: str, performer_text: str) -> str:
    return (
        "You are the Audience. Give a short reaction (1-2 sentences) and "
        "a score from 0 to 10. Return JSON only with keys reaction and score. "
        "No extra words, no code fences.\n\n"
        f"Draft:\n{draft}\n\nPerformance:\n{performer_text}"
    )


def build_review_prompt(
    draft: str,
    performer_text: str,
    critic_text: str,
    audience_reaction: str,
    score: float,
) -> str:
    return (
        "You are a stand-up coach. After the performance, respond like a teacher guiding a "
        "student reading a text. Return plain text only, no markdown, no bold markers, "
        "no asterisks.\n\n"
        "Use this exact structure (in English):\n"
        "Sentence Connections: <Explain how the sentences connect, transitions, and logic>\n"
        "Performance Demo: <Perform the piece once, 2-4 sentences>\n"
        "Actionable Feedback:\n"
        "- Style: Language Arts Teacher | <feedback>\n"
        "- Style: Stage Director | <feedback>\n"
        "- Style: Comedy Coach | <feedback>\n\n"
        "Keep each feedback concrete and actionable.\n\n"
        f"Draft:\n{draft}\n\nPerformance:\n{performer_text}\n\n"
        f"Critic:\n{critic_text}\n\nAudience reaction:\n{audience_reaction}\n\n"
        f"Score: {score}/10"
    )


def generate_text(prompt: str, model: str = "gpt-4o") -> str:
    client = get_openai_client()
    use_responses = os.getenv("OPENAI_USE_RESPONSES", "").lower() in ("1", "true", "yes")
    if use_responses and hasattr(client, "responses"):
        try:
            response = client.responses.create(model=model, input=prompt)
            return response.output_text
        except Exception:
            pass
    response = client.chat.completions.create(
        model=model, messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


def classify_style_label_llm(text: str):
    prompt = (
        "Classify the stand-up style of the following text into a short label "
        "(e.g., observational, absurd, dark, self-deprecating, clean, edgy). "
        "Respond as JSON with keys label and confidence (0-1).\n\n"
        f"Text:\n{text}"
    )
    result = generate_text(prompt)
    try:
        import json

        data = json.loads(result)
        label = str(data.get("label", "general"))
        confidence = float(data.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))
        return label, confidence
    except Exception:
        return "general", 0.5


def build_rehearsal_marker_prompt(
    script: str,
    windows: List[Dict],
    style_preset: str = "",
    audio_profiles: Dict[int, Dict] = None,
) -> str:
    profile_map = audio_profiles or {}
    compact_windows = []
    for index, window in enumerate(windows):
        compact_windows.append(
            {
                "window_index": index,
                "time_range": window.get("time_range"),
                "window_source": window.get("window_source", "sentence-boundary"),
                "script_range": window.get("script_range"),
                "segment_text": window.get("segment_text", ""),
                "transcript_text": window.get("transcript_text", ""),
                "gap_before": window.get("gap_before"),
                "audio_profile": profile_map.get(index, {}),
            }
        )
    style_line = style_preset if style_preset else "natural stage delivery"
    windows_json = json.dumps(compact_windows, ensure_ascii=True)
    return (
        "You are a stand-up rehearsal coach focused on delivery quality from audio behavior. "
        "Infer emotion, tone, pacing, pauses, and emphasis issues from each aligned window. "
        "Prioritize coaching that improves audience impact rather than pure pronunciation correction. "
        "Return JSON only with shape {\"markers\": [...]}.\n\n"
        "Rules:\n"
        "- Output up to 8 markers.\n"
        "- Each marker must include: window_index, issue_type, severity, instruction, rationale, "
        "time_range, and optional demo_text.\n"
        "- issue_type must be one of: pause-too-short, speed-up, low-energy, falling-intonation, "
        "unclear-emphasis, tone-flat, rhythm-break.\n"
        "- severity is 0..1.\n"
        "- time_range must stay inside the selected window time_range.\n"
        "- instruction and rationale must be short, concrete, and in English only.\n"
        "- demo_text should be a minimal readable script fragment.\n"
        f"- Preferred style: {style_line}.\n\n"
        f"Script:\n{script}\n\n"
        f"Windows:\n{windows_json}"
    )


def _extract_json_object(raw_text: str) -> str:
    text = raw_text.strip()
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    if text.startswith("{") and text.endswith("}"):
        return text
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        return match.group(0)
    return "{}"


def generate_json(prompt: str, model: str = "gpt-4o", default: Optional[Any] = None) -> Any:
    fallback = {} if default is None else default
    raw = generate_text(prompt, model=model)
    if not raw:
        return fallback
    try:
        return json.loads(_extract_json_object(raw))
    except Exception:
        return fallback


def generate_rehearsal_markers(
    script: str,
    windows: List[Dict],
    style_preset: str = "",
    audio_profiles: Dict[int, Dict] = None,
) -> List[Dict]:
    if not windows:
        return []
    settings = Settings()
    if not settings.openai_api_key:
        return []
    prompt = build_rehearsal_marker_prompt(
        script=script,
        windows=windows,
        style_preset=style_preset,
        audio_profiles=audio_profiles or {},
    )
    raw = generate_text(prompt)
    if not raw:
        return []
    try:
        payload = json.loads(_extract_json_object(raw))
    except Exception:
        return []
    markers = payload.get("markers", []) if isinstance(payload, dict) else []
    if not isinstance(markers, list):
        return []
    normalized = []
    for item in markers:
        if not isinstance(item, dict):
            continue
        normalized.append(item)
    return normalized



def build_comedy_utterance_prompt(
    script: str,
    utterances: List[Dict],
    style_preset: str = "",
) -> str:
    compact = []
    for utt in utterances:
        compact.append(
            {
                "id": utt.get("id"),
                "text": utt.get("text", ""),
                "time_range": utt.get("time_range"),
                "audio_features": utt.get("audio_features", {}),
                "context_before": utt.get("context_before", ""),
                "context_after": utt.get("context_after", ""),
            }
        )
    style_line = style_preset or "general stand up delivery"
    return (
        "You are a stand up comedy coach analyzing a rehearsal at the utterance level. "
        "For each spoken utterance, assign one comedy function from: setup, misdirect, pivot, punch, tag, bridge, callback, other. "
        "Focus spans are lines where a coach should spend time on delivery because they shape or release the joke. "
        "Return JSON only with shape {\"utterances\": [...]} and no markdown.\n\n"
        "Each utterance item must contain: id, comedy_function, function_confidence, is_focus_span, and optional delivery_tags.\n"
        "Delivery tags may include: rushed_release, weak_build, weak_release, flat_shape, weak_emphasis, unstable_rhythm.\n"
        f"Preferred style: {style_line}.\n\n"
        f"Script:\n{script}\n\n"
        f"Utterances:\n{json.dumps(compact, ensure_ascii=True)}"
    )



def generate_comedy_utterance_annotations(
    script: str,
    utterances: List[Dict],
    style_preset: str = "",
) -> List[Dict]:
    if not utterances:
        return []
    settings = Settings()
    if not settings.openai_api_key:
        return []
    prompt = build_comedy_utterance_prompt(
        script=script,
        utterances=utterances,
        style_preset=style_preset,
    )
    raw = generate_text(prompt)
    if not raw:
        return []
    try:
        payload = json.loads(_extract_json_object(raw))
    except Exception:
        return []
    items = payload.get("utterances", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized.append(item)
    return normalized



def build_focus_note_prompt(
    script: str,
    utterances: List[Dict],
    joke_units: List[Dict],
    style_preset: str = "",
) -> str:
    style_line = style_preset or "general stand up delivery"
    payload = {
        "utterances": utterances,
        "joke_units": joke_units,
    }
    return (
        "You are a stand up comedy coach. Read the utterance level analysis and produce coaching notes about delivery. "
        "Do not rewrite the joke. Coach how the performer builds expectation, pivots, and releases the laugh. "
        "Each note must target one utterance and one user audio span. "
        "Return JSON only with shape {\"notes\": [...]} and no markdown.\n\n"
        "Each note must include: utterance_id, joke_unit_id, comedy_function, focus_type, title, advice, why, try_next, delivery_tags.\n"
        "focus_type should usually be one of: build, turn, release, tag, emphasis, shape.\n"
        f"Preferred style: {style_line}.\n\n"
        f"Script:\n{script}\n\n"
        f"Analysis payload:\n{json.dumps(payload, ensure_ascii=True)}"
    )



def generate_focus_notes(
    script: str,
    utterances: List[Dict],
    joke_units: List[Dict],
    style_preset: str = "",
) -> List[Dict]:
    if not utterances:
        return []
    settings = Settings()
    if not settings.openai_api_key:
        return []
    prompt = build_focus_note_prompt(
        script=script,
        utterances=utterances,
        joke_units=joke_units,
        style_preset=style_preset,
    )
    raw = generate_text(prompt)
    if not raw:
        return []
    try:
        payload = json.loads(_extract_json_object(raw))
    except Exception:
        return []
    items = payload.get("notes", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized.append(item)
    return normalized


def build_pedagogical_abstraction_prompt(target: Dict) -> str:
    focal_span = str(target.get("focal_span", "")).strip()
    context_before = str(target.get("context_before", "")).strip()
    context_after = str(target.get("context_after", "")).strip()
    delivery_issue = str(target.get("delivery_issue", "")).strip()
    bit_function = str(target.get("bit_function", "")).strip()
    delivery_evidence = str(target.get("delivery_evidence_summary", "")).strip()
    return (
        "You are not retrieving a clip yet. You are constructing a pedagogical retrieval "
        "specification for a stand up rehearsal moment.\n"
        "Your job is to abstract away from topical wording and identify the reusable "
        "performance logic that the performer needs help with.\n"
        "Treat the target as part of an unfolding bit. Do not optimize for semantic "
        "similarity. Do not restate the line in different words. Do not give generic advice.\n"
        "You must identify: (1) what local function this moment serves in the bit, "
        "(2) what delivery failure is occurring, (3) what demonstrable performance move "
        "would help, and (4) what kinds of examples would be misleading even if they are "
        "semantically similar.\n"
        "Return only valid JSON with keys: moment_function, delivery_failure, "
        "target_demonstration, positive_constraints, negative_constraints, "
        "semantic_seed_query, retrieval_rationale.\n\n"
        f"TARGET MOMENT\n"
        f"Focal span: {focal_span}\n"
        f"Bit context before: {context_before}\n"
        f"Bit context after: {context_after}\n"
        f"Detected issue: {delivery_issue}\n"
        f"Inferred local function: {bit_function}\n"
        f"Available delivery evidence: {delivery_evidence}\n"
        "Construct a pedagogical retrieval specification for this moment."
    )


def generate_pedagogical_retrieval_spec(
    target: Dict,
    model: str = "gpt-4o",
) -> Dict:
    settings = Settings()
    if not settings.openai_api_key:
        return {}
    payload = generate_json(
        build_pedagogical_abstraction_prompt(target),
        model=model,
        default={},
    )
    return payload if isinstance(payload, dict) else {}


def build_functional_screening_prompt(pedagogical_spec: Dict, candidate: Dict) -> str:
    candidate_function_role = {
        "comedy_function": str(candidate.get("comedy_function", "")).strip(),
        "focus_type": str(candidate.get("focus_type", "")).strip(),
        "joke_role": str(candidate.get("joke_role", "")).strip(),
        "function_confidence": candidate.get("function_confidence", 0.0),
    }
    candidate_delivery_profile = {
        "pace_wps": candidate.get("pace_wps", 0.0),
        "pause_before_sec": candidate.get("pause_before_sec", 0.0),
        "pause_density": candidate.get("pause_density", 0.0),
        "energy_rms": candidate.get("energy_rms", 0.0),
        "delivery_tags": candidate.get("delivery_tags", []),
    }
    candidate_laughter_profile = {
        "laughter_score": candidate.get("laughter_score", 0.0),
        "laugh_delay_sec": candidate.get("laugh_delay_sec", 0.0),
        "laugh_duration_sec": candidate.get("laugh_duration_sec", 0.0),
    }
    pedagogical_tags = (
        candidate.get("pedagogical_tags")
        or candidate.get("delivery_tags")
        or []
    )
    return (
        "You are screening a retrieved stand up clip for pedagogical compatibility.\n"
        "This is not a topical similarity task. A candidate should survive screening "
        "only if it can plausibly teach the target performance move.\n"
        "You must apply hard gates before giving an overall decision.\n"
        "Hard gates: (1) Functional alignment: Does the candidate moment serve a "
        "compatible local function in its bit? (2) Demonstration alignment: Does the "
        "candidate visibly realize the target performance move? (3) Pedagogical visibility: "
        "Could a performer actually study this clip and observe what to imitate or adapt? "
        "(4) Transfer risk: Is the candidate overly dependent on persona, topic specific "
        "knowledge, or room specific conditions in a way that would make it misleading?\n"
        "A candidate that fails any hard gate should be rejected.\n"
        "Return only valid JSON with keys: candidate_id, hard_gates, comparative_assessment, "
        "failure_mode_if_used, screening_decision, screening_rationale.\n\n"
        "PEDAGOGICAL RETRIEVAL SPEC\n"
        f"{json.dumps(pedagogical_spec, ensure_ascii=True)}\n\n"
        "CANDIDATE\n"
        f"Candidate id: {str(candidate.get('id', '')).strip()}\n"
        f"Transcript span: {str(candidate.get('transcript_excerpt', '')).strip()}\n"
        f"Context before: {str(candidate.get('context_before', '')).strip()}\n"
        f"Context after: {str(candidate.get('context_after', '')).strip()}\n"
        f"Inferred function and role: {json.dumps(candidate_function_role, ensure_ascii=True)}\n"
        f"Delivery profile: {json.dumps(candidate_delivery_profile, ensure_ascii=True)}\n"
        f"Laughter profile: {json.dumps(candidate_laughter_profile, ensure_ascii=True)}\n"
        f"Pedagogical tags: {json.dumps(pedagogical_tags, ensure_ascii=True)}\n"
        "Screen this candidate."
    )


def screen_pedagogical_candidate(
    pedagogical_spec: Dict,
    candidate: Dict,
    model: str = "gpt-4o",
) -> Dict:
    settings = Settings()
    if not settings.openai_api_key:
        return {}
    payload = generate_json(
        build_functional_screening_prompt(pedagogical_spec, candidate),
        model=model,
        default={},
    )
    return payload if isinstance(payload, dict) else {}


def build_transferability_adjudication_prompt(
    pedagogical_spec: Dict,
    candidates: List[Dict],
) -> str:
    compact_candidates = []
    for candidate in candidates:
        compact_candidates.append(
            {
                "candidate_id": str(candidate.get("id", "")).strip(),
                "transcript_span": str(candidate.get("transcript_excerpt", "")).strip(),
                "context_before": str(candidate.get("context_before", "")).strip(),
                "context_after": str(candidate.get("context_after", "")).strip(),
                "comedy_function": str(candidate.get("comedy_function", "")).strip(),
                "focus_type": str(candidate.get("focus_type", "")).strip(),
                "joke_role": str(candidate.get("joke_role", "")).strip(),
                "delivery_profile": {
                    "pace_wps": candidate.get("pace_wps", 0.0),
                    "pause_before_sec": candidate.get("pause_before_sec", 0.0),
                    "pause_density": candidate.get("pause_density", 0.0),
                    "energy_rms": candidate.get("energy_rms", 0.0),
                    "delivery_tags": candidate.get("delivery_tags", []),
                },
                "laughter_profile": {
                    "laughter_score": candidate.get("laughter_score", 0.0),
                    "laugh_delay_sec": candidate.get("laugh_delay_sec", 0.0),
                    "laugh_duration_sec": candidate.get("laugh_duration_sec", 0.0),
                },
                "screening_summary": candidate.get("screening_summary", {}),
            }
        )
    return (
        "You are making the final transferability decision for a stand up rehearsal "
        "reference example.\n"
        "All candidates you receive have already passed initial screening. Your task is "
        "to choose the candidate whose performance logic is most reusable for the target moment.\n"
        "Do not choose based on topic similarity, wording overlap, or overall funniness.\n"
        "Choose based on: (1) whether the same kind of performance problem is being solved, "
        "(2) whether the solution is visibly demonstrated, (3) whether the demonstration can be "
        "adapted by another performer, and (4) whether the clip would guide revision of this exact "
        "target moment.\n"
        "A good final choice should be studyable, portable, and instructionally useful without "
        "inviting literal imitation.\n"
        "Return only valid JSON with keys: selected_candidate_id, why_this_clip, what_to_watch, "
        "adaptation_guidance, transferability_rationale, portability_notes, ranked_candidate_ids.\n\n"
        "PEDAGOGICAL RETRIEVAL SPEC\n"
        f"{json.dumps(pedagogical_spec, ensure_ascii=True)}\n\n"
        "SCREENED CANDIDATES\n"
        f"{json.dumps(compact_candidates, ensure_ascii=True)}"
    )


def adjudicate_transferable_candidate(
    pedagogical_spec: Dict,
    candidates: List[Dict],
    model: str = "gpt-5.2",
) -> Dict:
    settings = Settings()
    if not settings.openai_api_key or not candidates:
        return {}
    try:
        payload = generate_json(
            build_transferability_adjudication_prompt(pedagogical_spec, candidates),
            model=model,
            default={},
        )
    except Exception:
        if model == "gpt-4o":
            return {}
        payload = generate_json(
            build_transferability_adjudication_prompt(pedagogical_spec, candidates),
            model="gpt-4o",
            default={},
        )
    return payload if isinstance(payload, dict) else {}
