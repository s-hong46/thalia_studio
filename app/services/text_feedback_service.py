import re
from collections import defaultdict
from typing import Dict, List, Tuple

RUBRIC_ORDER = [
    "Timing and Pacing",
    "Vocal Expressiveness",
    "Clarity and Articulation",
    "Confidence and Control",
    "Lasting Impression",
    "Uniqueness and Individual Style",
    "Personal Involvement and Authentic Presence",
]

ISSUE_TO_RUBRIC = {
    "pause-too-short": "Timing and Pacing",
    "speed-up": "Timing and Pacing",
    "rhythm-break": "Timing and Pacing",
    "low-energy": "Vocal Expressiveness",
    "tone-flat": "Vocal Expressiveness",
    "unclear-emphasis": "Clarity and Articulation",
    "falling-intonation": "Confidence and Control",
}


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[A-Za-z0-9']+", str(text or ""))


def _split_sentences(text: str) -> List[str]:
    cleaned = _normalize_space(text)
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?])\s+|\n+", cleaned)
    return [p.strip() for p in parts if p.strip()]


def _quote_span(text: str, max_words: int = 8) -> str:
    cleaned = _normalize_space(text)
    if not cleaned:
        return "this part of the bit"
    words = cleaned.split()
    if len(words) > max_words:
        cleaned = " ".join(words[:max_words])
    return f'"{cleaned}"'


def _score_band(value: float) -> str:
    val = max(0.0, min(1.0, float(value or 0.0)))
    if val >= 0.72:
        return "priority"
    if val >= 0.45:
        return "developing"
    return "promising"


def _count_pattern(words: List[str], *targets: str) -> int:
    lowered = [w.lower() for w in words]
    wanted = {t.lower() for t in targets}
    return sum(1 for w in lowered if w in wanted)


def _script_signals(script: str) -> Dict[str, float]:
    words = _tokenize(script)
    word_count = len(words)
    sentences = _split_sentences(script)
    last_sentence = sentences[-1] if sentences else _normalize_space(script)
    first_person = _count_pattern(words, "i", "i'm", "im", "me", "my", "mine", "myself")
    family = _count_pattern(words, "mom", "mother", "dad", "father", "boyfriend", "girlfriend", "wife", "husband", "roommate", "grandma", "grandpa")
    numbers = len(re.findall(r"\b\d+\b", script or ""))
    quotes = len(re.findall(r"['\"][^'\"]{2,30}['\"]", script or ""))
    named = len(re.findall(r"(?<!^)(?:\s)([A-Z][a-z]{2,})", script or ""))
    concrete = family + numbers + quotes + named
    emotional = _count_pattern(words, "love", "hate", "afraid", "embarrassed", "ashamed", "angry", "scared", "nervous", "obsessed")
    contractions = len(re.findall(r"\b\w+'\w+\b", script or ""))
    specificity = min(1.0, concrete / max(1.0, word_count / 18.0)) if word_count else 0.0
    personal = min(1.0, (first_person + family + emotional + contractions * 0.25) / max(1.0, word_count / 12.0)) if word_count else 0.0
    ending_words = _tokenize(last_sentence)
    ending_specific = min(
        1.0,
        (len(re.findall(r"\b\d+\b", last_sentence)) + len(re.findall(r"['\"][^'\"]{2,30}['\"]", last_sentence)) + len(re.findall(r"(?<!^)(?:\s)([A-Z][a-z]{2,})", last_sentence))) / max(1.0, len(ending_words) / 8.0),
    ) if ending_words else 0.0
    return {
        "word_count": word_count,
        "sentence_count": len(sentences),
        "specificity": specificity,
        "personal": personal,
        "ending_specific": ending_specific,
        "last_sentence": last_sentence,
    }


def _timing_section(markers: List[Dict]) -> Tuple[float, Dict]:
    if not markers:
        severity = 0.18
        return severity, {
            "rubric_dimension": "Timing and Pacing",
            "score_band": _score_band(severity),
            "current_read": "The timing is not collapsing, but the set still needs a cleaner sense of setup, turn, and landing.",
            "why_it_matters": "When the turn and the punch arrive too close together, the room hears the sentence but not the full-size joke.",
            "what_to_work_on_next": "Keep the setup moving, then give the turn a touch of space before the line lands.",
            "evidence_spans": [],
        }
    top = sorted(markers, key=lambda m: float(m.get("severity", 0.0) or 0.0), reverse=True)[:2]
    severity = max(float(top[0].get("severity", 0.0) or 0.0), sum(float(m.get("severity", 0.0) or 0.0) for m in top) / max(1, len(top)))
    span = _quote_span(top[0].get("demo_text", ""))
    issue = str(top[0].get("issue_type", "")).strip()
    if issue == "pause-too-short":
        current = f"The set gets to the point too fast around {span}. The turn and the punch are arriving almost as one thought."
        next_step = f"On the next pass, keep the wording the same and only give {span} a little more room before you release the point."
    elif issue == "speed-up":
        current = f"The line starts to rush around {span}. Once you get near the point, it sounds like you are trying to get to the end quickly."
        next_step = f"On the next pass, stop pushing through {span}. Let the last thought land instead of racing to the finish."
    else:
        current = f"The beat gets uneven around {span}. The audience can follow the sentence, but the rhythm under it is not fully settled yet."
        next_step = f"On the next pass, keep one clean beat through {span} so the joke feels deliberate all the way in."
    return min(1.0, max(0.25, severity)), {
        "rubric_dimension": "Timing and Pacing",
        "score_band": _score_band(severity),
        "current_read": current,
        "why_it_matters": "Timing is part of the joke. If the room is still catching up when the point arrives, the laugh will come in smaller than it should.",
        "what_to_work_on_next": next_step,
        "evidence_spans": [span],
    }


def _vocal_section(markers: List[Dict]) -> Tuple[float, Dict]:
    if not markers:
        severity = 0.2
        return severity, {
            "rubric_dimension": "Vocal Expressiveness",
            "score_band": _score_band(severity),
            "current_read": "The voice is serviceable, but it is not yet helping the joke enough. Too much of the set sits at one level.",
            "why_it_matters": "If every line carries the same weight, the audience gets the information but not always the comic emphasis.",
            "what_to_work_on_next": "Stay conversational, then choose one place in each joke where the voice needs to sharpen or brighten.",
            "evidence_spans": [],
        }
    top = sorted(markers, key=lambda m: float(m.get("severity", 0.0) or 0.0), reverse=True)[:2]
    severity = max(float(top[0].get("severity", 0.0) or 0.0), sum(float(m.get("severity", 0.0) or 0.0) for m in top) / max(1, len(top)))
    span = _quote_span(top[0].get("demo_text", ""))
    issue = str(top[0].get("issue_type", "")).strip()
    if issue == "low-energy":
        current = f"The voice stays too level through {span}. The line is readable, but the point does not really brighten where it should."
        next_step = f"On the next pass, do not raise the whole bit. Leave the sentence mostly alone and let {span} carry more intent."
    else:
        current = f"The line needs more shape around {span}. Right now the attitude and emphasis are too even to point the room toward the laugh."
        next_step = f"Next time, make {span} the place where the tone shifts instead of reading the whole sentence at one level."
    return min(1.0, max(0.25, severity)), {
        "rubric_dimension": "Vocal Expressiveness",
        "score_band": _score_band(severity),
        "current_read": current,
        "why_it_matters": "The voice should help the audience find the point. If it does not change shape, the joke feels flatter than the material really is.",
        "what_to_work_on_next": next_step,
        "evidence_spans": [span],
    }


def _clarity_section(markers: List[Dict]) -> Tuple[float, Dict]:
    if not markers:
        severity = 0.2
        return severity, {
            "rubric_dimension": "Clarity and Articulation",
            "score_band": _score_band(severity),
            "current_read": "The set is understandable, but the key words still need to pop more cleanly.",
            "why_it_matters": "The audience should not have to work to figure out which word carries the joke.",
            "what_to_work_on_next": "Pick the word that carries the point and make sure it arrives cleaner than the rest of the sentence.",
            "evidence_spans": [],
        }
    top = sorted(markers, key=lambda m: float(m.get("severity", 0.0) or 0.0), reverse=True)[0]
    severity = float(top.get("severity", 0.0) or 0.0)
    span = _quote_span(top.get("demo_text", ""))
    return min(1.0, max(0.25, severity)), {
        "rubric_dimension": "Clarity and Articulation",
        "score_band": _score_band(severity),
        "current_read": f"The important word is getting buried inside {span}. The sentence reads, but the punch word does not arrive cleanly enough yet.",
        "why_it_matters": "If the key word blurs together with the rest of the line, the audience hears the sentence but misses the exact place where the joke turns.",
        "what_to_work_on_next": f"Keep the sentence natural, but make the key word inside {span} the clearest thing in that thought.",
        "evidence_spans": [span],
    }


def _confidence_section(markers: List[Dict]) -> Tuple[float, Dict]:
    if not markers:
        severity = 0.22
        return severity, {
            "rubric_dimension": "Confidence and Control",
            "score_band": _score_band(severity),
            "current_read": "The set is readable, but it still sounds a little careful instead of fully owned.",
            "why_it_matters": "The room trusts a joke more when it sounds settled. If the end of the line wobbles, the laugh usually shrinks with it.",
            "what_to_work_on_next": "Stay with the thought all the way through the finish instead of backing out of the line near the end.",
            "evidence_spans": [],
        }
    top = sorted(markers, key=lambda m: float(m.get("severity", 0.0) or 0.0), reverse=True)[:2]
    severity = max(float(top[0].get("severity", 0.0) or 0.0), sum(float(m.get("severity", 0.0) or 0.0) for m in top) / max(1, len(top)))
    span = _quote_span(top[0].get("demo_text", ""))
    return min(1.0, max(0.25, severity)), {
        "rubric_dimension": "Confidence and Control",
        "score_band": _score_band(severity),
        "current_read": f"The delivery sounds slightly careful around {span}. It reads more like getting through the line than owning it in the room."
        ,
        "why_it_matters": "Confidence on stage is not about sounding louder. It is about sounding settled enough that the room trusts the joke.",
        "what_to_work_on_next": f"Next pass, stay with {span} right through the finish. Do not let the line tail off or hurry away from itself.",
        "evidence_spans": [span],
    }


def _lasting_impression_section(script: str, signals: Dict[str, float], timing_markers: List[Dict]) -> Tuple[float, Dict]:
    ending_specific = float(signals.get("ending_specific", 0.0) or 0.0)
    timing_pressure = max((float(m.get("severity", 0.0) or 0.0) for m in timing_markers or []), default=0.0)
    need = max(0.18, 0.7 - (ending_specific * 0.55) - (0.2 if timing_pressure < 0.45 else 0.0))
    last_sentence = _normalize_space(signals.get("last_sentence", ""))
    current = f"The ending thought, {_quote_span(last_sentence, max_words=10)}, does not yet feel like a true finish." if last_sentence else "The set does not yet leave a strong finishing impression."
    next_step = "For the next pass, make sure the last laugh idea feels like a deliberate finish, not simply the place where the script stops." if need > 0.45 else "The ending is serviceable. Now make the final thought cleaner and more memorable than the lines before it."
    return min(1.0, need), {
        "rubric_dimension": "Lasting Impression",
        "score_band": _score_band(need),
        "current_read": current,
        "why_it_matters": "A strong ending tells the audience where to keep their attention. If the close drifts out instead of landing, the whole set feels smaller in memory.",
        "what_to_work_on_next": next_step,
        "evidence_spans": [_quote_span(last_sentence, max_words=10)] if last_sentence else [],
    }


def _uniqueness_section(script: str, signals: Dict[str, float]) -> Tuple[float, Dict]:
    specificity = float(signals.get("specificity", 0.0) or 0.0)
    need = max(0.18, 0.68 - specificity)
    if specificity < 0.28:
        current = "The premise is there, but the point of view still feels more general than personal. Right now the bit could belong to a lot of comedians."
        next_step = "Keep the joke structure, but add one detail or one phrasing choice that feels unmistakably yours."
    else:
        current = "There is already a point of view here, but it can still get sharper and more identifiable faster."
        next_step = "Lean a little harder into the detail, phrasing, or attitude that makes the bit sound specifically like you."
    return min(1.0, need), {
        "rubric_dimension": "Uniqueness and Individual Style",
        "score_band": _score_band(need),
        "current_read": current,
        "why_it_matters": "Audiences remember performers, not just topics. The more specific the comic point of view feels, the less interchangeable the set sounds.",
        "what_to_work_on_next": next_step,
        "evidence_spans": [],
    }


def _authenticity_section(script: str, signals: Dict[str, float]) -> Tuple[float, Dict]:
    personal = float(signals.get("personal", 0.0) or 0.0)
    need = max(0.16, 0.7 - personal)
    if personal < 0.24:
        current = "The audience can hear the joke, but they do not yet hear enough of you inside it. Right now it plays more like material than lived point of view."
        next_step = "On the next pass, make the relationship between you and the joke easier to feel. Let the room hear why this is your story, not just a workable premise."
    else:
        current = "There is already some personal stake in the material, but the performance can still sound more inhabited and less recited."
        next_step = "Keep the wording mostly the same, but say it as if you are remembering it in front of people rather than reading from a fixed script."
    return min(1.0, need), {
        "rubric_dimension": "Personal Involvement and Authentic Presence",
        "score_band": _score_band(need),
        "current_read": current,
        "why_it_matters": "Authentic presence is what makes the room feel that you are actually there with them rather than reciting finished material from a safe distance.",
        "what_to_work_on_next": next_step,
        "evidence_spans": [],
    }


def _build_marker_items(markers: List[Dict]) -> List[Dict]:
    items = []
    for marker in markers or []:
        if not isinstance(marker, dict):
            continue
        marker_id = str(marker.get("id", "")).strip()
        issue = str(marker.get("issue_type", "")).strip()
        rubric = ISSUE_TO_RUBRIC.get(issue, "Confidence and Control")
        span = _quote_span(marker.get("demo_text", ""))
        if issue == "pause-too-short":
            paragraph = f"Around {span}, the turn comes too fast. Give the room a cleaner beat before the point lands."
        elif issue == "speed-up":
            paragraph = f"Around {span}, the line starts to rush. Keep the setup moving, then let the last thought land instead of pushing through it."
        elif issue == "low-energy":
            paragraph = f"Around {span}, the voice stays too even. Keep the line conversational, but let the point carry more intent."
        elif issue == "tone-flat":
            paragraph = f"Around {span}, the tone stays on one level. Give that stretch more shape so the room can hear the change in attitude."
        elif issue == "unclear-emphasis":
            paragraph = f"The important word inside {span} is not popping yet. Make the punch word the clearest thing in that thought."
        elif issue == "falling-intonation":
            paragraph = f"The end of the line around {span} drops away too early. Stay with the thought all the way through the finish."
        elif issue == "rhythm-break":
            paragraph = f"The beat shifts awkwardly around {span}. Keep one steady pulse through that section."
        else:
            paragraph = f"The section around {span} is the first place to clean up on the next rehearsal pass."
        items.append({
            "marker_id": marker_id,
            "marker_ids": [marker_id] if marker_id else [],
            "issue_type": issue,
            "rubric_dimension": rubric,
            "score_band": _score_band(float(marker.get("severity", 0.0) or 0.0)),
            "title": rubric,
            "paragraph": _normalize_space(paragraph),
        })
    return items


def _overall_headline(priorities: List[Dict]) -> str:
    if not priorities:
        return "This pass is readable, but it still needs another rehearsal before the strongest performance notes are clear."
    top = priorities[0]["rubric_dimension"]
    mapping = {
        "Timing and Pacing": "The writing is not the first problem here. The next lift will come from timing the turns and letting the points land.",
        "Vocal Expressiveness": "The jokes are on the page. The next lift will come from giving the voice more shape and clearer emphasis.",
        "Clarity and Articulation": "The audience can follow the material, but the key words are not popping cleanly enough yet.",
        "Confidence and Control": "The room can hear the bit. What is missing right now is the feeling that you fully own the line from start to finish.",
        "Lasting Impression": "The set needs a stronger finish so the audience leaves with a clearer final impression.",
        "Uniqueness and Individual Style": "The premise is workable. The next lift will come from sounding more specifically like you.",
        "Personal Involvement and Authentic Presence": "The joke reads, but it still needs more of you in it to feel fully alive on stage.",
    }
    return mapping.get(top, "The next lift will come from performance choices in the room, not from rewriting the premise.")


def _overall_summary(priorities: List[Dict]) -> str:
    if not priorities:
        return "Use the next rehearsal to keep the material fixed and listen for where the performance starts to wobble."
    names = [p["rubric_dimension"] for p in priorities[:2]]
    joined = names[0] if len(names) == 1 else f"{names[0]} and {names[1]}"
    return f"For the next pass, keep the material fixed and put your attention on {joined}. That is where the biggest gain is likely to come from right now."


def _next_rehearsal_plan(priorities: List[Dict]) -> List[str]:
    plans = []
    for section in priorities[:3]:
        action = _normalize_space(section.get("what_to_work_on_next", ""))
        if action and action not in plans:
            plans.append(action)
    if len(plans) < 3:
        plans.append("Run the set once without trying to improve everything at once. Pick one performance priority and leave the rest alone.")
    return plans[:3]


def build_text_only_feedback(*, style_label: str, script: str, transcript_text: str, markers: List[Dict]) -> Dict:
    script_text = _normalize_space(script or transcript_text)
    signals = _script_signals(script_text)

    rubric_groups: Dict[str, List[Dict]] = defaultdict(list)
    for marker in markers or []:
        if not isinstance(marker, dict):
            continue
        issue = str(marker.get("issue_type", "")).strip()
        rubric_groups[ISSUE_TO_RUBRIC.get(issue, "Confidence and Control")].append(marker)

    scored_sections: List[Tuple[float, Dict]] = [
        _timing_section(rubric_groups.get("Timing and Pacing", [])),
        _vocal_section(rubric_groups.get("Vocal Expressiveness", [])),
        _clarity_section(rubric_groups.get("Clarity and Articulation", [])),
        _confidence_section(rubric_groups.get("Confidence and Control", [])),
        _lasting_impression_section(script_text, signals, rubric_groups.get("Timing and Pacing", []) + rubric_groups.get("Confidence and Control", [])),
        _uniqueness_section(script_text, signals),
        _authenticity_section(script_text, signals),
    ]

    ordered = sorted(scored_sections, key=lambda item: item[0], reverse=True)
    priority_sections = [section for _, section in ordered[:3]]
    headline = _overall_headline(priority_sections)
    overall_summary = _overall_summary(priority_sections)
    next_plan = _next_rehearsal_plan(priority_sections)
    marker_items = _build_marker_items(markers)
    snapshot = [section for _, section in sorted(scored_sections, key=lambda item: RUBRIC_ORDER.index(item[1]["rubric_dimension"]))]

    section_paragraphs = [
        f"{section['rubric_dimension']}: {section['current_read']} {section['why_it_matters']} {section['what_to_work_on_next']}"
        for section in priority_sections
    ]
    full_text = "\n\n".join([headline, overall_summary] + section_paragraphs + ["Next rehearsal focus: " + " ".join(next_plan)]).strip()

    return {
        "mode": "text_only_baseline",
        "headline": headline,
        "summary": overall_summary,
        "overall_summary": overall_summary,
        "priority_dimensions": priority_sections,
        "rubric_snapshot": snapshot,
        "next_rehearsal_plan": next_plan,
        "full_text": full_text,
        "items": marker_items,
    }
