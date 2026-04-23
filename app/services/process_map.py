import random
import re
from collections import defaultdict
from typing import Dict, List, Optional

from app.services.video_catalog_service import looks_like_video_id, resolve_video_metadata

AGENTS = ["Performer", "Critic", "Audience"]

RUBRIC_FOR_ISSUE = {
    "pause-too-short": "Timing and Pacing",
    "speed-up": "Timing and Pacing",
    "rhythm-break": "Timing and Pacing",
    "low-energy": "Vocal Expressiveness",
    "tone-flat": "Vocal Expressiveness",
    "falling-intonation": "Confidence and Control",
    "unclear-emphasis": "Clarity and Articulation",
}


def fake_process_nodes():
    nodes = []
    for i in range(10):
        nodes.append(
            {
                "id": i,
                "agent": AGENTS[i % 3],
                "text": f"Step {i} insight",
                "x": random.random(),
                "y": random.random(),
            }
        )
    return nodes



def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())



def _marker_time_label(marker: Dict) -> str:
    time_range = marker.get("time_range")
    if not isinstance(time_range, list) or len(time_range) < 2:
        return "--"
    start = float(time_range[0] or 0.0)
    end = float(time_range[1] or 0.0)
    return f"{start:.1f}s - {end:.1f}s"



def _fallback_name_from_path(video_path: str) -> str:
    cleaned = str(video_path or "").strip().replace("\\", "/")
    if not cleaned:
        return ""
    parent = _normalize_space(cleaned.split("/")[-2] if "/" in cleaned else "")
    if not parent or looks_like_video_id(parent):
        return ""
    return parent



def _resolve_performer_profile(reference: Dict) -> Dict:
    profile = resolve_video_metadata(
        video_path=str(reference.get("video_path", "")),
        performer_id=str(reference.get("performer_id", "")),
        video_id=str(reference.get("video_id", "")),
        title=str(reference.get("title", "")),
        channel=str(reference.get("channel", "")),
        performer_name=str(reference.get("performer_name", "")),
    )
    performer_name = (
        str(profile.get("performer_name", "")).strip()
        or _fallback_name_from_path(str(reference.get("video_path", "")))
        or ""
    )
    video_id = str(profile.get("video_id", "")).strip() or str(reference.get("performer_id", "")).strip()
    if not video_id and performer_name:
        video_id = performer_name
    return {
        "video_id": video_id,
        "display_name": performer_name,
        "title": str(profile.get("title", "")).strip() or str(reference.get("title", "")).strip(),
        "channel": str(profile.get("channel", "")).strip() or str(reference.get("channel", "")).strip(),
    }



def _quote_span(text: str) -> str:
    cleaned = _normalize_space(text)
    if not cleaned:
        return "this part of the bit"
    words = cleaned.split()
    if len(words) > 8:
        cleaned = " ".join(words[-8:])
    return f'"{cleaned}"'



def _marker_focus_span(marker: Dict, ref: Dict) -> str:
    return _quote_span(
        ref.get("user_focus_span")
        or marker.get("demo_text")
        or marker.get("instruction")
        or marker.get("rationale")
    )



def _rubric_band(severity: float) -> str:
    value = max(0.0, min(1.0, float(severity or 0.0)))
    if value >= 0.8:
        return "needs work"
    if value >= 0.5:
        return "developing"
    return "promising"



def _reference_titles(refs: List[Dict]) -> List[str]:
    titles = []
    for ref in refs or []:
        title = _normalize_space(ref.get("title", ""))
        if title and title not in titles:
            titles.append(title)
    return titles



def _marker_feedback_payload(issue_type: str, rubric_dimension: str, focus_span: str, linked_refs: List[Dict]) -> Dict:
    primary_ref = linked_refs[0] if linked_refs else {}
    reference_titles = _reference_titles(linked_refs)
    watch_hint = _normalize_space(primary_ref.get("watch_hint", ""))
    copy_action = _normalize_space(primary_ref.get("copy_action", ""))
    rehearsal_drill = _normalize_space(primary_ref.get("rehearsal_drill", ""))

    if rubric_dimension == "Timing and Pacing":
        current_read = (
            f"This section is moving faster than the joke wants. Around {focus_span}, the turn and the point arrive too close together."
        )
        why_it_matters = (
            "In stand up, timing is part of the laugh. If the audience is still processing the setup when the point arrives, the joke feels smaller than it should."
        )
        coaching_priority = (
            f"On the next take, give {focus_span} a touch more room. Let the thought turn, then let the point land."
        )
        demo_learning_goal = watch_hint or "Use the demo to study where the comedian lets the line breathe before the point."
        next_goal = (
            rehearsal_drill
            or f"Do three runs of the bit and only work on the pace into {focus_span}. Leave the wording and volume alone."
        )
        steps = [
            "Listen once without copying anything. Just find the beat where the line settles before the point.",
            copy_action or f"Run your line again and make room before {focus_span}.",
            next_goal,
        ]
    elif rubric_dimension == "Vocal Expressiveness":
        current_read = (
            f"The voice is carrying the information, but not enough of the feeling. Around {focus_span}, the line stays too even."
        )
        why_it_matters = (
            "A joke lands harder when the voice tells us what matters. If everything has the same weight, the point of the line is harder to hear."
        )
        coaching_priority = (
            f"Keep the sentence relaxed, then make {focus_span} the place where the line wakes up."
        )
        demo_learning_goal = watch_hint or "Use the demo to notice which word suddenly gets brighter or heavier."
        next_goal = (
            rehearsal_drill
            or f"Do three runs and only change the energy on {focus_span}. Do not try to lift the whole sentence."
        )
        steps = [
            "Watch the demo once and find the exact word or beat where the voice becomes more alive.",
            copy_action or f"Run your own line and let {focus_span} carry the weight.",
            next_goal,
        ]
    elif rubric_dimension == "Clarity and Articulation":
        current_read = (
            f"The important words are not separating clearly enough around {focus_span}. The line is understandable, but the key point is not clean yet."
        )
        why_it_matters = (
            "The audience should not have to work to catch the joke. If the crucial word or turn blurs together with the rest of the sentence, the laugh loses force."
        )
        coaching_priority = (
            f"On the next take, make the key word inside {focus_span} easier to hear than the rest of the line."
        )
        demo_learning_goal = watch_hint or "Use the demo to hear how one word becomes unmistakably clear."
        next_goal = (
            rehearsal_drill
            or f"Do three runs and only clean up the key word inside {focus_span}. Keep the rest conversational."
        )
        steps = [
            "Listen for the word in the demo that the audience cannot miss.",
            copy_action or f"Say your line again and make the key word in {focus_span} cleaner and clearer.",
            next_goal,
        ]
    elif rubric_dimension == "Confidence and Control":
        current_read = (
            f"The delivery around {focus_span} sounds a little provisional, as if you are trying to get through the end of the line instead of owning it."
        )
        why_it_matters = (
            "Confidence on stage is not about sounding loud. It is about sounding settled. When the end of the line wobbles, the audience trusts it less."
        )
        coaching_priority = (
            f"Stay with {focus_span} all the way through the finish. Do not let the end rush or trail off."
        )
        demo_learning_goal = watch_hint or "Use the demo to study how the comedian sounds settled at the end of the thought."
        next_goal = (
            rehearsal_drill
            or f"Do three runs and only work on finishing {focus_span} with more control."
        )
        steps = [
            "Watch the end of the demo line and notice how little extra motion there is in the finish.",
            copy_action or f"Say your line again and carry {focus_span} cleanly through the end.",
            next_goal,
        ]
    else:
        current_read = f"The section around {focus_span} is where the current weakness is easiest to hear."
        why_it_matters = "This affects how clearly the audience receives the joke."
        coaching_priority = f"On the next take, simplify the line and make the point around {focus_span} easier to follow."
        demo_learning_goal = watch_hint or "Use the demo to see a cleaner version of the same job."
        next_goal = rehearsal_drill or f"Do three runs and change only what happens around {focus_span}."
        steps = [
            copy_action or f"Try the line again and adjust what happens around {focus_span}.",
            demo_learning_goal,
            next_goal,
        ]

    return {
        "current_read": current_read,
        "why_it_matters": why_it_matters,
        "coaching_priority": coaching_priority,
        "demo_learning_goal": demo_learning_goal,
        "next_rehearsal_goal": next_goal,
        "steps": steps[:3],
        "reference_titles": reference_titles,
        "watch": demo_learning_goal,
        "copy_action": copy_action or coaching_priority,
        "drill": next_goal,
    }



def build_marker_feedback(
    style_label: str,
    markers: List[Dict],
    video_references: List[Dict],
    script: str = "",
) -> Dict:
    valid_markers = [item for item in (markers or []) if isinstance(item, dict)]
    refs = [item for item in (video_references or []) if isinstance(item, dict)]
    marker_to_refs: Dict[str, List[Dict]] = defaultdict(list)
    for ref in refs:
        for marker_id in ref.get("marker_ids", []) or []:
            marker_id = str(marker_id).strip()
            if marker_id:
                marker_to_refs[marker_id].append(ref)

    items = []
    rubric_counts: Dict[str, int] = defaultdict(int)
    for marker in valid_markers:
        marker_id = str(marker.get("id", "")).strip()
        issue_type = str(marker.get("issue_type", "")).strip()
        rubric_dimension = RUBRIC_FOR_ISSUE.get(issue_type, "Confidence and Control")
        rubric_counts[rubric_dimension] += 1
        linked_refs = marker_to_refs.get(marker_id, [])
        focus_span = _marker_focus_span(marker, linked_refs[0] if linked_refs else {})
        payload = _marker_feedback_payload(issue_type, rubric_dimension, focus_span, linked_refs)
        band = _rubric_band(float(marker.get("severity", 0.0) or 0.0))
        title = f"{rubric_dimension} | {band}"
        paragraph = " ".join(
            part
            for part in [
                payload["current_read"],
                payload["why_it_matters"],
                payload["coaching_priority"],
                payload["demo_learning_goal"],
                payload["next_rehearsal_goal"],
            ]
            if part
        ).strip()
        items.append(
            {
                "marker_id": marker_id,
                "marker_ids": [marker_id] if marker_id else [],
                "time_range": marker.get("time_range", [0.0, 0.0]),
                "issue_type": issue_type,
                "rubric_dimension": rubric_dimension,
                "score_band": band,
                "title": title,
                "focus_span": focus_span,
                "current_read": payload["current_read"],
                "why_it_matters": payload["why_it_matters"],
                "coaching_priority": payload["coaching_priority"],
                "demo_learning_goal": payload["demo_learning_goal"],
                "next_rehearsal_goal": payload["next_rehearsal_goal"],
                "reference_titles": payload["reference_titles"],
                "steps": payload["steps"],
                "watch": payload["watch"],
                "copy_action": payload["copy_action"],
                "drill": payload["drill"],
                "diagnosis": payload["current_read"],
                "change": payload["coaching_priority"],
                "paragraph": paragraph,
            }
        )

    style_text = _normalize_space(style_label) or "general"
    if items:
        top_dims = sorted(rubric_counts.items(), key=lambda item: item[1], reverse=True)[:2]
        dim_text = " and ".join(name for name, _ in top_dims)
        summary = (
            f"Detected style: {style_text}. On this take, the clearest coaching priorities sit in {dim_text}."
        )
    else:
        summary = (
            f"Detected style: {style_text}. This take does not show strong delivery markers yet, so use another rehearsal pass to build a fuller read."
        )

    full_text_parts = [summary] + [item["paragraph"] for item in items if item.get("paragraph")]
    return {
        "summary": summary,
        "full_text": "\n\n".join(full_text_parts).strip(),
        "items": items,
    }



def _describe_user_style(style_label: str, markers: List[Dict]) -> str:
    style_text = _normalize_space(style_label) or "general"
    issue_counts = defaultdict(int)
    for marker in markers or []:
        issue = str(marker.get("issue_type", "")).strip()
        if issue:
            issue_counts[issue] += 1
    if not issue_counts:
        return f"Current read: {style_text}. This take needs another pass before the style profile becomes very distinct."
    dominant = sorted(issue_counts.items(), key=lambda item: item[1], reverse=True)[:2]
    dimensions = []
    for issue_name, _ in dominant:
        dim = RUBRIC_FOR_ISSUE.get(issue_name, "Confidence and Control")
        if dim not in dimensions:
            dimensions.append(dim)
    joined = " and ".join(dimensions)
    return f"Current read: {style_text}. Right now the strongest opportunities are in {joined}."



def link_references_to_markers(markers: List[Dict], video_references: List[Dict]) -> List[Dict]:
    valid_markers = [item for item in (markers or []) if isinstance(item, dict) and str(item.get("id", "")).strip()]
    refs = [item for item in (video_references or []) if isinstance(item, dict)]
    if not refs:
        return []
    if not valid_markers:
        linked = []
        for ref in refs:
            enriched = dict(ref)
            profile = _resolve_performer_profile(enriched)
            enriched["performer_profile"] = profile
            enriched["performer_id"] = profile.get("video_id", "")
            enriched["performer_name"] = profile.get("display_name", "")
            enriched.setdefault("marker_ids", [])
            enriched.setdefault("primary_marker_id", None)
            linked.append(enriched)
        return linked

    ordered_markers = sorted(
        valid_markers,
        key=lambda item: float(item.get("severity", 0.0) or 0.0),
        reverse=True,
    )
    linked = []
    for idx, ref in enumerate(refs):
        enriched = dict(ref)
        profile = _resolve_performer_profile(enriched)
        primary = ordered_markers[idx % len(ordered_markers)]
        primary_marker_id = str(primary.get("id", "")).strip()
        marker_ids = list(enriched.get("marker_ids", []) or [])
        if primary_marker_id and primary_marker_id not in marker_ids:
            marker_ids.insert(0, primary_marker_id)
        marker_ids = [str(item).strip() for item in marker_ids if str(item).strip()]
        enriched["marker_ids"] = marker_ids or [primary_marker_id]
        enriched["primary_marker_id"] = primary_marker_id
        enriched["performer_profile"] = profile
        enriched["performer_id"] = profile.get("video_id", "") or primary_marker_id
        enriched["performer_name"] = profile.get("display_name", "")
        linked.append(enriched)
    return linked



def build_similarity_process_map(
    style_label: str,
    markers: List[Dict],
    video_references: List[Dict],
    comedian_matches: Optional[List[Dict]] = None,
) -> Dict:
    style_text = _normalize_space(style_label) or "general"
    refs = [item for item in (video_references or []) if isinstance(item, dict)]
    valid_markers = [item for item in (markers or []) if isinstance(item, dict)]
    marker_by_id = {
        str(item.get("id", "")).strip(): item
        for item in valid_markers
        if str(item.get("id", "")).strip()
    }

    performers: List[Dict] = []
    if comedian_matches:
        for entry in comedian_matches:
            if not isinstance(entry, dict):
                continue
            performer = dict(entry)
            performer.setdefault("marker_ids", list(marker_by_id.keys())[:3])
            performers.append(performer)
    else:
        grouped = defaultdict(list)
        for ref in refs:
            profile = ref.get("performer_profile") if isinstance(ref.get("performer_profile"), dict) else _resolve_performer_profile(ref)
            key = str(profile.get("video_id", "")).strip() or str(profile.get("display_name", "")).strip()
            if key:
                grouped[key].append(dict(ref, performer_profile=profile))
        for _, performer_refs in grouped.items():
            profile = performer_refs[0]["performer_profile"]
            performer_name = str(profile.get("display_name", "")).strip()
            if not performer_name or performer_name.lower() == "unknown comedian":
                continue
            performer_id = str(profile.get("video_id", "")).strip() or performer_name
            match_scores = [float(ref.get("match_score", 0.0) or 0.0) for ref in performer_refs]
            style_scores = [float(ref.get("style_score", 0.0) or 0.0) for ref in performer_refs]
            rhythm_scores = [float(ref.get("rhythm_score", 0.0) or 0.0) for ref in performer_refs]
            marker_ids = sorted({str(mid).strip() for ref in performer_refs for mid in (ref.get("marker_ids", []) or []) if str(mid).strip() in marker_by_id})
            performers.append(
                {
                    "name": performer_name,
                    "performer_id": performer_id,
                    "title": str(profile.get("title", "")).strip(),
                    "channel": str(profile.get("channel", "")).strip(),
                    "similarity": round(sum(match_scores) / max(1, len(match_scores)), 4),
                    "style_score": round(sum(style_scores) / max(1, len(style_scores)), 4),
                    "rhythm_score": round(sum(rhythm_scores) / max(1, len(rhythm_scores)), 4),
                    "reference_count": len(performer_refs),
                    "marker_ids": marker_ids,
                    "style_summary": f"Closest overall delivery match in this library: {performer_name}.",
                    "ai_note": "This is based on pacing, pause pattern, vocal energy, and overall delivery shape rather than joke topic.",
                }
            )

    performers.sort(key=lambda item: item.get("similarity", 0.0), reverse=True)
    performers = performers[:6]
    top_performers = performers[:3]

    nodes = [{"id": "user-style", "type": "user", "label": f"user ({style_text})", "score": 1.0}]
    edges = []
    for performer in performers:
        performer_node_id = f"performer:{performer['name']}"
        nodes.append(
            {
                "id": performer_node_id,
                "type": "performer",
                "label": performer["name"],
                "score": performer.get("similarity", 0.0),
            }
        )
        edges.append({"source": "user-style", "target": performer_node_id, "weight": performer.get("similarity", 0.0)})
        for marker_id in performer.get("marker_ids", []):
            marker = marker_by_id.get(marker_id)
            if not marker:
                continue
            marker_node_id = f"marker:{marker_id}"
            if not any(node.get("id") == marker_node_id for node in nodes):
                nodes.append(
                    {
                        "id": marker_node_id,
                        "type": "marker",
                        "label": f"{_marker_time_label(marker)} | {marker.get('issue_type', '')}",
                        "score": float(marker.get("severity", 0.0) or 0.0),
                    }
                )
            edges.append({"source": marker_node_id, "target": performer_node_id, "weight": performer.get("similarity", 0.0)})

    style_description = _describe_user_style(style_text, valid_markers)
    ai_summary = ""
    if top_performers:
        names = [str(item.get("name", "")).strip() for item in top_performers if str(item.get("name", "")).strip()]
        if names:
            ai_summary = (
                f"Closest comedian matches right now: {' / '.join(names)}. Study their delivery choices, not their material or persona."
            )

    return {
        "status": "ready" if performers else "metadata_unavailable",
        "title": "Which Comedian Are You Most Like?",
        "style_label": style_text,
        "style_description": style_description,
        "top_performer": performers[0]["name"] if performers else "",
        "top_performers": top_performers,
        "ai_summary": ai_summary,
        "performers": performers,
        "nodes": nodes,
        "edges": edges,
    }
