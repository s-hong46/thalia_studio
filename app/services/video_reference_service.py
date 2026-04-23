from typing import Dict, List, Optional, Sequence

from app.services.video_match_service import match_video_references


def recommend_video_references(
    script: str,
    transcript_segments: List[Dict],
    style_label: str,
    markers: Optional[List[Dict]] = None,
    issue_types: Optional[Sequence[str]] = None,
    top_k: int = 3,
) -> List[Dict]:
    return match_video_references(
        script=script,
        transcript_segments=transcript_segments,
        markers=markers or [],
        style_label=style_label,
        issue_types=list(issue_types or []),
        top_k=top_k,
        initial_top_k=max(20, int(top_k) * 6),
    )
