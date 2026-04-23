from typing import List, Tuple
import os
from app.services.openai_client import get_openai_client
import app.services.llm_service as llm_service


def embed_text(text: str) -> List[float]:
    client = get_openai_client()
    resp = client.embeddings.create(model="text-embedding-3-large", input=text)
    return resp.data[0].embedding


def classify_style_label(text: str) -> Tuple[str, float]:
    if os.getenv("OPENAI_API_KEY"):
        return llm_service.classify_style_label_llm(text)
    lowered = text.lower()
    if "absurd" in lowered or "surreal" in lowered:
        return "absurd", 0.7
    if "dark" in lowered or "mortality" in lowered:
        return "dark", 0.65
    if "self" in lowered or "i " in lowered:
        return "self-deprecating", 0.6
    if "about" in lowered or "observ" in lowered:
        return "observational", 0.6
    return "general", 0.55
