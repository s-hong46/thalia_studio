def split_paragraphs(text: str) -> list:
    parts = [p.strip() for p in text.split("\n\n")]
    return [p for p in parts if len(p) >= 40]
