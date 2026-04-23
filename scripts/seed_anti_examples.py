import uuid
from app.services.pinecone_client import ensure_indexes
from app.services.embedding_service import embed_text
from app.config import Settings

ANTI_EXAMPLES = [
    "Overlong setup with no punchline payoff.",
    "Punchline repeats the setup without a twist.",
    "Abrupt topic switch that breaks narrative momentum.",
    "Joke relies on a cliche without subversion.",
    "Excessive explanation of the joke intent.",
    "Punchline appears before premise is established.",
    "Too many unrelated tangents in a short span.",
    "Predictable ending with no surprise.",
    "Vague premise with no concrete imagery.",
    "Tone shift into meanness without clear target.",
]


def seed():
    settings = Settings()
    pc = ensure_indexes()
    index = pc.Index(settings.pinecone_index_anti)
    vectors = []
    for text in ANTI_EXAMPLES:
        vec = embed_text(text)
        vectors.append(
            {
                "id": str(uuid.uuid4()),
                "values": vec,
                "metadata": {"text": text},
            }
        )
    index.upsert(vectors=vectors)
    print("Seeded anti-examples.")


if __name__ == "__main__":
    seed()
