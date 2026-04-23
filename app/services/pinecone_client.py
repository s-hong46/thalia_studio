from pinecone import Pinecone, ServerlessSpec
from app.config import Settings


def get_pinecone():
    settings = Settings()
    return Pinecone(api_key=settings.pinecone_api_key)


def ensure_indexes():
    settings = Settings()
    pc = get_pinecone()
    index_names = pc.list_indexes().names()
    if settings.pinecone_index_preferences not in index_names:
        pc.create_index(
            name=settings.pinecone_index_preferences,
            dimension=3072,
            metric="cosine",
            spec=ServerlessSpec(
                cloud=settings.pinecone_cloud, region=settings.pinecone_region
            ),
        )
    if settings.pinecone_index_anti not in index_names:
        pc.create_index(
            name=settings.pinecone_index_anti,
            dimension=3072,
            metric="cosine",
            spec=ServerlessSpec(
                cloud=settings.pinecone_cloud, region=settings.pinecone_region
            ),
        )
    if settings.pinecone_index_video_clips not in index_names:
        pc.create_index(
            name=settings.pinecone_index_video_clips,
            dimension=3072,
            metric="cosine",
            spec=ServerlessSpec(
                cloud=settings.pinecone_cloud, region=settings.pinecone_region
            ),
        )
    return pc
