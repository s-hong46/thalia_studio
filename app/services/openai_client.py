from openai import OpenAI
from app.config import Settings


def get_openai_client(timeout_sec=None):
    settings = Settings()
    timeout = settings.openai_timeout_sec if timeout_sec is None else timeout_sec
    return OpenAI(api_key=settings.openai_api_key, timeout=timeout)
