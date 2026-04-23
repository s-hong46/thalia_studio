import os

from app import create_app

app = create_app()

if __name__ == "__main__":
    settings = app.config.get("SETTINGS")
    if settings is not None and not settings.openai_key_configured:
        print(settings.missing_openai_key_message())
    debug = str(os.getenv("FLASK_DEBUG", "1")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    host = str(os.getenv("HOST", "127.0.0.1")).strip() or "127.0.0.1"
    port = int(str(os.getenv("PORT", "5000")).strip() or "5000")
    app.run(host=host, port=port, debug=debug, use_reloader=False)
