from flask import Flask
from app.config import Settings
from app.db import ensure_schema
from app.logging_setup import setup_app_logging
from app.routes.api import api
from app.routes.pages import pages
from app.services.video_dataset_ingest_service import initialize_video_dataset_status, start_video_dataset_ingest


def create_app():
    log_file = setup_app_logging()
    app = Flask(__name__)
    settings = Settings()
    app.config["SETTINGS"] = settings
    app.config["APP_RUN_LOG_FILE"] = log_file
    if settings.mysql_url:
        ensure_schema(settings.mysql_url)
    initialize_video_dataset_status(settings)
    start_video_dataset_ingest(settings)
    app.register_blueprint(api)
    app.register_blueprint(pages)
    app.logger.info("app created with dataset_root=%s", settings.video_dataset_root)
    if settings.openai_key_configured:
        app.logger.info("OPENAI_API_KEY detected. loaded_env_files=%s", settings.loaded_env_files or ["(none)"])
    else:
        app.logger.warning(settings.missing_openai_key_message())
    return app
