from app.config import Settings
from app.db import ensure_schema
from app.services.video_dataset_ingest_service import initialize_video_dataset_status, run_video_dataset_ingest_now


def main():
    settings = Settings()
    if not str(settings.mysql_url or "").strip():
        raise SystemExit("MYSQL_URL is not configured.")
    ensure_schema(settings.mysql_url)
    summary = run_video_dataset_ingest_now(settings=settings, force_reference_reindex=True)
    initialize_video_dataset_status(settings)
    print(summary)
    status = str(summary.get("status", "")).strip().lower()
    reference_status = str(summary.get("reference_status", "")).strip().lower()
    if status != "ready" or reference_status != "ready":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
