from app import create_app


def test_create_app_sets_run_log_file():
    app = create_app()
    log_file = app.config.get("APP_RUN_LOG_FILE", "")
    assert isinstance(log_file, str)
    assert log_file
    assert log_file.endswith(".log")
