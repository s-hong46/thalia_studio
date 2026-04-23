from app import create_app


def test_process_logs_route():
    app = create_app()
    client = app.test_client()
    r = client.get("/api/process-logs?draft_id=1")
    assert r.status_code in (200, 404)
