from app import create_app


def test_stream_endpoint():
    app = create_app()
    client = app.test_client()
    r = client.get("/api/stream?draft_id=1")
    assert r.status_code == 200
