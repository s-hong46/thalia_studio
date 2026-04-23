from app.models import Draft


def test_draft_has_created_at_and_status():
    assert hasattr(Draft, "created_at")
    assert hasattr(Draft, "status")
