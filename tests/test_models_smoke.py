from app.models import User, Draft


def test_models_exist():
    assert User.__tablename__ == "users"
    assert Draft.__tablename__ == "drafts"
