from app.models import Performance, PerformanceEvent


def test_performance_models_exist():
    assert Performance.__tablename__ == "performances"
    assert PerformanceEvent.__tablename__ == "performance_events"
