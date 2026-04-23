import csv
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool


def _case_root():
    from pathlib import Path

    root = Path("artifacts/test_tmp/dataset_reference_service_case").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _reset_db(monkeypatch):
    import app.db as db_module
    import app.models  # noqa: F401

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    monkeypatch.setenv("MYSQL_URL", "sqlite://")
    db_module._engine = engine
    db_module._engine_url = ""
    db_module._session_factory = None
    db_module._session_url = ""
    db_module._schema_ready = False
    db_module._schema_url = ""
    monkeypatch.setattr(db_module, "get_engine", lambda: engine)
    db_module.ensure_schema("sqlite://")


def test_rebuild_dataset_reference_index_builds_searchable_spans(monkeypatch):
    _reset_db(monkeypatch)

    dataset_root = _case_root() / "dataset"
    label_dir = dataset_root / "Examples_label"
    catalog_dir = dataset_root / "CSV_clean"
    label_dir.mkdir(parents=True, exist_ok=True)
    catalog_dir.mkdir(parents=True, exist_ok=True)

    catalog_path = catalog_dir / "StandUp4AI_v1.csv"
    with catalog_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["url", "title", "description", "duration", "channel_id", "channel", "view_count", "lang", "file", "region"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "url": "https://www.youtube.com/watch?v=Yp9LaRbLyLo",
                "title": "Parking Spot - Test Comic - Stand-Up Featuring",
                "description": "test",
                "duration": "120.0",
                "channel_id": "standup",
                "channel": "Comedy Central Stand-Up",
                "view_count": "100",
                "lang": "en",
                "file": "./CSV_clean/StandUp4AI_v1.csv",
                "region": "en_us",
            }
        )

    label_path = label_dir / "Yp9LaRbLyLo.csv"
    with label_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["text", "timestamp", "label"])
        writer.writeheader()
        rows = [
            ("My", "[0.0, 0.15]", "O"),
            ("dad", "[0.15, 0.32]", "O"),
            ("said", "[0.32, 0.50]", "O"),
            ("he'd", "[0.50, 0.66]", "O"),
            ("die", "[0.66, 0.82]", "O"),
            ("for", "[0.82, 0.94]", "O"),
            ("me.", "[0.94, 1.18]", "O"),
            ("Turns", "[1.80, 2.02]", "O"),
            ("out", "[2.02, 2.22]", "O"),
            ("he", "[2.22, 2.32]", "O"),
            ("just", "[2.32, 2.50]", "O"),
            ("wanted", "[2.50, 2.82]", "O"),
            ("my", "[2.82, 2.94]", "O"),
            ("parking", "[2.94, 3.25]", "L"),
            ("spot.", "[3.25, 3.58]", "L"),
        ]
        for text, timestamp, label in rows:
            writer.writerow({"text": text, "timestamp": timestamp, "label": label})

    monkeypatch.setenv("VIDEO_DATASET_LABEL_ROOTS", str(dataset_root))

    import app.services.video_catalog_service as catalog_service
    from app.services.dataset_reference_service import load_dataset_reference_spans, rebuild_dataset_reference_index

    catalog_service.load_video_catalog.cache_clear()
    summary = rebuild_dataset_reference_index(force=True)

    assert summary["status"] == "ready"
    assert summary["processed_files"] == 1
    assert summary["reference_spans"] > 0

    items = load_dataset_reference_spans(limit=10)

    assert items
    assert any(item["video_id"] == "Yp9LaRbLyLo" for item in items)
    matched = next(item for item in items if item["video_id"] == "Yp9LaRbLyLo")
    assert matched["watch_url"] == "https://www.youtube.com/watch?v=Yp9LaRbLyLo"
    assert matched["title"] == "Parking Spot - Test Comic - Stand-Up Featuring"
    assert matched["performer_name"] == "Test Comic"
    assert matched["language"] == "en"
    assert "parking" in matched["match_text"].lower()
