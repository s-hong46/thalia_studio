import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional


def _resolve_path(path_value: str) -> str:
    value = str(path_value or "").strip()
    if not value:
        value = "artifacts/logs"
    value = os.path.expanduser(value)
    if os.path.isabs(value):
        return os.path.normpath(value)
    project_root = Path(__file__).resolve().parents[1]
    return os.path.normpath(str(project_root / value))


def setup_app_logging() -> str:
    log_dir = _resolve_path(os.getenv("APP_LOG_DIR", "artifacts/logs"))
    os.makedirs(log_dir, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    log_file = os.path.join(log_dir, f"run-{run_id}.log")
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for handler in list(root.handlers):
        if getattr(handler, "_talkshow_file_handler", False):
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    if not any(getattr(handler, "_talkshow_console_handler", False) for handler in root.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler._talkshow_console_handler = True  # type: ignore[attr-defined]
        root.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler._talkshow_file_handler = True  # type: ignore[attr-defined]
    root.addHandler(file_handler)

    logging.getLogger(__name__).info("logging initialized: %s", log_file)
    return log_file
