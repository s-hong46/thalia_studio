from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from app.config import Settings
from threading import Lock

Base = declarative_base()
_engine = None
_engine_url = ""
_session_factory = None
_session_url = ""
_schema_ready = False
_schema_url = ""
_schema_lock = Lock()
_factory_lock = Lock()


def get_engine():
    global _engine, _engine_url, _session_factory, _session_url
    settings = Settings()
    current_url = str(settings.mysql_url or "").strip()
    if not current_url:
        raise RuntimeError("MYSQL_URL is not set")
    if _engine is None or _engine_url != current_url:
        with _factory_lock:
            if _engine is not None and _engine_url != current_url:
                try:
                    _engine.dispose()
                except Exception:
                    pass
            _engine = create_engine(current_url, future=True)
            _engine_url = current_url
            _session_factory = None
            _session_url = ""
    return _engine


def get_session() -> Session:
    global _session_factory, _session_url
    engine = get_engine()
    current_url = str(Settings().mysql_url or "").strip()
    if _session_factory is None or _session_url != current_url:
        with _factory_lock:
            if _session_factory is None or _session_url != current_url:
                _session_factory = sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=engine,
                )
                _session_url = current_url
    return _session_factory()


def ensure_schema(database_url: str = ""):
    global _schema_ready, _schema_url
    if _schema_ready and _schema_url == (database_url or ""):
        return
    with _schema_lock:
        if _schema_ready and _schema_url == (database_url or ""):
            return
        Base.metadata.create_all(get_engine())
        _schema_ready = True
        _schema_url = database_url or ""
