from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Float
from sqlalchemy.sql import func
from app.db import Base


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    nickname = Column(String(64), unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class Draft(Base):
    __tablename__ = "drafts"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    title = Column(String(200), nullable=False, default="Untitled Draft")
    content = Column(Text, nullable=False, default="")
    status = Column(String(20), nullable=False, default="active")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class DraftVersion(Base):
    __tablename__ = "draft_versions"
    id = Column(Integer, primary_key=True)
    draft_id = Column(Integer, ForeignKey("drafts.id"), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class Observation(Base):
    __tablename__ = "observations"
    id = Column(Integer, primary_key=True)
    draft_id = Column(Integer, ForeignKey("drafts.id"), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class Suggestion(Base):
    __tablename__ = "suggestions"
    id = Column(Integer, primary_key=True)
    draft_id = Column(Integer, ForeignKey("drafts.id"), nullable=False)
    text = Column(Text, nullable=False)
    source = Column(String(50), nullable=False, default="llm")
    created_at = Column(DateTime, server_default=func.now())


class StyleLabel(Base):
    __tablename__ = "style_labels"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    label = Column(String(80), nullable=False)
    confidence = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, server_default=func.now())


class StylePreset(Base):
    __tablename__ = "style_presets"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(120), nullable=False)
    description = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ProcessLog(Base):
    __tablename__ = "process_logs"
    id = Column(Integer, primary_key=True)
    draft_id = Column(Integer, ForeignKey("drafts.id"), nullable=False)
    agent = Column(String(30), nullable=False)
    text = Column(Text, nullable=False)
    score = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, server_default=func.now())


class Performance(Base):
    __tablename__ = "performances"
    id = Column(Integer, primary_key=True)
    draft_id = Column(Integer, ForeignKey("drafts.id"), nullable=False)
    status = Column(String(20), nullable=False, default="running")
    source_text = Column(Text, nullable=False, default="")
    score = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class PerformanceEvent(Base):
    __tablename__ = "performance_events"
    id = Column(Integer, primary_key=True)
    performance_id = Column(Integer, ForeignKey("performances.id"), nullable=False)
    role = Column(String(30), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class VideoAsset(Base):
    __tablename__ = "video_assets"
    id = Column(Integer, primary_key=True)
    file_path = Column(String(600), nullable=False, unique=True)
    file_name = Column(String(255), nullable=False)
    file_size = Column(Integer, nullable=False, default=0)
    mtime = Column(Float, nullable=False, default=0.0)
    duration_sec = Column(Float, nullable=False, default=0.0)
    ingest_status = Column(String(20), nullable=False, default="pending")
    last_error = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())


class VideoChunk(Base):
    __tablename__ = "video_chunks"
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("video_assets.id"), nullable=False)
    chunk_idx = Column(Integer, nullable=False)
    start_sec = Column(Float, nullable=False, default=0.0)
    end_sec = Column(Float, nullable=False, default=0.0)
    transcript = Column(Text, nullable=False, default="")
    style_label = Column(String(80), nullable=False, default="general")
    pace_wps = Column(Float, nullable=False, default=0.0)
    pause_density = Column(Float, nullable=False, default=0.0)
    energy_rms = Column(Float, nullable=False, default=0.0)
    embedding_ready = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())


class VideoSpan(Base):
    __tablename__ = "video_spans"
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("video_assets.id"), nullable=False)
    chunk_id = Column(Integer, ForeignKey("video_chunks.id"), nullable=False)
    span_idx = Column(Integer, nullable=False, default=0)
    start_sec = Column(Float, nullable=False, default=0.0)
    end_sec = Column(Float, nullable=False, default=0.0)
    transcript = Column(Text, nullable=False, default="")
    comedy_function = Column(String(40), nullable=False, default="other")
    focus_type = Column(String(40), nullable=False, default="shape")
    joke_role = Column(String(40), nullable=False, default="shape")
    function_confidence = Column(Float, nullable=False, default=0.0)
    delivery_tags_json = Column(Text, nullable=False, default="[]")
    quality_score = Column(Float, nullable=False, default=0.0)
    laughter_score = Column(Float, nullable=False, default=0.0)
    laugh_start_sec = Column(Float, nullable=False, default=0.0)
    laugh_end_sec = Column(Float, nullable=False, default=0.0)
    laugh_delay_sec = Column(Float, nullable=False, default=0.0)
    laugh_duration_sec = Column(Float, nullable=False, default=0.0)
    pace_wps = Column(Float, nullable=False, default=0.0)
    pause_before_sec = Column(Float, nullable=False, default=0.0)
    pause_density = Column(Float, nullable=False, default=0.0)
    energy_rms = Column(Float, nullable=False, default=0.0)
    style_label = Column(String(80), nullable=False, default="general")
    match_text = Column(Text, nullable=False, default="")
    payload_json = Column(Text, nullable=False, default="{}")
    source_kind = Column(String(40), nullable=False, default="heuristic")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())


class DatasetReferenceSpan(Base):
    __tablename__ = "dataset_reference_spans"
    id = Column(Integer, primary_key=True)
    video_id = Column(String(40), nullable=False, default="")
    span_idx = Column(Integer, nullable=False, default=0)
    label_file = Column(String(600), nullable=False, default="")
    label_mtime = Column(Float, nullable=False, default=0.0)
    source_url = Column(String(600), nullable=False, default="")
    title = Column(String(255), nullable=False, default="")
    channel = Column(String(255), nullable=False, default="")
    performer_name = Column(String(255), nullable=False, default="")
    language = Column(String(40), nullable=False, default="")
    start_sec = Column(Float, nullable=False, default=0.0)
    end_sec = Column(Float, nullable=False, default=0.0)
    transcript = Column(Text, nullable=False, default="")
    comedy_function = Column(String(40), nullable=False, default="other")
    focus_type = Column(String(40), nullable=False, default="shape")
    joke_role = Column(String(40), nullable=False, default="shape")
    function_confidence = Column(Float, nullable=False, default=0.0)
    delivery_tags_json = Column(Text, nullable=False, default="[]")
    quality_score = Column(Float, nullable=False, default=0.0)
    laughter_score = Column(Float, nullable=False, default=0.0)
    laugh_start_sec = Column(Float, nullable=False, default=0.0)
    laugh_end_sec = Column(Float, nullable=False, default=0.0)
    laugh_delay_sec = Column(Float, nullable=False, default=0.0)
    laugh_duration_sec = Column(Float, nullable=False, default=0.0)
    token_count = Column(Integer, nullable=False, default=0)
    laughter_token_count = Column(Integer, nullable=False, default=0)
    pace_wps = Column(Float, nullable=False, default=0.0)
    pause_before_sec = Column(Float, nullable=False, default=0.0)
    pause_density = Column(Float, nullable=False, default=0.0)
    energy_rms = Column(Float, nullable=False, default=0.0)
    style_label = Column(String(80), nullable=False, default="general")
    match_text = Column(Text, nullable=False, default="")
    payload_json = Column(Text, nullable=False, default="{}")
    source_kind = Column(String(40), nullable=False, default="dataset-label")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())
