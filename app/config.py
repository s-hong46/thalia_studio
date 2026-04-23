from dataclasses import dataclass, field
import os
from pathlib import Path
from dotenv import load_dotenv


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _normalize_path(value: str | os.PathLike[str]) -> str:
    return str(Path(value).expanduser().resolve())


def _candidate_env_paths() -> list[Path]:
    candidates: list[Path] = []
    explicit = os.getenv("COMEDYCOACH_ENV_FILE", "").strip()
    if explicit:
        candidates.append(Path(explicit).expanduser())

    cwd = Path.cwd()
    candidates.append(_PROJECT_ROOT / '.env')
    candidates.append(cwd / '.env')
    candidates.append(_PROJECT_ROOT.parent / '.env')

    for parent in cwd.parents:
        candidates.append(parent / '.env')
        if len(candidates) >= 10:
            break

    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_path(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(Path(normalized))
    return unique


def _load_env_files() -> tuple[list[str], list[str]]:
    loaded: list[str] = []
    checked: list[str] = []
    for candidate in _candidate_env_paths():
        candidate_str = _normalize_path(candidate)
        checked.append(candidate_str)
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            loaded.append(candidate_str)

    example_path = _PROJECT_ROOT / '.env.example'
    if example_path.is_file():
        load_dotenv(example_path, override=False)
        checked.append(_normalize_path(example_path))
    return loaded, checked


_LOADED_ENV_FILES, _CHECKED_ENV_PATHS = _load_env_files()


@dataclass
class Settings:
    openai_api_key: str = ""
    openai_timeout_sec: float = 45.0
    pinecone_api_key: str = ""
    mysql_url: str = ""
    tts_model: str = "gpt-4o-mini-tts"
    tts_voice: str = "marin"
    tts_format: str = "mp3"
    tts_output_dir: str = "app/static/tts"
    asr_model: str = "gpt-4o-mini-transcribe"
    asr_language: str = ""
    pinecone_cloud: str = "aws"
    pinecone_region: str = "us-east-1"
    pinecone_index_preferences: str = "talkshow-user-preferences"
    pinecone_index_anti: str = "talkshow-anti-examples"
    pinecone_index_video_clips: str = "talkshow-video-clips"
    video_dataset_root: str = "movies"
    video_dataset_cache_dir: str = "artifacts/video_dataset/cache"
    video_dataset_preview_dir: str = "app/static/rehearsal/video_preview"
    video_dataset_label_roots: str = ""
    video_dataset_chunk_len_sec: float = 30.0
    video_dataset_overlap_sec: float = 5.0
    video_dataset_top_k: int = 3
    video_dataset_initial_top_k: int = 20
    video_dataset_preview_clip_len_sec: float = 30.0
    video_dataset_partial_ready_min_files: int = 8
    video_dataset_partial_ready_ratio: float = 0.1
    video_dataset_max_files_for_test: int = 0
    loaded_env_files: list[str] = field(default_factory=list, init=False, repr=False)
    checked_env_paths: list[str] = field(default_factory=list, init=False, repr=False)

    @staticmethod
    def _resolve_project_path(path_value: str) -> str:
        value = str(path_value or "").strip()
        if not value:
            return value
        value = os.path.expanduser(value)
        if os.path.isabs(value):
            return os.path.normpath(value)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        return os.path.normpath(os.path.join(project_root, value))

    @property
    def project_root(self) -> str:
        return _normalize_path(_PROJECT_ROOT)

    @property
    def openai_key_configured(self) -> bool:
        return bool(str(self.openai_api_key or "").strip())

    def config_diagnostics(self) -> dict:
        return {
            "project_root": self.project_root,
            "loaded_env_files": list(self.loaded_env_files),
            "checked_env_paths": list(self.checked_env_paths),
            "openai_api_key_present": self.openai_key_configured,
        }

    def missing_openai_key_message(self) -> str:
        checked = ", ".join(self.checked_env_paths) if self.checked_env_paths else "(none)"
        loaded = ", ".join(self.loaded_env_files) if self.loaded_env_files else "(none)"
        expected = os.path.join(self.project_root, ".env")
        return (
            "OPENAI_API_KEY is not set. "
            f"Loaded env files: {loaded}. "
            f"Checked env paths: {checked}. "
            f"Put your real key in {expected} or set the OPENAI_API_KEY environment variable before starting the app."
        )

    def __post_init__(self):
        self.loaded_env_files = list(_LOADED_ENV_FILES)
        self.checked_env_paths = list(_CHECKED_ENV_PATHS)
        self.openai_api_key = os.getenv("OPENAI_API_KEY", self.openai_api_key)
        self.openai_timeout_sec = float(
            os.getenv("OPENAI_TIMEOUT_SEC", self.openai_timeout_sec)
        )
        self.pinecone_api_key = os.getenv("PINECONE_API_KEY", self.pinecone_api_key)
        self.mysql_url = (
            os.getenv("DATABASE_URL", "").strip()
            or os.getenv("MYSQL_URL", self.mysql_url)
        )
        self.tts_model = os.getenv("OPENAI_TTS_MODEL", self.tts_model)
        self.tts_voice = os.getenv("OPENAI_TTS_VOICE", self.tts_voice)
        self.tts_format = os.getenv("OPENAI_TTS_FORMAT", self.tts_format)
        self.tts_output_dir = os.getenv("OPENAI_TTS_OUTPUT_DIR", self.tts_output_dir)
        self.asr_model = os.getenv("OPENAI_ASR_MODEL", self.asr_model)
        self.asr_language = os.getenv("OPENAI_ASR_LANGUAGE", self.asr_language)
        self.pinecone_cloud = os.getenv("PINECONE_CLOUD", self.pinecone_cloud)
        self.pinecone_region = os.getenv("PINECONE_REGION", self.pinecone_region)
        self.pinecone_index_preferences = os.getenv(
            "PINECONE_INDEX_PREFERENCES", self.pinecone_index_preferences
        )
        self.pinecone_index_anti = os.getenv(
            "PINECONE_INDEX_ANTI", self.pinecone_index_anti
        )
        self.pinecone_index_video_clips = os.getenv(
            "PINECONE_INDEX_VIDEO_CLIPS", self.pinecone_index_video_clips
        )
        self.video_dataset_root = os.getenv("VIDEO_DATASET_ROOT", self.video_dataset_root)
        self.video_dataset_cache_dir = os.getenv(
            "VIDEO_DATASET_CACHE_DIR", self.video_dataset_cache_dir
        )
        self.video_dataset_preview_dir = os.getenv(
            "VIDEO_DATASET_PREVIEW_DIR", self.video_dataset_preview_dir
        )
        self.video_dataset_label_roots = os.getenv(
            "VIDEO_DATASET_LABEL_ROOTS", self.video_dataset_label_roots
        )
        self.video_dataset_chunk_len_sec = float(
            os.getenv("VIDEO_DATASET_CHUNK_LEN_SEC", self.video_dataset_chunk_len_sec)
        )
        self.video_dataset_overlap_sec = float(
            os.getenv("VIDEO_DATASET_OVERLAP_SEC", self.video_dataset_overlap_sec)
        )
        self.video_dataset_top_k = int(
            os.getenv("VIDEO_DATASET_TOP_K", self.video_dataset_top_k)
        )
        self.video_dataset_initial_top_k = int(
            os.getenv("VIDEO_DATASET_INITIAL_TOP_K", self.video_dataset_initial_top_k)
        )
        self.video_dataset_preview_clip_len_sec = float(
            os.getenv("VIDEO_DATASET_PREVIEW_CLIP_LEN_SEC", self.video_dataset_preview_clip_len_sec)
        )
        self.video_dataset_partial_ready_min_files = int(
            os.getenv("VIDEO_DATASET_PARTIAL_READY_MIN_FILES", self.video_dataset_partial_ready_min_files)
        )
        self.video_dataset_partial_ready_ratio = float(
            os.getenv("VIDEO_DATASET_PARTIAL_READY_RATIO", self.video_dataset_partial_ready_ratio)
        )
        self.video_dataset_max_files_for_test = int(
            os.getenv("VIDEO_DATASET_MAX_FILES_FOR_TEST", self.video_dataset_max_files_for_test)
        )
        self.tts_output_dir = self._resolve_project_path(self.tts_output_dir)
        self.video_dataset_root = self._resolve_project_path(self.video_dataset_root)
        self.video_dataset_cache_dir = self._resolve_project_path(self.video_dataset_cache_dir)
        self.video_dataset_preview_dir = self._resolve_project_path(
            self.video_dataset_preview_dir
        )
