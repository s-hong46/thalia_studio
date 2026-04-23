import argparse
import importlib.util
from pathlib import Path


def _load_module():
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "api_inject_demo.py"
    spec = importlib.util.spec_from_file_location("api_inject_demo", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_cmd_asr_transcribe_posts_audio(monkeypatch):
    module = _load_module()
    tmp_dir = Path("artifacts/test_tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    audio = tmp_dir / "inject-demo-sample.mp3"
    audio.write_bytes(b"fake-audio")

    called = {}

    class FakeResponse:
        status_code = 200
        ok = True
        text = '{"text":"hello world"}'

        @staticmethod
        def json():
            return {"text": "hello world"}

    def fake_post(url, files=None, timeout=0, **kwargs):
        called["url"] = url
        called["files"] = files
        called["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(module.requests, "post", fake_post)

    args = argparse.Namespace(
        base_url="http://127.0.0.1:5000",
        audio=str(audio),
        draft_id=None,
        nickname="",
        append=True,
        overwrite=False,
    )
    rc = module.cmd_asr_transcribe(args)
    assert rc == 0
    assert called["url"].endswith("/api/asr/transcribe")
    assert "audio" in called["files"]
