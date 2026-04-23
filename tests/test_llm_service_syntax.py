import pathlib


def test_llm_service_compiles():
    root = pathlib.Path(__file__).resolve().parents[1]
    target = root / "app" / "services" / "llm_service.py"
    source = target.read_text(encoding="utf-8")
    compile(source, str(target), "exec")
