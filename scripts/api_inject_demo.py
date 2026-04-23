import argparse
import json
import pathlib
import sys
import time
from typing import Dict, Iterable, Optional, Tuple

import requests

DEFAULT_DEMO_DRAFT_TEXT = (
    "I hate alarm clocks. They are passive-aggressive roosters with a degree in torture. "
    "Every morning I negotiate with snooze like it is a labor contract."
)


def _json(resp: requests.Response) -> Dict:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text}


def _print_result(resp: requests.Response) -> Dict:
    payload = _json(resp)
    print(f"HTTP {resp.status_code}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def _iter_sse_lines(
    resp: requests.Response, deadline: float
) -> Iterable[Tuple[Optional[str], Optional[str]]]:
    event_name = None
    event_data = None
    for raw in resp.iter_lines(decode_unicode=True):
        if time.time() > deadline:
            return
        if raw is None:
            continue
        line = raw.strip()
        if not line:
            if event_name or event_data:
                yield event_name, event_data
            event_name = None
            event_data = None
            continue
        if line.startswith("event:"):
            event_name = line[len("event:") :].strip()
        elif line.startswith("data:"):
            event_data = line[len("data:") :].strip()


def cmd_list_drafts(args: argparse.Namespace) -> int:
    resp = requests.get(
        f"{args.base_url}/api/drafts", params={"nickname": args.nickname}, timeout=20
    )
    _print_result(resp)
    return 0 if resp.ok else 1


def cmd_create_draft(args: argparse.Namespace) -> int:
    resp = requests.post(
        f"{args.base_url}/api/drafts",
        json={"nickname": args.nickname, "title": args.title},
        timeout=20,
    )
    payload = _print_result(resp)
    if resp.ok:
        draft_id = payload.get("draft_id")
        if draft_id is not None:
            print(f"\nDraft created: {draft_id}")
    return 0 if resp.ok else 1


def cmd_save(args: argparse.Namespace) -> int:
    content = args.content
    if args.content_file:
        content = pathlib.Path(args.content_file).read_text(encoding="utf-8")
    resp = requests.post(
        f"{args.base_url}/api/save",
        json={"draft_id": args.draft_id, "content": content},
        timeout=20,
    )
    _print_result(resp)
    return 0 if resp.ok else 1


def cmd_analysis(args: argparse.Namespace) -> int:
    draft = args.draft
    if args.draft_file:
        draft = pathlib.Path(args.draft_file).read_text(encoding="utf-8")
    resp = requests.post(
        f"{args.base_url}/api/analysis",
        json={"draft_id": args.draft_id, "draft": draft},
        timeout=60,
    )
    _print_result(resp)
    return 0 if resp.ok else 1


def cmd_start_performance(args: argparse.Namespace) -> int:
    text = args.text
    if args.text_file:
        text = pathlib.Path(args.text_file).read_text(encoding="utf-8")
    resp = requests.post(
        f"{args.base_url}/api/performance/start",
        json={"draft_id": args.draft_id, "text": text},
        timeout=120,
    )
    _print_result(resp)
    return 0 if resp.ok else 1


def cmd_rehearsal(args: argparse.Namespace) -> int:
    audio_path = pathlib.Path(args.audio)
    if not audio_path.exists():
        print(f"Audio not found: {audio_path}")
        return 1
    form_data = {"script": args.script, "style_preset": args.style_preset}
    if args.draft_id is not None:
        form_data["draft_id"] = str(args.draft_id)
    with audio_path.open("rb") as fp:
        files = {"audio": (audio_path.name, fp)}
        resp = requests.post(
            f"{args.base_url}/api/rehearsal/analyze",
            data=form_data,
            files=files,
            timeout=180,
        )
    _print_result(resp)
    return 0 if resp.ok else 1


def cmd_stream(args: argparse.Namespace) -> int:
    url = f"{args.base_url}/api/stream"
    print(f"Connecting SSE: {url}?draft_id={args.draft_id}")
    deadline = time.time() + args.seconds
    try:
        with requests.get(
            url,
            params={"draft_id": args.draft_id},
            stream=True,
            timeout=(10, args.seconds + 5),
        ) as resp:
            if not resp.ok:
                _print_result(resp)
                return 1
            for event_name, event_data in _iter_sse_lines(resp, deadline):
                if event_name is None and event_data is None:
                    continue
                stamp = time.strftime("%H:%M:%S")
                print(f"[{stamp}] event={event_name} data={event_data}")
    except requests.exceptions.RequestException:
        if time.time() >= deadline:
            return 0
        raise
    return 0


def _normalize_append_flag(args: argparse.Namespace) -> bool:
    if hasattr(args, "append"):
        return bool(args.append)
    return not bool(getattr(args, "overwrite", False))


def cmd_asr_transcribe(args: argparse.Namespace) -> int:
    audio_path = pathlib.Path(args.audio)
    if not audio_path.exists():
        print(f"Audio not found: {audio_path}")
        return 1

    with audio_path.open("rb") as fp:
        files = {"audio": (audio_path.name, fp)}
        resp = requests.post(
            f"{args.base_url}/api/asr/transcribe",
            files=files,
            timeout=120,
        )
    payload = _print_result(resp)
    if not resp.ok:
        return 1

    transcript = str(payload.get("text", "")).strip()
    if not transcript:
        print("ASR returned empty text.")
        return 1

    if args.draft_id is None:
        return 0

    nickname = str(getattr(args, "nickname", "")).strip()
    if not nickname:
        print("传入 draft_id 时需要同时提供 nickname。")
        return 1

    draft_resp = requests.get(
        f"{args.base_url}/api/drafts/{args.draft_id}",
        params={"nickname": nickname},
        timeout=20,
    )
    draft_payload = _print_result(draft_resp)
    if not draft_resp.ok:
        return 1

    current_content = str(draft_payload.get("content", ""))
    append_mode = _normalize_append_flag(args)
    if append_mode and current_content.strip():
        separator = "" if current_content.endswith((" ", "\n")) else " "
        next_content = f"{current_content}{separator}{transcript}"
    else:
        next_content = transcript

    save_resp = requests.post(
        f"{args.base_url}/api/save",
        json={"draft_id": args.draft_id, "content": next_content},
        timeout=20,
    )
    _print_result(save_resp)
    if save_resp.ok:
        print("已将转写内容写回草稿。")
        print("注意：网页编辑器通常不会自动改写现有文本，需要刷新或重新载入草稿查看。")
    return 0 if save_resp.ok else 1


def _prompt(label: str, default: str = "") -> str:
    suffix = f"（默认: {default}）" if default else ""
    text = input(f"{label}{suffix}: ").strip()
    if not text:
        return default
    return text


def _prompt_int(label: str, default: Optional[int] = None) -> Optional[int]:
    default_str = "" if default is None else str(default)
    raw = _prompt(label, default_str)
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"输入无效，需要整数: {raw}")
        return None


def _prompt_bool(label: str, default: bool = True) -> bool:
    default_flag = "Y/n" if default else "y/N"
    raw = input(f"{label} [{default_flag}]: ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes", "1", "true")


def run_interactive(base_url: str) -> int:
    print("=== Talkshow API 交互测试菜单 ===")
    print(f"Base URL: {base_url}")
    while True:
        print("\n请选择要测试的功能：")
        print("1. 列出草稿（list-drafts）")
        print("2. 新建草稿（create-draft）")
        print("3. 保存草稿内容（save）")
        print("4. 触发分析反馈（analysis）")
        print("5. 触发表演流程（start-performance）")
        print("6. 注入排练音频（rehearsal）")
        print("7. 监听实时事件（stream）")
        print("8. ASR转写测试（asr-transcribe）")
        print("0. 退出")
        choice = input("请输入功能编号: ").strip()

        ns = argparse.Namespace(base_url=base_url)
        rc = 1

        if choice == "0":
            return 0
        if choice == "1":
            ns.nickname = _prompt("nickname", "demo")
            rc = cmd_list_drafts(ns)
        elif choice == "2":
            ns.nickname = _prompt("nickname", "demo")
            ns.title = _prompt("title", "Interactive Draft")
            rc = cmd_create_draft(ns)
        elif choice == "3":
            draft_id = _prompt_int("draft_id")
            if draft_id is None:
                continue
            ns.draft_id = draft_id
            ns.content_file = _prompt("content_file 路径(可选)", "")
            ns.content = _prompt("content 文本(可选)")
            rc = cmd_save(ns)
        elif choice == "4":
            draft_id = _prompt_int("draft_id")
            if draft_id is None:
                continue
            ns.draft_id = draft_id
            ns.draft_file = _prompt("draft_file 路径(可选)", "")
            ns.draft = _prompt("draft 文本(可选)", DEFAULT_DEMO_DRAFT_TEXT)
            rc = cmd_analysis(ns)
        elif choice == "5":
            draft_id = _prompt_int("draft_id")
            if draft_id is None:
                continue
            ns.draft_id = draft_id
            ns.text_file = _prompt("text_file 路径(可选)", "")
            ns.text = _prompt("text 文本(可选)", DEFAULT_DEMO_DRAFT_TEXT)
            rc = cmd_start_performance(ns)
        elif choice == "6":
            draft_id = _prompt_int("draft_id（可选，留空表示不推送SSE）", None)
            ns.draft_id = draft_id
            ns.script = _prompt("script", DEFAULT_DEMO_DRAFT_TEXT)
            ns.audio = _prompt("audio 文件路径", "out.mp3")
            ns.style_preset = _prompt("style_preset", "dry observational")
            rc = cmd_rehearsal(ns)
        elif choice == "7":
            draft_id = _prompt_int("draft_id")
            if draft_id is None:
                continue
            ns.draft_id = draft_id
            seconds = _prompt_int("监听秒数", 60)
            if seconds is None:
                continue
            ns.seconds = seconds
            rc = cmd_stream(ns)
        elif choice == "8":
            ns.audio = _prompt("audio 文件路径", "..\\out.mp3")
            ns.draft_id = _prompt_int("draft_id（可选，留空仅做ASR）", None)
            ns.nickname = ""
            ns.append = True
            ns.overwrite = False
            if ns.draft_id is not None:
                ns.nickname = _prompt("nickname", "demo")
                ns.append = _prompt_bool("写回草稿时是否追加到末尾", True)
                ns.overwrite = not ns.append
            rc = cmd_asr_transcribe(ns)
        else:
            print(f"未知选项: {choice}")
            continue

        print("\n命令执行完成，返回码:", rc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inject API requests into the running Talkshow web backend."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:5000",
        help="Backend base url (default: http://127.0.0.1:5000)",
    )
    sub = parser.add_subparsers(dest="command", required=False)

    p_list = sub.add_parser("list-drafts", help="List drafts by nickname")
    p_list.add_argument("--nickname", required=True)
    p_list.set_defaults(func=cmd_list_drafts)

    p_create = sub.add_parser("create-draft", help="Create a draft")
    p_create.add_argument("--nickname", required=True)
    p_create.add_argument("--title", required=True)
    p_create.set_defaults(func=cmd_create_draft)

    p_save = sub.add_parser("save", help="Save draft content")
    p_save.add_argument("--draft-id", required=True, type=int)
    p_save.add_argument("--content", default="")
    p_save.add_argument("--content-file")
    p_save.set_defaults(func=cmd_save)

    p_analysis = sub.add_parser(
        "analysis", help="Trigger /api/analysis (pushes SSE feedback to web)"
    )
    p_analysis.add_argument("--draft-id", required=True, type=int)
    p_analysis.add_argument("--draft", default="")
    p_analysis.add_argument("--draft-file")
    p_analysis.set_defaults(func=cmd_analysis)

    p_perf = sub.add_parser(
        "start-performance", help="Trigger performer/critic/audience flow"
    )
    p_perf.add_argument("--draft-id", required=True, type=int)
    p_perf.add_argument("--text", default="")
    p_perf.add_argument("--text-file")
    p_perf.set_defaults(func=cmd_start_performance)

    p_reh = sub.add_parser("rehearsal", help="Call /api/rehearsal/analyze with audio")
    p_reh.add_argument(
        "--draft-id",
        type=int,
        help="Optional draft id; when provided, backend will publish live SSE update.",
    )
    p_reh.add_argument("--script", required=True)
    p_reh.add_argument("--audio", required=True, help="Path to local audio file")
    p_reh.add_argument("--style-preset", default="")
    p_reh.set_defaults(func=cmd_rehearsal)

    p_stream = sub.add_parser("stream", help="Listen to SSE events for a draft")
    p_stream.add_argument("--draft-id", required=True, type=int)
    p_stream.add_argument(
        "--seconds", default=60, type=int, help="Listen duration (default: 60)"
    )
    p_stream.set_defaults(func=cmd_stream)

    p_asr = sub.add_parser(
        "asr-transcribe",
        help="Upload local audio to /api/asr/transcribe, optionally write transcript back to draft.",
    )
    p_asr.add_argument("--audio", required=True, help="Path to local audio file")
    p_asr.add_argument("--draft-id", type=int, help="Optional draft id to write transcript")
    p_asr.add_argument(
        "--nickname",
        default="",
        help="Nickname required when --draft-id is provided",
    )
    p_asr.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite draft content instead of appending",
    )
    p_asr.set_defaults(func=cmd_asr_transcribe)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        return run_interactive(args.base_url)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

