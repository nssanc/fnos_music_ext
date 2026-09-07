#!/usr/bin/env python3
"""Install/remove the reversible fnmusic-ext settings script tag."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

START = "<!-- fnmusic-ext-ui:start -->"
END = "<!-- fnmusic-ext-ui:end -->"
TAG = f'{START}<link rel="stylesheet" href="/music/api/v1/_ext/assets/settings.css"><script src="/music/api/v1/_ext/assets/settings.js"></script>{END}'
DEFAULT_ROOTS = (
    Path("/usr/local/apps/trim.music"),
    Path("/usr/local/apps/@appcenter/trim.music"),
    Path("/usr/local/apps/@appstore/trim.music"),
    Path("/usr/local/apps/@appdata/trim.music"),
    Path("/usr/trim"),
)


def looks_like_music_index(path: Path, text: str) -> bool:
    lowered_path = str(path).lower()
    lowered = text.lower()
    return (
        path.name == "index.html"
        and "<html" in lowered
        and "<script" in lowered
        and ("trim.music" in lowered_path or "/usr/trim/" in lowered_path)
        and ("music" in lowered or "main.js" in lowered)
    )


def discover(explicit: str | None = None, roots: tuple[Path, ...] = DEFAULT_ROOTS) -> list[Path]:
    if explicit:
        path = Path(explicit)
        return [path.resolve()] if path.is_file() else []
    found = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("index.html"):
            try:
                if path.stat().st_size > 2 * 1024 * 1024:
                    continue
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                continue
            if looks_like_music_index(path, text):
                found.append(path.resolve())
    return sorted(set(found))


def replace_atomic(path: Path, text: str) -> None:
    stat = path.stat()
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.chmod(temp_name, stat.st_mode)
        try:
            os.chown(temp_name, stat.st_uid, stat.st_gid)
        except PermissionError:
            pass
        os.utime(temp_name, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def install(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    existing_start = text.find(START)
    existing_end = text.find(END, existing_start + len(START)) if existing_start >= 0 else -1
    if existing_start >= 0 and existing_end >= 0:
        text = text[:existing_start] + text[existing_end + len(END):]
    lower = text.lower()
    # Load before the Music module so new Audio() can be observed even when its
    # element is deliberately kept outside the document tree.
    position = lower.find('<script type="module"')
    if position < 0:
        position = lower.find("<script")
    if position < 0:
        position = lower.rfind("</head>")
    if position < 0:
        position = lower.rfind("</body>")
    updated = text[:position] + TAG + text[position:] if position >= 0 else TAG + text
    if updated == path.read_text(encoding="utf-8"):
        return False
    replace_atomic(path, updated)
    return True


def remove(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    start = text.find(START)
    end = text.find(END, start + len(START)) if start >= 0 else -1
    if start < 0 or end < 0:
        return False
    updated = text[:start] + text[end + len(END):]
    replace_atomic(path, updated)
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("install", "remove", "status"))
    parser.add_argument("--path")
    parser.add_argument("--state")
    args = parser.parse_args()
    paths = discover(args.path)
    if args.action == "remove" and args.state and Path(args.state).is_file():
        try:
            stored = json.loads(Path(args.state).read_text(encoding="utf-8"))
            paths = sorted(set(paths + [Path(value) for value in stored if Path(value).is_file()]))
        except (OSError, ValueError, TypeError):
            pass
    changed = []
    for path in paths:
        try:
            did_change = install(path) if args.action == "install" else remove(path) if args.action == "remove" else START in path.read_text(encoding="utf-8")
            if did_change:
                changed.append(str(path))
                print(path)
        except (OSError, UnicodeError) as exc:
            print(f"warning: {path}: {exc}", file=os.sys.stderr)
    if args.action == "install" and args.state:
        state = Path(args.state)
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(json.dumps([str(path) for path in paths], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0 if paths else 2


if __name__ == "__main__":
    raise SystemExit(main())
