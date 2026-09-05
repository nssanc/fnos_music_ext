"""Persistent runtime switches for fnmusic-ext built-in sources."""
from __future__ import annotations

import json
import os
import threading
from typing import Any


class SourceRegistry:
    """Small atomic JSON store used by the settings panel.

    Environment variables remain the install-time defaults.  Values written here
    override them at runtime, so toggles survive a proxy restart without rewriting
    the user's .env file.
    """

    def __init__(self, path: str, defaults: dict[str, bool]):
        self.path = path
        self.defaults = dict(defaults)
        self._lock = threading.RLock()

    def _read(self) -> dict[str, Any]:
        try:
            with open(self.path, encoding="utf-8") as handle:
                value = json.load(handle)
            return value if isinstance(value, dict) else {}
        except (FileNotFoundError, OSError, ValueError):
            return {}

    def _write(self, value: dict[str, Any]) -> None:
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    def enabled(self, source_id: str) -> bool:
        with self._lock:
            values = self._read().get("enabled", {})
            if isinstance(values, dict) and isinstance(values.get(source_id), bool):
                return values[source_id]
            return bool(self.defaults.get(source_id, False))

    def override(self, source_id: str) -> bool | None:
        with self._lock:
            values = self._read().get("enabled", {})
            value = values.get(source_id) if isinstance(values, dict) else None
            return value if isinstance(value, bool) else None

    def set_enabled(self, source_id: str, enabled: bool) -> None:
        if source_id not in self.defaults:
            raise KeyError(source_id)
        with self._lock:
            data = self._read()
            values = data.get("enabled")
            if not isinstance(values, dict):
                values = {}
            values[source_id] = bool(enabled)
            data["version"] = 1
            data["enabled"] = values
            self._write(data)

    def snapshot(self) -> dict[str, bool]:
        return {source_id: self.enabled(source_id) for source_id in self.defaults}

    def preference(self, key: str, default: str = "auto") -> str:
        with self._lock:
            values = self._read().get("preferences", {})
            value = values.get(key) if isinstance(values, dict) else None
            return value if isinstance(value, str) and value else default

    def set_preference(self, key: str, value: str) -> None:
        with self._lock:
            data = self._read()
            values = data.get("preferences")
            if not isinstance(values, dict):
                values = {}
            values[key] = value
            data["version"] = 2
            data["preferences"] = values
            self._write(data)
