import json

import pytest

from proxy.source_registry import SourceRegistry


def test_source_registry_uses_defaults_and_persists_override(tmp_path):
    path = tmp_path / "sources.json"
    registry = SourceRegistry(str(path), {"musicdl": True, "qq": False})
    assert registry.snapshot() == {"musicdl": True, "qq": False}

    registry.set_enabled("qq", True)
    assert registry.enabled("qq") is True
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text(encoding="utf-8"))["enabled"]["qq"] is True


def test_source_registry_rejects_unknown_source(tmp_path):
    registry = SourceRegistry(str(tmp_path / "sources.json"), {"qq": False})
    with pytest.raises(KeyError):
        registry.set_enabled("unknown", True)


def test_source_registry_persists_preferences(tmp_path):
    registry = SourceRegistry(str(tmp_path / "sources.json"), {"qq": True})
    assert registry.preference("audioSource") == "auto"
    registry.set_preference("audioSource", "qqmusic")
    assert registry.preference("audioSource") == "qqmusic"
