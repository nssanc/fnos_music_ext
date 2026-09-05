from pathlib import Path

from scripts.ui_hook import END, START, discover, install, remove


def test_ui_hook_is_idempotent_and_reversible(tmp_path):
    root = tmp_path / "usr" / "local" / "apps" / "trim.music" / "www"
    root.mkdir(parents=True)
    index = root / "index.html"
    original = '<html><body><script src="main.js"></script><main>music</main></body></html>'
    index.write_text(original, encoding="utf-8")
    assert discover(roots=(tmp_path / "usr" / "local" / "apps" / "trim.music",)) == [index]
    assert install(index) is True
    assert install(index) is False
    assert index.read_text(encoding="utf-8").count(START) == 1
    assert END in index.read_text(encoding="utf-8")
    assert remove(index) is True
    assert remove(index) is False
    assert index.read_text(encoding="utf-8") == original
