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


def test_extension_assets_include_mobile_layout_and_safe_areas():
    static = Path(__file__).resolve().parents[1] / "static"
    css = (static / "ext-settings.css").read_text(encoding="utf-8")
    script = (static / "ext-settings.js").read_text(encoding="utf-8")

    for content in (css, script):
        assert "@media(max-width:720px)" in content
        assert "safe-area-inset-bottom" in content
        assert "100dvh" in content
        assert "grid-template-columns:repeat(2" in content
    assert ".min-w-\\[1120px\\]" in css
    assert ".min-w-\\\\[1120px\\\\]" in script
    assert "font-size:16px" in script  # Prevent iOS form zoom.
    assert "#fmx-player-source" in script
    assert "fmx-mobile-nav" in script
    assert "手机导航" in script
    assert "fmx-now-playing-open" in script
    assert "--music-player-now-playing-player-left-width:100%" in script
    assert "[data-lyric-index]" in script
    assert "aria-label','在线音源设置'" in script
    assert "lastObservedGuid" in script
    assert "本地音乐" in script
    assert "尚未识别当前歌曲" in script
    assert "z-index:100002" in script
