"""Normalizers and helpers for optional QQ Music and LX-compatible sources."""
from __future__ import annotations

from typing import Any


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def extract_qq_tracks(payload: Any) -> list[dict]:
    """Find the song list across QQ API mobile/web response variants."""
    candidates: list[list] = []
    for obj in _walk_dicts(payload):
        for key in ("list", "items", "itemlist", "item_song", "songlist", "tracks"):
            value = obj.get(key)
            if not isinstance(value, list) or not value:
                continue
            if any(isinstance(item, dict) and (item.get("mid") or item.get("songmid")) for item in value):
                candidates.append(value)
    raw = max(candidates, key=len, default=[])
    return [track for item in raw if (track := normalize_qq_track(item)) is not None]


def normalize_qq_track(item: dict) -> dict | None:
    track = item.get("track") if isinstance(item.get("track"), dict) else item
    mid = str(track.get("mid") or track.get("songmid") or "").strip()
    if not mid:
        return None
    singers = track.get("singer") or track.get("singers") or []
    if isinstance(singers, list):
        artist = " / ".join(
            str(value.get("name") or "") if isinstance(value, dict) else str(value)
            for value in singers
        ).strip(" /")
    elif isinstance(singers, dict):
        artist = str(singers.get("name") or "")
    else:
        artist = str(singers)
    album_obj = track.get("album") if isinstance(track.get("album"), dict) else {}
    album_mid = str(album_obj.get("mid") or track.get("albummid") or "")
    album = str(album_obj.get("name") or track.get("albumname") or "")
    duration = track.get("interval") or track.get("duration") or 0
    try:
        duration_s = float(duration or 0)
    except (TypeError, ValueError):
        duration_s = 0.0
    file_info = track.get("file") if isinstance(track.get("file"), dict) else {}
    has_lossless = any(file_info.get(key, 0) for key in ("size_flac", "size_hires", "size_new"))
    cover = f"https://y.qq.com/music/photo_new/T002R500x500M000{album_mid}.jpg" if album_mid else ""
    return {
        "id": f"qq:{mid}",
        "source": "qq",
        "title": str(track.get("name") or track.get("songname") or track.get("title") or ""),
        "artist": artist,
        "album": album,
        "duration_s": duration_s,
        "ext": "flac" if has_lossless else "mp3",
        "cover_url": cover,
        "qq_mid": mid,
        "qq_id": track.get("id") or track.get("songid"),
        "raw": track,
    }


def qq_play_url(payload: Any, song_mid: str) -> tuple[str, int]:
    data = payload.get("data") if isinstance(payload, dict) else None
    mapping = data if isinstance(data, dict) else payload if isinstance(payload, dict) else {}
    item = mapping.get(song_mid)
    if not isinstance(item, dict) and isinstance(mapping.get("data"), dict):
        item = mapping["data"].get(song_mid)
    if not isinstance(item, dict):
        return "", 0
    try:
        size = int(item.get("size") or 0)
    except (TypeError, ValueError):
        size = 0
    return str(item.get("url") or ""), size


def lx_source_key(source: str) -> str:
    return {
        "kuwo": "kw",
        "kugou": "kg",
        "qq": "tx",
        "netease": "wy",
        "migu": "mg",
    }.get((source or "").lower(), "local")


def lx_music_info(item: dict, guid: str) -> dict:
    """Expose common LX field aliases because scripts vary by platform/source."""
    raw_id = str(
        item.get("lx_music_id")
        or item.get("qq_mid")
        or item.get("id")
        or guid.split(":")[-1]
    )
    if ":" in raw_id:
        raw_id = raw_id.rsplit(":", 1)[-1]
    title = str(item.get("title") or item.get("name") or "")
    artist = str(item.get("artist") or "")
    album = str(item.get("album") or "")
    return {
        "id": raw_id,
        "songmid": raw_id,
        "hash": raw_id,
        "copyrightId": raw_id,
        "name": title,
        "songName": title,
        "singer": artist,
        "singers": artist,
        "albumName": album,
        "album": album,
        "source": lx_source_key(str(item.get("source") or "")),
        "_fnmusicGuid": guid,
    }
