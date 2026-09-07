"""fnmusic-ext 拦截代理 (FastAPI + httpx).

功能：
1. 通用透传：所有非拦截路径原样转发到 trim-music unix socket
2. 搜索合并：GET /music/api/v1/search/track* （兼容 q/keyword，并行 musicdl）
3. 在线播放：stream + HLS 兜底 + transcode 空操作 + tee 缓存回放（音频与歌词 sidecar）
4. 在线元数据/歌词/封面
5. GET /_ext/healthz
"""
from __future__ import annotations

import asyncio
import base64
import glob
import html
import json
import logging
import os
import re
import shutil
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Coroutine
from urllib.parse import quote
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

try:
    from . import recommend as dailyrec
    from .online_sources import extract_qq_tracks, lx_music_info, lx_source_key, qq_play_url
    from .source_registry import SourceRegistry
    from .version import get_version
except ImportError:  # uvicorn --app-dir proxy
    import recommend as dailyrec  # type: ignore
    from online_sources import extract_qq_tracks, lx_music_info, lx_source_key, qq_play_url  # type: ignore
    from source_registry import SourceRegistry  # type: ignore
    from version import get_version  # type: ignore

logger = logging.getLogger("fnmusic_proxy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

_HOME = dailyrec.home_dir()
_STATIC_DIR = Path(__file__).resolve().parent / "static"

CONF = {
    "musicdl_url": os.environ.get("FNMUSIC_MUSICDL_URL", "http://127.0.0.1:8768"),
    "musicbox_url": os.environ.get("FNMUSIC_MUSICBOX_URL", "http://127.0.0.1:8770"),
    "qqmusic_url": os.environ.get("FNMUSIC_QQMUSIC_URL", "http://127.0.0.1:8771"),
    "lx_source_url": os.environ.get("FNMUSIC_LX_SOURCE_URL", "http://127.0.0.1:8772"),
    "lx_url": os.environ.get("FNMUSIC_LX_URL", "http://127.0.0.1:8773"),
    "musicdl_enabled": os.environ.get("FNMUSIC_MUSICDL_ENABLED", "true").lower() in ("true", "1", "yes"),
    "netease_enabled": os.environ.get("FNMUSIC_NETEASE_ENABLED", "true").lower() in ("true", "1", "yes"),
    "qqmusic_enabled": os.environ.get("FNMUSIC_QQMUSIC_ENABLED", "false").lower() in ("true", "1", "yes"),
    "lx_source_enabled": os.environ.get("FNMUSIC_LX_SOURCE_ENABLED", "false").lower() in ("true", "1", "yes"),
    "lx_enabled": os.environ.get("FNMUSIC_LX_ENABLED", "true").lower() in ("true", "1", "yes"),
    "lx_search_limit": int(os.environ.get("FNMUSIC_LX_SEARCH_LIMIT", "50")),
    "lx_quality": os.environ.get("FNMUSIC_LX_QUALITY", "lossless"),
    "netease_wait_s": float(os.environ.get("FNMUSIC_NETEASE_WAIT_S", "3.0")),
    "netease_quality": os.environ.get("FNMUSIC_NETEASE_QUALITY", "lossless"),
    "netease_search_limit": int(os.environ.get("FNMUSIC_NETEASE_SEARCH_LIMIT", "100")),
    "musicdl_search_limit": int(os.environ.get("FNMUSIC_MUSICDL_SEARCH_LIMIT", "100")),
    "qqmusic_search_limit": int(os.environ.get("FNMUSIC_QQMUSIC_SEARCH_LIMIT", "50")),
    "qqmusic_quality": os.environ.get("FNMUSIC_QQMUSIC_QUALITY", "F000"),
    "upstream_sock": os.environ.get("FNMUSIC_UPSTREAM_SOCK", "/var/run/trim_music_upstream.socket"),
    "online_limit": int(os.environ.get("FNMUSIC_ONLINE_LIMIT", "100")),
    "search_list_path": os.environ.get("FNMUSIC_SEARCH_LIST_PATH", "data.list"),
    "cache_dir": os.environ.get("FNMUSIC_CACHE_DIR", os.path.join(_HOME, "cache")),
    # 空=从飞牛 shared_library.path 自动探测；测试可覆盖到临时目录
    "library_dir": os.environ.get("FNMUSIC_LIBRARY_DIR", ""),
    "music_db": os.environ.get(
        "FNMUSIC_MUSIC_DB", "/usr/local/apps/@appdata/trim.music/db/music.db"
    ),
    "merge_suggest": os.environ.get("FNMUSIC_MERGE_SUGGEST", "true").lower() in ("true", "1", "yes"),
    "online_sources": os.environ.get("FNMUSIC_ONLINE_SOURCES", "KuwoMusicClient,MiguMusicClient"),
    "lyric_field": os.environ.get("FNMUSIC_LYRIC_FIELD", "data.lyric"),
    "search_timeout": float(os.environ.get("FNMUSIC_SEARCH_TIMEOUT", "15")),
    "search_cache_ttl": float(os.environ.get("FNMUSIC_SEARCH_CACHE_TTL", "604800")),
    "late_page_wait_s": float(os.environ.get("FNMUSIC_LATE_PAGE_WAIT_S", "5.0")),
    "fav_dir": os.environ.get(
        "FNMUSIC_FAV_DIR", os.path.join(_HOME, "online_favorites")
    ),
    "llm_base_url": (os.environ.get("FNMUSIC_LLM_BASE_URL") or "").strip().rstrip("/"),
    "llm_model": (os.environ.get("FNMUSIC_LLM_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini",
}

SOURCE_REGISTRY = SourceRegistry(
    os.environ.get("FNMUSIC_SOURCE_CONFIG", os.path.join(_HOME, "source-config.json")),
    {
        "musicdl": CONF["musicdl_enabled"],
        "netease": CONF["netease_enabled"],
        "qqmusic": CONF["qqmusic_enabled"],
        "lx": CONF["lx_source_enabled"],
        "lxmusic": CONF["lx_enabled"],
    },
)


def source_enabled(source_id: str) -> bool:
    override = SOURCE_REGISTRY.override(source_id)
    if override is not None:
        return override
    conf_key = {
        "musicdl": "musicdl_enabled",
        "netease": "netease_enabled",
        "qqmusic": "qqmusic_enabled",
        "lx": "lx_source_enabled",
        "lxmusic": "lx_enabled",
    }.get(source_id)
    return bool(CONF.get(conf_key, False)) if conf_key else False


SOURCE_LABELS = {
    "qq": "QQ音乐",
    "qqmusic": "QQ音乐",
    "netease": "网易云",
    "kuwo": "酷我",
    "migu": "咪咕",
    "kugou": "酷狗",
    "lx": "洛雪聚合",
    "lxsource": "洛雪自定义源",
    "musicdl": "聚合源",
}


def source_family(source_id: str) -> str:
    source_id = (source_id or "").lower()
    if source_id == "qq":
        return "qqmusic"
    if source_id == "lx":
        return "lxmusic"
    if source_id in ("kuwo", "migu", "kugou", "musicdl"):
        return "musicdl"
    return source_id


def source_label(source_id: str) -> str:
    return SOURCE_LABELS.get((source_id or "").lower(), source_id or "在线")


def entity_cover_url(item: dict) -> str:
    """Accept the cover field variants returned by musicbox/NetEase entities."""
    for key in (
        "cover_url", "coverUrl", "pic_url", "picUrl", "img1v1_url", "img1v1Url",
        "avatar_url", "avatarUrl", "album_pic_url", "blur_pic_url", "blurPicUrl",
    ):
        value = str(item.get(key) or "").strip()
        if value.startswith(("http://", "https://")):
            return value
    return ""


def order_online_items(items: list[dict]) -> list[dict]:
    preferred = SOURCE_REGISTRY.preference("audioSource")
    if preferred == "auto":
        return items
    return sorted(items, key=lambda item: source_family(str(item.get("source") or "")) != preferred)

_REDACT_KEY_PARTS = ("api_key", "apikey", "token", "secret", "password")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

CACHE_EXTS = ("mp3", "flac", "wav", "ogg", "opus", "m4a", "aac", "ape", "wv", "dsf", "dff", "tta")

# 飞牛 Kl() 归一化：mpeg/mp3→mp3，wav/pcm→wav，m4a/aac/mp4→m4a，其余小写原样（flac/ogg/ape/wv…）
_FORMAT_ALIASES = {
    "mp3": "mp3",
    "mpeg": "mp3",
    "mpga": "mp3",
    "flac": "flac",
    "wav": "wav",
    "wave": "wav",
    "pcm": "wav",
    "lpcm": "wav",
    "ogg": "ogg",
    "vorbis": "ogg",
    "opus": "opus",
    "m4a": "m4a",
    "mp4": "m4a",
    "mp4a": "m4a",
    "aac": "m4a",
    "alac": "m4a",
    "ape": "ape",
    "wv": "wv",
    "wavpack": "wv",
    "dsf": "dsf",
    "dff": "dff",
    "dsd": "dsd",
    "tta": "tta",
    "tak": "tak",
    "wma": "wma",
    "aiff": "aiff",
    "aif": "aiff",
}


# 模块级搜索缓存
_SEARCH_CACHE: dict[str, dict] = {}
_ENTITY_SEARCH_CACHE: dict[tuple[str, str], dict] = {}
_ONLINE_ENTITY_CACHE: dict[str, dict] = {}
_COVER_CACHE: dict[str, dict] = {}


def _clean_search_cache() -> None:
    """清理过期缓存，若仍超过容量上限（2000条），按 ts 升序淘汰最旧的一半。"""
    now = time.time()
    ttl = CONF.get("search_cache_ttl", 604800.0)
    expired_keys = [k for k, v in _SEARCH_CACHE.items() if now - v.get("ts", 0) >= ttl]
    for k in expired_keys:
        _SEARCH_CACHE.pop(k, None)
    if len(_SEARCH_CACHE) > 2000:
        sorted_keys = sorted(_SEARCH_CACHE.keys(), key=lambda k: _SEARCH_CACHE[k].get("ts", 0))
        to_remove = sorted_keys[: len(sorted_keys) // 2]
        for k in to_remove:
            _SEARCH_CACHE.pop(k, None)
    if len(_ENTITY_SEARCH_CACHE) > 200:
        sorted_keys = sorted(
            _ENTITY_SEARCH_CACHE.keys(), key=lambda k: _ENTITY_SEARCH_CACHE[k].get("ts", 0)
        )
        for key in sorted_keys[: len(sorted_keys) // 2]:
            _ENTITY_SEARCH_CACHE.pop(key, None)


def _set_search_cache(keyword: str, entry: dict) -> None:
    _clean_search_cache()
    _SEARCH_CACHE[keyword] = entry


def _set_online_entity_cache(guid: str, entry: dict) -> None:
    if len(_ONLINE_ENTITY_CACHE) >= 1000 and guid not in _ONLINE_ENTITY_CACHE:
        oldest = sorted(
            _ONLINE_ENTITY_CACHE,
            key=lambda key: _ONLINE_ENTITY_CACHE[key].get("detail_ts")
            or _ONLINE_ENTITY_CACHE[key].get("ts")
            or 0,
        )
        for key in oldest[:500]:
            _ONLINE_ENTITY_CACHE.pop(key, None)
    _ONLINE_ENTITY_CACHE[guid] = entry


ONLINE_TRIAL_MARKERS = (
    "(试听)", "（试听）", "试听片段", "片段试听", "试听版",
    "[试听]", "【试听】", "- 试听", " - 试听",
)


def is_playable_online_track(item: dict, require_id: bool = False) -> bool:
    """Reject explicit trials, paid-only records and invalid direct URLs."""
    if not isinstance(item, dict):
        return False
    title = str(item.get("title") or item.get("name") or item.get("song_name") or "").strip()
    if not title:
        return False
    if require_id and not str(
        item.get("id") or item.get("song_id") or item.get("guid") or ""
    ).strip():
        return False
    if any(marker in title for marker in ONLINE_TRIAL_MARKERS):
        return False
    if item.get("is_trial") is True or item.get("freeTrialInfo") or item.get("freeTrialPrivilege"):
        return False

    def _number(key: str) -> int:
        try:
            return int(item.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    if _number("is_free_part") != 0 or _number("fail_process") == 4:
        return False
    if _number("pay_type") != 0 or _number("pkg_price") != 0 or _number("price") != 0:
        return False
    if item.get("fee") is not None and _number("fee") not in (0, 8):
        return False
    if item.get("unplayable") is True or item.get("playable") is False:
        return False
    if item.get("has_stream") is False:
        return False
    if "download_url" in item:
        url = str(item.get("download_url") or "").strip()
        if not url.startswith(("http://", "https://")) or "error.html" in url:
            return False
    if "url" in item:
        url = str(item.get("url") or "").strip()
        if not url or "error.html" in url:
            return False
    return True


def deduplicate_online_items(items: list[dict]) -> list[dict]:
    """同一音源内去重；跨音源同名歌曲保留，便于用户切换版本。"""
    seen = set()
    res = []
    for it in items:
        if not is_playable_online_track(it):
            continue
        t = str(it.get("title") or it.get("name") or "").strip().lower()
        a = str(it.get("artist") or "").strip().lower()
        if t and a:
            key = (source_family(str(it.get("source") or "")), t, a)
            if key in seen:
                continue
            seen.add(key)
        res.append(it)
    return res


def play_format_from_ext(ext: str | None) -> str:
    raw = (ext or "mp3").strip().lower().lstrip(".")
    if raw.startswith("audio/"):
        raw = raw.split("/", 1)[-1]
    return _FORMAT_ALIASES.get(raw, raw or "mp3")


def filter_headers(headers: Any, exclude_keys: set | None = None) -> dict:
    exclude = HOP_BY_HOP | {k.lower() for k in (exclude_keys or set())}
    return {k: v for k, v in headers.items() if k.lower() not in exclude}


def copy_incoming_headers(request: Request) -> dict:
    """透传鉴权 Cookie / Token。Starlette 头名为小写，需显式回填以免丢失 music-token。"""
    headers = filter_headers(request.headers, exclude_keys={"host", "content-length"})
    headers["accept-encoding"] = "identity"
    for key in ("cookie", "authorization", "x-trim-music-temp-token"):
        val = request.headers.get(key)
        if val:
            headers[key] = val
    return headers


def get_by_path(d: Any, path: str) -> Any:
    curr = d
    for p in path.split("."):
        if isinstance(curr, dict) and p in curr:
            curr = curr[p]
        else:
            return None
    return curr


def set_by_path(d: dict, path: str, val: Any):
    parts = path.split(".")
    curr = d
    for p in parts[:-1]:
        if p not in curr or not isinstance(curr[p], dict):
            curr[p] = {}
        curr = curr[p]
    curr[parts[-1]] = val


def extract_keyword(request: Request) -> str:
    """前端打包用 q，部分调用/验收用 keyword。"""
    params = request.query_params
    return (params.get("keyword") or params.get("q") or params.get("query") or "").strip()


def online_guid_from_item(item: dict) -> str:
    raw_id = str(item.get("id") or "")
    src = str(item.get("source") or "")
    if raw_id.startswith("online:"):
        return raw_id
    if ":" in raw_id:
        return f"online:{raw_id}"
    return f"online:{src}:{raw_id}"


def song_id_from_online_guid(guid: str) -> str:
    if guid.startswith("online:"):
        return guid[len("online:") :]
    return guid


def is_online_guid(guid: str) -> bool:
    return bool(guid) and guid.startswith("online:")


def source_from_online_guid(guid: str) -> str:
    parts = (guid or "").split(":")
    return parts[1] if len(parts) >= 3 else ""


def build_online_track(item: dict) -> dict:
    """对齐飞牛前端 ZQ 解构 / _h() 期望：artists、album 对象、genres 数组、audioSpec、duration 毫秒。"""
    guid = online_guid_from_item(item)
    _set_online_entity_cache(guid, {**item, "ts": time.time()})
    src = str(item.get("source") or source_from_online_guid(guid) or "")
    title = str(item.get("title") or item.get("name") or "")
    label = source_label(src)
    artist = str(item.get("artist") or "")
    album = str(item.get("album") or "")
    display_album = f"{album} 〔{label}〕" if album else f"〔{label}〕"
    duration_s = item.get("duration_s") or 0
    try:
        duration_s = float(duration_s)
    except (TypeError, ValueError):
        duration_s = 0
    duration_ms = int(duration_s * 1000)
    ext = str(item.get("ext") or "mp3") or "mp3"
    play_format = play_format_from_ext(ext)
    file_size = item.get("file_size") or 0
    try:
        file_size = int(file_size or 0)
    except (TypeError, ValueError):
        file_size = 0
    cover = str(item.get("cover_url") or "")
    now = int(time.time())
    try:
        created_at = int(item.get("createdAt") or item.get("created_at") or now)
    except (TypeError, ValueError):
        created_at = now
    try:
        updated_at = int(item.get("updatedAt") or item.get("updated_at") or created_at)
    except (TypeError, ValueError):
        updated_at = created_at
    # 路径带真实后缀，飞牛 ll() 用 path 解析 extension；封面走 guid 以便 /static/cover 拦截
    spec_path = f"online/{src}/{guid}.{play_format}"

    # Use a resolvable, source-independent entity GUID.  Song GUID suffixes such
    # as ``online:qq:<mid>:artist`` cannot be parsed by the artist detail routes.
    artist_guid = f"online:netease:artist:name:{artist}" if artist else ""
    album_guid = f"online:netease:album:name:{album}" if album else f"{guid}:album"
    artists_list = [{"name": artist, "guid": artist_guid}] if artist else []
    album_obj = {
        "name": display_album,
        "guid": album_guid,
        "artists": artists_list,
        "coverId": guid,
    }
    audio_spec = {
        "path": spec_path,
        "format": play_format,
        "codec": play_format,
        "container": play_format,
        "duration": duration_ms,
        "size": file_size,
        "channel": 2,
        "sampleRate": 44100,
        "bitDepth": 16 if play_format in ("wav", "flac", "aiff") else None,
        "bitrate": 1411000 if play_format in ("flac", "wav", "ape", "wv") else 320000,
    }
    audio_spec = {k: v for k, v in audio_spec.items() if v is not None}

    return {
        "guid": guid,
        "id": guid,
        "title": title,
        "name": title,
        "artist": artist,
        "artists": artists_list,
        "album": album_obj,
        "albumName": display_album,
        "originalAlbum": album,
        "subtitle": f"来源：{label}",
        "audioSpec": audio_spec,
        "duration": duration_ms,
        "duration_ms": duration_ms,
        "durationMs": duration_ms,
        "duration_s": duration_s,
        "codec": play_format,
        "codecName": play_format,
        "format": play_format,
        "ext": ext,
        "size": file_size,
        "file_size": file_size,
        "coverId": guid,
        "cover_url": cover,
        "coverUrl": cover,
        "coverURL": cover,
        "source": src,
        "sourceName": label,
        "sourceLabel": label,
        "is_online": True,
        "isFavorite": False,
        "isCue": False,
        # Third-party fnOS clients use this flag to decide whether the lyric
        # endpoint is worth querying. Search APIs normally do not include the
        # lyric body, although every online source can resolve it on demand.
        "hasLyric": True,
        "genres": [],
        "accessStatus": 0,
        "createdAt": created_at,
        "updatedAt": updated_at,
    }


def repair_track_entity_links(track: dict) -> dict:
    """Upgrade persisted daily/favorite tracks created with legacy song suffix links."""
    if not isinstance(track, dict) or not is_online_guid(str(track.get("guid") or "")):
        return track
    artist = str(track.get("artist") or "").strip()
    artists = track.get("artists")
    if not artist and isinstance(artists, list) and artists and isinstance(artists[0], dict):
        artist = str(artists[0].get("name") or "").strip()
    if artist:
        artist_guid = online_entity_guid("artist:name", artist)
        if not isinstance(artists, list) or not artists:
            track["artists"] = [{"name": artist, "guid": artist_guid}]
        else:
            for entry in artists:
                if isinstance(entry, dict):
                    entry["guid"] = online_entity_guid(
                        "artist:name", str(entry.get("name") or artist).strip()
                    )

    album = track.get("album")
    album_name = str(track.get("originalAlbum") or "").strip()
    if isinstance(album, dict):
        if not album_name:
            album_name = str(album.get("name") or "").split(" 〔", 1)[0].strip()
        if album_name:
            album["guid"] = online_entity_guid("album:name", album_name)
        album_artists = album.get("artists")
        if isinstance(album_artists, list) and artist:
            for entry in album_artists:
                if isinstance(entry, dict):
                    entry["guid"] = online_entity_guid(
                        "artist:name", str(entry.get("name") or artist).strip()
                    )
    guid = str(track.get("guid") or "")
    if guid not in _ONLINE_ENTITY_CACHE:
        cover = str(track.get("cover_url") or track.get("coverUrl") or "")
        _set_online_entity_cache(guid, {
            "id": song_id_from_online_guid(guid),
            "source": source_from_online_guid(guid),
            "title": str(track.get("title") or ""),
            "artist": artist,
            "album": album_name,
            "cover_url": cover if cover.startswith(("http://", "https://")) else "",
            "ts": time.time(),
        })
    return track


def online_entity_guid(kind: str, raw_id: Any) -> str:
    return f"online:netease:{kind}:{raw_id}"


def parse_online_entity_guid(guid: str, kind: str | None = None) -> tuple[str, str] | None:
    parts = (guid or "").split(":")
    if len(parts) < 4 or parts[:2] != ["online", "netease"]:
        return None
    entity_kind = parts[2]
    if parts[3] == "name" and len(parts) >= 5:
        entity_kind = f"{entity_kind}-name"
        raw_id = ":".join(parts[4:])
    else:
        raw_id = ":".join(parts[3:])
    if kind and entity_kind != kind:
        return None
    return entity_kind, raw_id


def build_online_artist(item: dict) -> dict | None:
    raw_id = item.get("artist_id") or item.get("id")
    name = str(item.get("artists_name") or item.get("artist_name") or item.get("name") or "").strip()
    if not raw_id or not name:
        return None
    guid = online_entity_guid("artist", raw_id)
    cover = entity_cover_url(item)
    result = {
        "guid": guid,
        "id": guid,
        "name": name,
        # Third-party clients do not derive an artist image from its tracks;
        # a non-empty coverId is required before they call /static/cover.
        "coverId": guid,
        "cover_url": cover,
        "albumCount": int(item.get("album_count") or 0),
        "trackCount": int(item.get("track_count") or 0),
        "alias": str(item.get("alias") or ""),
        "isOnline": True,
    }
    _set_online_entity_cache(guid, {**result, "raw": dict(item), "ts": time.time()})
    return result


def build_online_album(item: dict) -> dict | None:
    raw_id = item.get("album_id") or item.get("id")
    name = str(item.get("albums_name") or item.get("album_name") or item.get("name") or "").strip()
    if not raw_id or not name:
        return None
    artist_name = str(item.get("artists_name") or item.get("artist_name") or item.get("artist") or "").strip()
    guid = online_entity_guid("album", raw_id)
    cover = entity_cover_url(item)
    artists = []
    if artist_name:
        artists = [{"guid": online_entity_guid("artist:name", artist_name), "name": artist_name}]
    result = {
        "guid": guid,
        "id": guid,
        "name": name,
        "artists": artists,
        "coverId": guid,
        "cover_url": cover,
        "trackCount": int(item.get("track_count") or 0),
        "releaseYear": item.get("release_year"),
        "isOnline": True,
    }
    _set_online_entity_cache(guid, {**result, "raw": dict(item), "ts": time.time()})
    return result


def build_online_playlist(item: dict) -> dict | None:
    raw_id = item.get("playlist_id") or item.get("id")
    name = str(item.get("playlist_name") or item.get("name") or "").strip()
    if not raw_id or not name:
        return None
    guid = online_entity_guid("playlist", raw_id)
    cover = entity_cover_url(item)
    result = {
        "guid": guid,
        "id": guid,
        "name": name,
        "coverId": guid,
        "cover_url": cover,
        "trackCount": int(item.get("track_count") or 0),
        "createdAt": int(item.get("created_at") or 0),
        "updatedAt": int(item.get("updated_at") or 0),
        "creatorName": str(item.get("creator_name") or ""),
        "isOnline": True,
    }
    _set_online_entity_cache(guid, {**result, "raw": dict(item), "ts": time.time()})
    return result


def artist_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    a = item.get("artist") or item.get("singer") or item.get("singers") or ""
    if isinstance(a, list):
        names = []
        for x in a:
            if isinstance(x, dict):
                names.append(str(x.get("name") or ""))
            else:
                names.append(str(x))
        return " ".join(n for n in names if n).strip().lower()
    if isinstance(a, dict):
        return str(a.get("name") or "").strip().lower()
    return str(a).strip().lower()


def title_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("title") or item.get("name") or "").strip().lower()


def should_cache(range_header: str | None) -> bool:
    """完整拉取才落盘：无 Range，或 bytes=0-（开区间）。Safari bytes=0-1 探测不落盘。"""
    if not range_header:
        return True
    r = range_header.strip().lower()
    return bool(re.match(r"^bytes=0-$", r))


def is_range_from_zero_or_none(range_header: str | None) -> bool:
    return should_cache(range_header)


def cache_safe_guid(guid: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", guid)


def online_file_id(guid: str) -> str:
    """online:migu:600929… → 600929…，仅用于查找旧文件，不再写进文件名。"""
    return song_id_from_online_guid(guid).rsplit(":", 1)[-1]


def safe_basename_title(title: str) -> str:
    t = re.sub(r'[/\\:\0]', "_", (title or "").strip()) or "unknown"
    t = re.sub(r"\s+", " ", t).strip(" .")
    return (t or "unknown")[:120]


def library_basename(title: str, artist: str = "") -> str:
    """歌手目录内的曲库文件名：仅使用歌名，不包含源站 id。"""
    return safe_basename_title(title)


def artist_directory_name(artist: str) -> str:
    """把同名歌手稳定映射到同一安全目录；缺失歌手时集中到“未知歌手”。"""
    raw = (artist or "").strip()
    if not raw:
        return "未知歌手"
    safe = safe_basename_title(raw)
    return "未知歌手" if safe == "unknown" else safe


def library_artist_dir(artist: str) -> str:
    root = detect_library_dir()
    directory = os.path.join(root, artist_directory_name(artist))
    os.makedirs(directory, exist_ok=True)
    try:
        parent_stat = os.stat(root)
        os.chown(directory, parent_stat.st_uid, parent_stat.st_gid)
        os.chmod(directory, 0o755)
    except Exception:
        pass
    return directory


def media_ref_path(guid: str) -> str:
    return os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.ref")


def _path_stem(path: str) -> str:
    root, ext = os.path.splitext(path)
    known = set(CACHE_EXTS) | {"lrc", "part"}
    if ext.lstrip(".").lower() in known:
        return root
    return path


def remember_media_path(guid: str, media_path: str) -> None:
    """记住曲库里的文件词干（不含扩展名），音频和 .lrc 共用。"""
    try:
        os.makedirs(CONF["cache_dir"], exist_ok=True)
        with open(media_ref_path(guid), "w", encoding="utf-8") as f:
            f.write(_path_stem(media_path))
    except Exception as e:
        logger.warning("Failed to remember media path for %s: %s", guid, e)


def recalled_media_stem(guid: str) -> str | None:
    ref = media_ref_path(guid)
    if not os.path.exists(ref):
        return None
    try:
        with open(ref, encoding="utf-8") as f:
            stem = _path_stem(f.read().strip())
        if stem:
            return stem
    except Exception:
        return None
    return None


def recalled_media_path(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if not stem:
        return None
    for ext in CACHE_EXTS:
        path = f"{stem}.{ext}"
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


def unique_library_path(directory: str, basename: str, ext: str) -> str:
    dest = os.path.join(directory, f"{basename}.{ext}")
    if not os.path.exists(dest):
        return dest
    n = 2
    while os.path.exists(os.path.join(directory, f"{basename} ({n}).{ext}")):
        n += 1
    return os.path.join(directory, f"{basename} ({n}).{ext}")


def write_audio_tags(path: str, title: str, artist: str = "", album: str = "") -> None:
    """写入 title/artist/album，飞牛扫描后用标签而不是文件名显示。"""
    title, artist, album = (title or "").strip(), (artist or "").strip(), (album or "").strip()
    if not title and not artist:
        return
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(path, easy=True)
        if audio is None:
            return
        if getattr(audio, "tags", None) is None:
            try:
                audio.add_tags()
            except Exception:
                pass
        if title:
            audio["title"] = title
        if artist:
            audio["artist"] = artist
        if album:
            audio["album"] = album
        audio.save()
    except Exception as e:
        logger.warning("Failed to write audio tags for %s: %s", path, e)


def detect_library_dir() -> str:
    """优先环境变量，否则读飞牛 music.db 的共享库路径，最后回退到仓库 cache/。"""
    explicit = str(CONF.get("library_dir") or "").strip()
    if explicit:
        return explicit
    db = str(CONF.get("music_db") or "")
    if db and os.path.exists(db):
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                rows = con.execute("SELECT path FROM shared_library ORDER BY id").fetchall()
            finally:
                con.close()
            for (path,) in rows:
                if path and os.path.isdir(path):
                    return path
        except Exception as e:
            logger.warning("Failed to read shared_library path: %s", e)
    return CONF["cache_dir"]


def iter_media_dirs() -> list[str]:
    dirs: list[str] = []
    lib = detect_library_dir()
    for d in (lib, CONF["cache_dir"]):
        if d and d not in dirs:
            dirs.append(d)
    return dirs


def adopt_library_perms(path: str) -> None:
    try:
        parent = os.path.dirname(path) or "."
        st = os.stat(parent)
        os.chown(path, st.st_uid, st.st_gid)
        os.chmod(path, 0o644)
    except Exception:
        pass


def find_cache_file(guid: str) -> str | None:
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    file_id = online_file_id(guid)
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        for ext in CACHE_EXTS:
            exact = os.path.join(d, f"{safe}.{ext}")
            if os.path.exists(exact) and os.path.getsize(exact) > 0:
                return exact
            pattern = os.path.join(d, f"* - {glob.escape(file_id)}.{ext}")
            for path in glob.glob(pattern):
                if os.path.getsize(path) > 0:
                    return path
    return None


def promote_cache_hit(guid: str, audio_path: str) -> str:
    """旧 cache/ 音频：若曲库已有对应文件或歌词，则对齐过去。"""
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    lib = detect_library_dir()
    try:
        if os.path.abspath(os.path.dirname(audio_path)) == os.path.abspath(lib):
            remember_media_path(guid, audio_path)
            return audio_path
    except Exception:
        return audio_path
    file_id = online_file_id(guid)
    ext = os.path.splitext(audio_path)[1] or ".mp3"
    dest = None
    if os.path.isdir(lib):
        for lrc in glob.glob(os.path.join(lib, f"* - {glob.escape(file_id)}.lrc")):
            dest = os.path.splitext(lrc)[0] + ext
            break
    if not dest:
        return audio_path
    if not os.path.exists(dest):
        try:
            os.makedirs(lib, exist_ok=True)
            shutil.copy2(audio_path, dest)
            adopt_library_perms(dest)
        except Exception as e:
            logger.warning("Failed to promote cache audio into library: %s", e)
            return audio_path
    remember_media_path(guid, dest)
    return dest


def library_media_path(guid: str, title: str, ext: str, artist: str = "") -> str:
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    stem = recalled_media_stem(guid)
    if stem:
        return f"{stem}.{ext}"
    lib = detect_library_dir()
    file_id = online_file_id(guid)
    if os.path.isdir(lib):
        for path in glob.glob(os.path.join(lib, f"* - {glob.escape(file_id)}.{ext}")):
            if os.path.getsize(path) > 0:
                return path
    os.makedirs(lib, exist_ok=True)
    artist_dir = library_artist_dir(artist)
    return unique_library_path(artist_dir, library_basename(title, artist), ext)


def organize_cached_media(guid: str, audio_path: str, title: str, artist: str) -> str:
    """把旧版落在曲库根目录的在线音频及同名歌词迁入歌手目录。"""
    if not audio_path or not (title or "").strip() or not (artist or "").strip():
        return audio_path
    target_dir = library_artist_dir(artist)
    try:
        if os.path.abspath(os.path.dirname(audio_path)) == os.path.abspath(target_dir):
            remember_media_path(guid, audio_path)
            return audio_path
    except Exception:
        return audio_path

    ext = os.path.splitext(audio_path)[1].lstrip(".") or "mp3"
    dest = unique_library_path(target_dir, library_basename(title, artist), ext)
    old_lyric = os.path.splitext(audio_path)[0] + ".lrc"
    new_lyric = os.path.splitext(dest)[0] + ".lrc"
    try:
        try:
            os.replace(audio_path, dest)
        except OSError:
            shutil.move(audio_path, dest)
        adopt_library_perms(dest)
        if os.path.exists(old_lyric) and not os.path.exists(new_lyric):
            try:
                os.replace(old_lyric, new_lyric)
            except OSError:
                shutil.move(old_lyric, new_lyric)
            adopt_library_perms(new_lyric)
        remember_media_path(guid, dest)
        return dest
    except Exception as e:
        logger.warning("Failed to organize cached media for %s: %s", guid, e)
        return audio_path


def find_lyric_file(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if stem:
        sibling = f"{stem}.lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    audio = find_cache_file(guid)
    if audio:
        sibling = os.path.splitext(audio)[0] + ".lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    file_id = online_file_id(guid)
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        exact = os.path.join(d, f"{safe}.lrc")
        if os.path.exists(exact) and os.path.getsize(exact) > 0:
            return exact
        pattern = os.path.join(d, f"* - {glob.escape(file_id)}.lrc")
        for path in glob.glob(pattern):
            if os.path.getsize(path) > 0:
                return path
    return None


def lyric_cache_path(guid: str, title: str = "", artist: str = "") -> str:
    found = find_lyric_file(guid)
    if found:
        return found
    audio = find_cache_file(guid)
    if audio:
        return os.path.splitext(audio)[0] + ".lrc"
    if (title or "").strip() or (artist or "").strip():
        d = library_artist_dir(artist)
        return os.path.join(d, f"{library_basename(title, artist)}.lrc")
    d = detect_library_dir()
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{cache_safe_guid(guid)}.lrc")


def normalize_timed_lyric(value: Any) -> str:
    """Normalize LRC/QRC variants returned by QQ, NetEase, musicdl and LX."""
    if isinstance(value, dict):
        for key in ("lyric", "lrc", "content", "text"):
            if value.get(key):
                return normalize_timed_lyric(value[key])
        return ""
    if isinstance(value, list):
        return "\n".join(filter(None, (normalize_timed_lyric(item) for item in value))).strip()
    text = str(value or "").strip().lstrip("\ufeff")
    if not text:
        return ""
    for _ in range(2):
        decoded = html.unescape(text)
        if decoded == text:
            break
        text = decoded
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    qrc_attr = re.search(r'LyricContent=["\'](.*?)["\'](?:\s|/?>)', text, re.S | re.I)
    if qrc_attr:
        text = html.unescape(qrc_attr.group(1))

    # Some QQ-compatible servers ignore decode=1 and still return base64 LRC.
    if "[" not in text and len(text) >= 24 and re.fullmatch(r"[A-Za-z0-9+/=\s]+", text):
        try:
            candidate = base64.b64decode("".join(text.split()), validate=True).decode("utf-8-sig")
            if "[" in candidate:
                text = candidate
        except Exception:
            pass

    # QQ QRC uses [start_ms,duration_ms] and per-word (start,duration) marks.
    def qrc_timestamp(match: re.Match[str]) -> str:
        milliseconds = int(match.group(1))
        minutes, remainder = divmod(milliseconds, 60000)
        seconds, millis = divmod(remainder, 1000)
        return f"[{minutes:02d}:{seconds:02d}.{millis:03d}]"

    text = re.sub(r"\[(\d+),(\d+)\]", qrc_timestamp, text)
    text = re.sub(r"\(\d+,\d+\)", "", text)
    text = re.sub(r"\[(\d{1,3}):(\d{2})[,:](\d{1,3})\]", r"[\1:\2.\3]", text)
    return "\n".join(line.strip() for line in text.splitlines() if line.strip()).strip()


def read_lyric_cache(guid: str) -> str:
    path = find_lyric_file(guid)
    if not path:
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            return normalize_timed_lyric(f.read())
    except Exception as e:
        logger.warning("Failed to read lyric cache %s: %s", path, e)
        return ""


def write_lyric_cache(guid: str, text: str, title: str = "", artist: str = "") -> None:
    text = normalize_timed_lyric(text)
    if not text:
        return
    existing = find_lyric_file(guid)
    if existing and (title or "").strip() and (artist or "").strip():
        target_dir = library_artist_dir(artist)
        if os.path.abspath(os.path.dirname(existing)) != os.path.abspath(target_dir):
            dest = unique_library_path(target_dir, library_basename(title, artist), "lrc")
            try:
                try:
                    os.replace(existing, dest)
                except OSError:
                    shutil.move(existing, dest)
                adopt_library_perms(dest)
                remember_media_path(guid, dest)
                existing = dest
            except Exception as e:
                logger.warning("Failed to organize lyric cache for %s: %s", guid, e)
    if existing:
        try:
            with open(existing, encoding="utf-8") as f:
                if text == f.read().strip():
                    return
        except Exception:
            pass
    path = lyric_cache_path(guid, title=title, artist=artist)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        with open(part_path, "w", encoding="utf-8") as f:
            f.write(text)
            f.write("\n")
        os.replace(part_path, path)
        adopt_library_perms(path)
        remember_media_path(guid, path)
    except Exception as e:
        logger.warning("Failed to write lyric cache %s: %s", path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass


async def cache_lyrics_from_musicdl(musicdl_client: httpx.AsyncClient, guid: str) -> dict | None:
    """与音频 tee 并行：把 musicdl /info 里的 LRC 落到曲库同目录 sidecar。"""
    song_id = song_id_from_online_guid(guid)
    try:
        r = await musicdl_client.get("/info", params={"id": song_id}, timeout=10.0)
        if r.status_code != 200:
            return None
        data = r.json()
        if not (isinstance(data, dict) and data.get("ok") is not False):
            return None
        write_lyric_cache(
            guid,
            str(data.get("lyric") or ""),
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
        return data
    except Exception as e:
        logger.warning("lyric sidecar fetch failed for %s: %s", guid, e)
        return None


def _pick_lyric_match(items: list[dict], title: str, artist: str) -> dict | None:
    wanted_title = title.strip().casefold()
    wanted_artist = artist.strip().casefold()
    compact = lambda value: re.sub(r"[^\w\u4e00-\u9fff]+", "", value.casefold())
    wanted_title_compact = compact(wanted_title)
    wanted_artist_compact = compact(wanted_artist)
    for item in items:
        item_title = str(item.get("title") or item.get("name") or "").strip().casefold()
        item_artist = str(item.get("artist") or "").strip().casefold()
        title_matches = item_title == wanted_title or (
            bool(wanted_title_compact) and compact(item_title) == wanted_title_compact
        )
        item_artist_compact = compact(item_artist)
        artist_matches = not wanted_artist_compact or (
            wanted_artist_compact in item_artist_compact
            or item_artist_compact in wanted_artist_compact
        )
        if title_matches and artist_matches:
            return item
    return None


def _pick_playback_match(
    items: list[dict], title: str, artist: str, expected_duration_s: float = 0
) -> dict | None:
    """Pick the full-length exact match instead of a same-name preview/edit."""
    matches = []
    for index, item in enumerate(items):
        if _pick_lyric_match([item], title, artist) is None:
            continue
        try:
            duration = float(item.get("duration_s") or item.get("duration") or 0)
        except (TypeError, ValueError):
            duration = 0.0
        short_preview = expected_duration_s >= 90 and 0 < duration < max(
            60, expected_duration_s * 0.55
        )
        duration_delta = abs(duration - expected_duration_s) if duration and expected_duration_s else 0
        matches.append((short_preview, duration_delta, -duration, index, item))
    if not matches:
        return None
    matches.sort(key=lambda row: row[:3])
    return matches[0][4]


def response_media_size(headers: Any) -> int:
    content_range = str(headers.get("content-range") or "")
    matched = re.search(r"/(\d+)\s*$", content_range)
    if matched:
        return int(matched.group(1))
    value = str(headers.get("content-length") or "")
    return int(value) if value.isdigit() else 0


def is_probable_audio_preview(info: dict | None, media_size: int, url: str = "") -> bool:
    """Detect common 30-second trial streams without downloading them fully."""
    lowered_url = (url or "").casefold()
    if any(marker in lowered_url for marker in ("preview", "trial", "audition")):
        return True
    try:
        duration_s = float((info or {}).get("duration_s") or 0)
    except (TypeError, ValueError):
        duration_s = 0.0
    # Tiny payloads are common in unit probes and error wrappers; a real
    # encoded 30-second preview is normally well above 128 KiB.
    if duration_s < 90 or media_size < 128 * 1024:
        return False
    # Even a low 64-kbit/s full track needs roughly 8 KB/s. Keep a small floor
    # so a 30-second 128-kbit/s preview (~480 KB) is rejected reliably.
    minimum_full_size = max(700_000, int(duration_s * 8_000 * 0.75))
    return media_size < minimum_full_size


async def fetch_preferred_lyric(request: Request, guid: str, provider: str) -> str:
    info = dict(_ONLINE_ENTITY_CACHE.get(guid) or {})
    if not info:
        info = dict(await _online_info(request, guid) or {})
    title = str(info.get("title") or info.get("name") or "").strip()
    artist = str(info.get("artist") or "").strip()
    if not title:
        return ""
    if source_family(source_from_online_guid(guid)) == provider:
        direct_info = await _online_info(request, guid) or {}
        direct_text = normalize_timed_lyric(
            direct_info.get("lyric") or direct_info.get("lyrics")
        )
        if direct_text:
            return direct_text
    keyword = " ".join(part for part in (title, artist) if part)

    try:
        if provider == "netease":
            items = await fetch_musicbox_search(get_musicbox_client(request.app), keyword, 10) or []
            match = _pick_lyric_match(items, title, artist)
            if match:
                song_id = str(match.get("id") or "").split(":")[-1]
                response = await get_musicbox_client(request.app).get(
                    f"/api/v1/song/{song_id}/lyric", timeout=10.0
                )
                payload = response.json() if response.status_code == 200 else {}
                data = payload.get("data") if isinstance(payload, dict) else None
                return normalize_timed_lyric(data.get("lyric")) if isinstance(data, dict) else ""
        elif provider == "qqmusic":
            items = await fetch_qqmusic_search(get_qqmusic_client(request.app), keyword, 10) or []
            match = _pick_lyric_match(items, title, artist)
            if match:
                song_mid = str(match.get("qq_mid") or match.get("id") or "").split(":")[-1]
                response = await get_qqmusic_client(request.app).get(
                    "/song/lyric", params={"mid": song_mid, "decode": 1}, timeout=12.0
                )
                payload = response.json() if response.status_code == 200 else {}
                data = payload.get("data") if isinstance(payload, dict) else None
                return normalize_timed_lyric(data.get("lyric")) if isinstance(data, dict) else ""
        elif provider == "musicdl":
            payload = await fetch_musicdl_search(get_musicdl_client(request.app), keyword, 10) or {}
            items = payload.get("items") if isinstance(payload.get("items"), list) else []
            match = _pick_lyric_match(items, title, artist)
            if match:
                response = await get_musicdl_client(request.app).get(
                    "/info", params={"id": str(match.get("id") or "")}, timeout=10.0
                )
                data = response.json() if response.status_code == 200 else {}
                return normalize_timed_lyric(data.get("lyric")) if isinstance(data, dict) else ""
        elif provider == "lx":
            result = await resolve_lx_action(request, guid, info, action="lyric")
            return normalize_timed_lyric(result)
        elif provider == "lxmusic":
            items = await fetch_lx_search(get_lx_client(request.app), keyword, 10) or []
            match = _pick_lyric_match(items, title, artist)
            if match:
                matched_guid = online_guid_from_item(match)
                _set_online_entity_cache(matched_guid, {**match, "ts": time.time()})
                detail = await _online_info(request, matched_guid) or match
                return normalize_timed_lyric(detail.get("lyric") or detail.get("lyrics"))
    except Exception as exc:
        logger.warning("preferred lyric source %s failed for %s: %s", provider, guid, exc)
    return ""


async def resolve_online_lyric(request: Request, guid: str) -> str:
    """本地 .lrc 优先；没有再向源站要，拿到就落盘。"""
    preferred = SOURCE_REGISTRY.preference("lyricSource")
    if preferred not in ("auto", "same"):
        preferred_text = normalize_timed_lyric(await fetch_preferred_lyric(request, guid, preferred))
        if preferred_text:
            info = _ONLINE_ENTITY_CACHE.get(guid) or {}
            write_lyric_cache(
                guid,
                preferred_text,
                title=str(info.get("title") or ""),
                artist=str(info.get("artist") or ""),
            )
            return preferred_text

    cached = read_lyric_cache(guid)
    if cached:
        return cached

    src = source_from_online_guid(guid)
    if src == "netease":
        musicbox_client = get_musicbox_client(request.app)
        raw_song_id = song_id_from_online_guid(guid)
        song_id = raw_song_id.split(":")[-1]
        try:
            r = await musicbox_client.get(f"/api/v1/song/{song_id}/lyric", timeout=10.0)
            if r.status_code == 200:
                res_data = r.json()
                if isinstance(res_data, dict) and res_data.get("ok") is not False:
                    l_data = res_data.get("data")
                    if isinstance(l_data, dict):
                        lyric_text = normalize_timed_lyric(l_data.get("lyric"))
                        if lyric_text:
                            info = await _online_info(request, guid)
                            write_lyric_cache(
                                guid,
                                lyric_text,
                                title=str((info or {}).get("title") or ""),
                                artist=str((info or {}).get("artist") or ""),
                            )
                            return lyric_text
        except Exception as e:
            logger.warning("musicbox lyric fetch failed for %s: %s", guid, e)
        return ""

    data = await _online_info(request, guid)
    text = normalize_timed_lyric((data or {}).get("lyric"))
    if text:
        write_lyric_cache(
            guid,
            text,
            title=str((data or {}).get("title") or ""),
            artist=str((data or {}).get("artist") or ""),
        )
    return text


def media_type_for_ext(ext: str) -> str:
    return {
        "mp3": "audio/mpeg",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "ogg": "audio/ogg",
        "opus": "audio/ogg",
        "m4a": "audio/mp4",
        "aac": "audio/aac",
        "ape": "audio/x-ape",
        "wv": "audio/x-wavpack",
        "dsf": "audio/x-dsd",
        "dff": "audio/x-dff",
        "tta": "audio/x-tta",
        "wma": "audio/x-ms-wma",
        "aiff": "audio/aiff",
    }.get(ext.lower(), "application/octet-stream")


def ext_from_content_type(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "flac" in ct:
        return "flac"
    if "wavpack" in ct or "x-wv" in ct:
        return "wv"
    if "wav" in ct or "wave" in ct:
        return "wav"
    if "opus" in ct:
        return "opus"
    if "ogg" in ct:
        return "ogg"
    if "ape" in ct:
        return "ape"
    if "aiff" in ct:
        return "aiff"
    if "mp4" in ct or "m4a" in ct:
        return "m4a"
    if "aac" in ct:
        return "aac"
    if "mpeg" in ct or "mp3" in ct:
        return "mp3"
    return play_format_from_ext(ct.split("/")[-1] if "/" in ct else "mp3")


def parse_http_range(range_header: str | None, file_size: int) -> tuple[int, int] | None:
    if not range_header:
        return None
    m = re.match(r"bytes=(\d*)-(\d*)", range_header.strip(), re.I)
    if not m:
        return None
    start_s, end_s = m.group(1), m.group(2)
    if start_s == "" and end_s == "":
        return None
    if start_s == "":
        suffix = int(end_s)
        start = max(file_size - suffix, 0)
        end = file_size - 1
    else:
        start = int(start_s)
        end = int(end_s) if end_s else file_size - 1
    end = min(end, file_size - 1)
    if start < 0 or start >= file_size or start > end:
        return None
    return start, end


def serve_file_with_range(path: str, range_header: str | None, media_type: str) -> Response:
    file_size = os.path.getsize(path)
    rng = parse_http_range(range_header, file_size)

    def iter_file(offset: int, length: int) -> AsyncGenerator[bytes, None]:
        async def gen() -> AsyncGenerator[bytes, None]:
            remaining = length
            with open(path, "rb") as fp:
                fp.seek(offset)
                while remaining > 0:
                    chunk = fp.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return gen()

    if rng is None:
        return StreamingResponse(
            iter_file(0, file_size),
            status_code=200,
            headers={
                "Content-Type": media_type,
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes",
            },
        )

    start, end = rng
    length = end - start + 1
    return StreamingResponse(
        iter_file(start, length),
        status_code=206,
        headers={
            "Content-Type": media_type,
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
        },
    )


def _missing_local_track(item: Any) -> bool:
    """True when a fnOS track still points at a deleted local media file."""
    if not isinstance(item, dict) or is_online_guid(str(item.get("guid") or "")):
        return False
    spec = item.get("audioSpec")
    path = str(spec.get("path") or "") if isinstance(spec, dict) else ""
    # fnOS libraries are mounted below /volN. Restrict the filesystem probe to
    # that namespace so URLs and synthetic paths are never treated as deleted.
    if not re.match(r"^/vol\d+(?:/|$)", path):
        return False
    return not os.path.isfile(path)


def prune_missing_local_tracks(value: Any) -> tuple[Any, int]:
    """Remove stale fnOS track rows from arbitrary API envelopes.

    The official scanner may leave a DB row visible for a while after a file is
    deleted outside the Music UI. Filtering at the proxy keeps web and native
    third-party clients consistent immediately, without writing to music.db.
    """
    removed = 0
    if isinstance(value, list):
        result = []
        for item in value:
            if _missing_local_track(item):
                removed += 1
                continue
            cleaned, child_removed = prune_missing_local_tracks(item)
            removed += child_removed
            result.append(cleaned)
        return result, removed
    if isinstance(value, dict):
        result = dict(value)
        for key, child in value.items():
            cleaned, child_removed = prune_missing_local_tracks(child)
            result[key] = cleaned
            removed += child_removed
            direct_removed = (
                len(child) - len(cleaned)
                if isinstance(child, list) and isinstance(cleaned, list)
                else 0
            )
            if direct_removed and key in {"list", "items", "rows", "tracks"}:
                total = result.get("total")
                if isinstance(total, int):
                    result["total"] = max(total - direct_removed, 0)
        return result, removed
    return value, 0


def get_upstream_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "upstream_client", None)
    if client is None:
        transport = httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"])
        client = httpx.AsyncClient(transport=transport, base_url="http://unix", timeout=30.0)
        fastapi_app.state.upstream_client = client
    return client


def get_musicdl_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "musicdl_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["musicdl_url"], timeout=45.0)
        fastapi_app.state.musicdl_client = client
    return client


def get_musicbox_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "musicbox_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["musicbox_url"], timeout=20.0)
        fastapi_app.state.musicbox_client = client
    return client


def get_qqmusic_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "qqmusic_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["qqmusic_url"], timeout=25.0)
        fastapi_app.state.qqmusic_client = client
    return client


def get_lx_source_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "lx_source_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["lx_source_url"], timeout=25.0)
        fastapi_app.state.lx_source_client = client
    return client


def get_lx_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    """Client for the searchable LX aggregation service (not the JS runner)."""
    client = getattr(fastapi_app.state, "lx_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["lx_url"], timeout=25.0)
        fastapi_app.state.lx_client = client
    return client


def get_cover_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "cover_client", None)
    if client is None:
        client = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        fastapi_app.state.cover_client = client
    return client


def get_llm_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "llm_client", None)
    if client is None:
        client = httpx.AsyncClient(timeout=dailyrec.LLM_TIMEOUT_S)
        fastapi_app.state.llm_client = client
    return client


async def forward_to_upstream(request: Request, client: httpx.AsyncClient) -> Response:
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"

    headers = copy_incoming_headers(request)
    body = await request.body()

    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    resp = await client.send(req, stream=True)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})

    content_type = resp.headers.get("content-type", "").lower()
    if request.method == "GET" and "text/html" in content_type:
        content = await resp.aread()
        await resp.aclose()
        marker = b"/music/api/v1/_ext/assets/settings.js"
        if marker not in content:
            tag = b'<link rel="stylesheet" href="/music/api/v1/_ext/assets/settings.css"><script defer src="/music/api/v1/_ext/assets/settings.js"></script>'
            lower = content.lower()
            position = lower.rfind(b"</body>")
            content = content[:position] + tag + content[position:] if position >= 0 else content + tag
        return Response(
            content=content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )

    if request.method == "GET" and "application/json" in content_type:
        content = await resp.aread()
        await resp.aclose()
        try:
            payload = json.loads(content)
        except Exception:
            payload = None
        if isinstance(payload, (dict, list)):
            cleaned, removed = prune_missing_local_tracks(payload)
            if removed:
                logger.info("filtered %d missing local track(s) from %s", removed, request.url.path)
            return JSONResponse(content=cleaned, status_code=resp.status_code, headers=resp_headers)

    async def body_stream() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()

    return StreamingResponse(
        body_stream(),
        status_code=resp.status_code,
        headers=resp_headers,
    )


async def fetch_upstream_envelope(request: Request, client: httpx.AsyncClient) -> Response | dict:
    """透传上游并解析 JSON 信封。失败时返回 Response，成功返回 dict。"""
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)
    body = await request.body()
    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    if not isinstance(payload, dict):
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    payload, removed = prune_missing_local_tracks(payload)
    if removed:
        logger.info("filtered %d missing local track(s) from %s", removed, request.url.path)
    payload["_ext_headers"] = resp_headers
    return payload


async def fetch_musicdl_search(client: httpx.AsyncClient, keyword: str, limit: int) -> dict | None:
    if not keyword:
        return None
    params: dict[str, Any] = {"keyword": keyword, "limit": limit}
    if CONF["online_sources"]:
        params["sources"] = CONF["online_sources"]
    timeout = max(float(CONF.get("search_timeout") or 25), 8.0)
    try:
        r = await client.get("/search", params=params, timeout=timeout)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                if data.get("errors"):
                    logger.warning("musicdl search partial errors: %s", data.get("errors"))
                raw_items = data.get("items")
                if isinstance(raw_items, list):
                    data["items"] = [it for it in raw_items if is_playable_online_track(it)]
                return data
    except Exception as e:
        logger.warning("Failed to fetch online search from musicdl: %s", e)
    return None


async def fetch_qqmusic_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict] | None:
    if not keyword:
        return None
    try:
        response = await client.post(
            "/search/byType",
            json={"keyword": keyword, "type": 0, "num": min(max(limit, 1), 100), "page": 1},
            timeout=25.0,
        )
        if response.status_code == 200:
            payload = response.json()
            if isinstance(payload, dict) and payload.get("code") in (None, 0):
                tracks = extract_qq_tracks(payload)
                if tracks:
                    return tracks
        response = await client.post(
            "/search/general",
            json={"keyword": keyword, "num": min(max(limit, 1), 100), "page": 1},
            timeout=25.0,
        )
        if response.status_code == 200:
            payload = response.json()
            if isinstance(payload, dict) and payload.get("code") in (None, 0):
                tracks = extract_qq_tracks(payload)
                if tracks:
                    return tracks
        response = await client.get("/search/quick", params={"keyword": keyword}, timeout=15.0)
        if response.status_code == 200:
            return extract_qq_tracks(response.json())
        return None
    except Exception as exc:
        logger.warning("Failed to fetch QQ Music search: %s", exc)
        return None


async def resolve_lx_action(
    request: Request,
    guid: str,
    item: dict,
    action: str = "musicUrl",
    quality: str = "flac",
) -> Any:
    if not source_enabled("lx"):
        return None
    origin_source = str(
        item.get("lx_origin_source")
        or item.get("source")
        or source_from_online_guid(guid)
    )
    source = lx_source_key(origin_source)
    music_info = lx_music_info({**item, "source": origin_source}, guid)
    try:
        response = await get_lx_source_client(request.app).post(
            "/api/v1/resolve",
            json={
                "source": source,
                "action": action,
                "quality": quality,
                "musicInfo": music_info,
            },
            timeout=25.0,
        )
        if response.status_code != 200:
            return None
        payload = response.json()
        return payload.get("value") if isinstance(payload, dict) and payload.get("ok") else None
    except Exception as exc:
        logger.warning("LX source %s failed for %s: %s", action, guid, exc)
        return None


async def resolve_qq_url(client: httpx.AsyncClient, song_mid: str) -> tuple[str, str, int]:
    preferred = str(CONF.get("qqmusic_quality") or "F000").upper()
    quality_order = []
    for quality in (preferred, "F000", "O800", "M800", "M500"):
        if quality not in quality_order:
            quality_order.append(quality)
    for quality in quality_order:
        try:
            response = await client.get(
                "/song/urls", params={"mids": song_mid, "type": quality}, timeout=15.0
            )
            if response.status_code != 200:
                continue
            url, size = qq_play_url(response.json(), song_mid)
            if url:
                ext = "flac" if quality.startswith(("F", "Q", "AI")) else "ogg" if quality.startswith("O") else "mp3"
                return url, ext, size
        except Exception as exc:
            logger.debug("QQ Music URL quality %s failed for %s: %s", quality, song_mid, exc)
    return "", "mp3", 0


async def fetch_musicbox_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict] | None:
    if not keyword:
        return None
    try:
        r = await client.get(
            "/api/v1/search",
            params={"keyword": keyword, "limit": limit, "type": "song"},
            timeout=20.0,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, dict) or data.get("ok") is False:
            return None
        raw_list = data.get("data")
        if not isinstance(raw_list, list):
            return None
        items = []
        song_ids = []
        for it in raw_list:
            if not isinstance(it, dict):
                continue
            if not is_playable_online_track(it):
                continue
            sid = str(it.get("song_id") or it.get("id") or "")
            if not sid:
                continue
            title = str(it.get("song_name") or it.get("title") or it.get("name") or "")
            artist = str(it.get("artist") or "")
            album = str(it.get("album_name") or it.get("album") or "")
            duration = it.get("duration") or 0
            try:
                duration_s = float(duration)
            except (TypeError, ValueError):
                duration_s = 0.0
            quality = str(it.get("quality") or "").upper()
            ext = "flac" if any(q in quality for q in ("SQ", "HR", "无损")) else "mp3"
            items.append({
                "id": f"netease:{sid}",
                "source": "netease",
                "title": title,
                "artist": artist,
                "album": album,
                "duration_s": duration_s,
                "ext": ext,
                "cover_url": "",
                "lyric": "",
            })
            song_ids.append(sid)

        if song_ids:
            try:
                detail_resp = await client.get(
                    "/api/v1/songs/detail",
                    params={"ids": ",".join(song_ids)},
                    timeout=15.0,
                )
                if detail_resp.status_code == 200:
                    detail_json = detail_resp.json()
                    if isinstance(detail_json, dict) and detail_json.get("ok") is not False:
                        detail_list = detail_json.get("data")
                        if isinstance(detail_list, list):
                            detail_map = {}
                            for d_item in detail_list:
                                if isinstance(d_item, dict):
                                    d_sid = str(d_item.get("song_id") or d_item.get("id") or "")
                                    if d_sid:
                                        detail_map[d_sid] = d_item
                            for item in items:
                                raw_sid = item["id"].split(":", 1)[-1]
                                d_info = detail_map.get(raw_sid)
                                if d_info:
                                    pic_url = str(d_info.get("album_pic_url") or "")
                                    if pic_url:
                                        item["cover_url"] = pic_url
                                    if d_info.get("has_sq") or d_info.get("has_hr"):
                                        item["ext"] = "flac"
            except Exception as detail_err:
                logger.warning("Failed to fetch songs detail for %s: %s", keyword, detail_err)

        return [it for it in items if is_playable_online_track(it)]
    except Exception as e:
        logger.warning("Failed to fetch musicbox search: %s", e)
        return None


async def fetch_musicbox_entity_search(
    client: httpx.AsyncClient, keyword: str, entity_type: str, limit: int
) -> list[dict] | None:
    if not keyword or entity_type not in {"artist", "album", "playlist"}:
        return None
    try:
        r = await client.get(
            "/api/v1/search",
            params={"keyword": keyword, "limit": min(max(limit, 1), 100), "type": entity_type},
            timeout=20.0,
        )
        if r.status_code != 200:
            return None
        payload = r.json()
        if not isinstance(payload, dict) or payload.get("ok") is False:
            return None
        raw = payload.get("data")
        return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else None
    except Exception as e:
        logger.warning("Failed to fetch musicbox %s search: %s", entity_type, e)
        return None


def normalize_netease_track(item: dict) -> dict | None:
    song_id = item.get("song_id") or item.get("id")
    if not song_id:
        return None
    quality = str(item.get("quality") or item.get("level") or "").upper()
    raw_type = str(item.get("type") or "").lower()
    ext = "flac" if any(q in quality for q in ("SQ", "HR", "无损")) else (raw_type or "mp3")
    try:
        duration_s = float(item.get("duration") or item.get("duration_s") or 0)
    except (TypeError, ValueError):
        duration_s = 0.0
    return {
        "id": f"netease:{song_id}",
        "source": "netease",
        "title": str(item.get("song_name") or item.get("name") or item.get("title") or ""),
        "artist": str(item.get("artist") or item.get("artists_name") or ""),
        "album": str(item.get("album_name") or item.get("album") or ""),
        "album_id": item.get("album_id"),
        "duration_s": duration_s,
        "ext": play_format_from_ext(ext),
        "cover_url": str(item.get("cover_url") or item.get("album_pic_url") or ""),
        "lyric": "",
    }


async def load_online_entity_bundle(request: Request, guid: str) -> dict | None:
    parsed = parse_online_entity_guid(guid)
    if not parsed:
        return None
    entity_kind, raw_id = parsed
    cached = _ONLINE_ENTITY_CACHE.get(guid)
    if cached and cached.get("tracks") is not None:
        if time.time() - float(cached.get("detail_ts") or 0) < CONF["search_cache_ttl"]:
            return cached

    client = get_musicbox_client(request.app)
    resolved_guid = guid
    resolved_name = ""
    if entity_kind in {"artist-name", "album-name"}:
        lookup_kind = entity_kind.split("-", 1)[0]
        matches = await fetch_musicbox_entity_search(client, raw_id, lookup_kind, 20) or []
        name_keys = ("artists_name", "artist_name", "name") if lookup_kind == "artist" else ("albums_name", "album_name", "name")
        exact = next(
            (
                item
                for item in matches
                if str(next((item.get(key) for key in name_keys if item.get(key)), "")).casefold() == raw_id.casefold()
            ),
            matches[0] if matches else None,
        )
        if not exact:
            return None
        resolved_name = str(next((exact.get(key) for key in name_keys if exact.get(key)), raw_id))
        id_keys = ("artist_id", "id") if lookup_kind == "artist" else ("album_id", "id")
        raw_id = str(next((exact.get(key) for key in id_keys if exact.get(key)), ""))
        entity_kind = lookup_kind
        resolved_guid = online_entity_guid(lookup_kind, raw_id)

    endpoint = {
        "artist": f"/api/v1/artist/{raw_id}",
        "album": f"/api/v1/album/{raw_id}",
        "playlist": f"/api/v1/playlist/{raw_id}",
    }.get(entity_kind)
    if not endpoint:
        return None
    params = {"limit": 100} if entity_kind == "artist" else None
    try:
        response = await client.get(endpoint, params=params, timeout=40.0)
        if response.status_code != 200:
            return None
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("ok") is False:
            return None
        raw_tracks = payload.get("data")
        if not isinstance(raw_tracks, list):
            return None
    except Exception as e:
        logger.warning("Failed to load online %s %s: %s", entity_kind, raw_id, e)
        return None

    normalized = [normalize_netease_track(item) for item in raw_tracks if isinstance(item, dict)]
    tracks = [build_online_track(item) for item in normalized if item]
    prior = dict(_ONLINE_ENTITY_CACHE.get(guid) or _ONLINE_ENTITY_CACHE.get(resolved_guid) or {})
    name = str(prior.get("name") or "")
    if not name and normalized:
        if entity_kind == "album":
            name = str(normalized[0].get("album") or "")
        elif entity_kind == "artist":
            name = resolved_name or raw_id
    cover_id = str(tracks[0].get("guid") or "") if tracks else ""
    bundle = {
        **prior,
        "guid": guid,
        "id": guid,
        "name": name or str(prior.get("name") or "在线内容"),
        "coverId": cover_id,
        "cover_url": entity_cover_url(prior),
        "trackCount": len(tracks),
        "tracks": tracks,
        "detail_ts": time.time(),
        "entityKind": entity_kind,
        "rawId": raw_id,
    }
    if entity_kind == "artist":
        albums: dict[str, dict] = {}
        for item in normalized:
            album_id = item.get("album_id")
            album_name = str(item.get("album") or "").strip()
            if not album_id or not album_name:
                continue
            album_guid = online_entity_guid("album", album_id)
            albums.setdefault(
                album_guid,
                {
                    "guid": album_guid,
                    "id": album_guid,
                    "name": album_name,
                    "artists": [{"guid": guid, "name": bundle["name"]}],
                    "coverId": online_guid_from_item(item),
                    "trackCount": 0,
                    "isOnline": True,
                },
            )
            albums[album_guid]["trackCount"] += 1
        bundle["albums"] = list(albums.values())
        bundle["albumCount"] = len(albums)
    elif entity_kind == "album":
        artist_name = str(normalized[0].get("artist") or "") if normalized else ""
        bundle["artists"] = prior.get("artists") or (
            [{"guid": online_entity_guid("artist:name", artist_name), "name": artist_name}]
            if artist_name
            else []
        )
    _set_online_entity_cache(guid, bundle)
    if resolved_guid != guid:
        _set_online_entity_cache(
            resolved_guid, {**bundle, "guid": resolved_guid, "id": resolved_guid}
        )
    return bundle


async def fetch_lx_search(
    client: httpx.AsyncClient, keyword: str, limit: int
) -> list[dict] | None:
    """Search the standalone LX aggregation service."""
    if not keyword:
        return None
    timeout = max(float(CONF.get("search_timeout") or 25), 8.0)
    try:
        response = await client.get(
            "/api/v1/search",
            params={"keyword": keyword, "limit": limit},
            timeout=timeout,
        )
        if response.status_code != 200:
            return None
        payload = response.json()
        raw_items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(raw_items, list) or payload.get("ok") is False:
            return None
        items: list[dict] = []
        for raw in raw_items:
            if not isinstance(raw, dict) or not is_playable_online_track(raw):
                continue
            track_id = str(raw.get("id") or "")
            if not track_id:
                continue
            try:
                duration_s = float(raw.get("duration_s") or 0)
            except (TypeError, ValueError):
                duration_s = 0.0
            try:
                file_size = int(raw.get("file_size") or 0)
            except (TypeError, ValueError):
                file_size = 0
            items.append({
                "id": track_id,
                "source": "lx",
                "lx_source": str(raw.get("lx_source") or ""),
                "title": str(raw.get("title") or raw.get("name") or ""),
                "artist": str(raw.get("artist") or ""),
                "album": str(raw.get("album") or ""),
                "duration_s": duration_s,
                "ext": str(raw.get("ext") or "mp3") or "mp3",
                "cover_url": str(raw.get("cover_url") or ""),
                "file_size": file_size,
                "lyric": "",
            })
        return [item for item in items if is_playable_online_track(item)]
    except Exception as exc:
        logger.warning("Failed to fetch online search from lxmusic: %s", exc)
        return None


async def resolve_lx_url(client: httpx.AsyncClient, song_id: str) -> dict | None:
    """Resolve an LX aggregation ID such as ``lx:kg:<hash>``."""
    qualities: list[str] = []
    primary = str(CONF.get("lx_quality") or "lossless").strip()
    if primary:
        qualities.append(primary)
    for fallback in ("high", "standard"):
        if fallback not in qualities:
            qualities.append(fallback)
    for quality in qualities:
        try:
            response = await client.get(
                "/api/v1/track/url",
                params={"id": song_id, "quality": quality},
                timeout=15.0,
            )
            if response.status_code != 200:
                continue
            payload = response.json()
            data = payload.get("data") if isinstance(payload, dict) else None
            if payload.get("ok") is not False and isinstance(data, dict) and data.get("url"):
                return data
        except Exception as exc:
            logger.warning("resolve_lx_url error for %s (quality=%s): %s", song_id, quality, exc)
    return None


async def resolve_netease_url(client: httpx.AsyncClient, song_id: str) -> str | None:
    qualities = []
    primary = str(CONF.get("netease_quality") or "lossless").strip()
    if primary:
        qualities.append(primary)
    if "exhigh" not in qualities:
        qualities.append("exhigh")

    for q in qualities:
        try:
            r = await client.get(f"/api/v1/song/{song_id}/url", params={"quality": q}, timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data.get("ok") is not False:
                    inner = data.get("data")
                    if isinstance(inner, dict):
                        code = inner.get("code")
                        url = inner.get("url")
                        if code == 200 and url:
                            return str(url)
        except Exception as e:
            logger.warning("resolve_netease_url error for %s (quality=%s): %s", song_id, q, e)
    return None


def ensure_search_list(upstream_json: dict) -> list:
    """保证 data.list 存在，本地 0 条时仍能追加在线条目。"""
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {}
        upstream_json["data"] = data
    target = get_by_path(upstream_json, CONF["search_list_path"])
    if isinstance(target, list):
        return target
    for key in ("list", "items", "tracks", "records"):
        if isinstance(data.get(key), list):
            if key != "list":
                data["list"] = data[key]
            return data["list"]
    data["list"] = []
    if "total" not in data:
        data["total"] = 0
    return data["list"]


def merge_online_tracks(
    upstream_json: dict,
    online_data: list[dict] | dict | None,
    page: int = 1,
    size: int = 50,
) -> dict:
    target_list = ensure_search_list(upstream_json)
    if not online_data:
        return upstream_json

    if isinstance(online_data, dict):
        raw_items = online_data.get("items", [])
    elif isinstance(online_data, list):
        raw_items = online_data
    else:
        raw_items = []

    if not raw_items:
        return upstream_json

    existing_keys = set()
    for item in target_list:
        t = title_from_track(item)
        a = artist_from_track(item)
        if t and a:
            existing_keys.add((t, a))

    filtered_online = []
    for online_item in raw_items:
        if not is_playable_online_track(online_item, require_id=True):
            continue
        ot = str(online_item.get("title") or online_item.get("name") or "").strip().lower()
        oa = str(online_item.get("artist") or "").strip().lower()
        if ot and oa and (ot, oa) in existing_keys:
            continue
        filtered_online.append(online_item)

    # Keep the upstream page size so the client can continue requesting pages.
    # FNMUSIC_ONLINE_LIMIT is a safety cap per page, not a first-page-only cap.
    online_page_size = min(size, max(int(CONF["online_limit"]), 1))
    start = (page - 1) * online_page_size
    page_online = filtered_online[start : start + online_page_size]

    for it in page_online:
        target_list.append(build_online_track(it))

    parts = CONF["search_list_path"].split(".")
    parent = upstream_json
    for p in parts[:-1]:
        if isinstance(parent, dict) and p in parent:
            parent = parent[p]
    if isinstance(parent, dict):
        orig_total = parent.get("total")
        if not isinstance(orig_total, int):
            orig_total = len(target_list) - len(page_online)
        parent["total"] = orig_total + len(filtered_online)

    return upstream_json


def extract_guid(request: Request, path_guid: str | None = None) -> str:
    if path_guid:
        return path_guid
    return (
        request.query_params.get("guid")
        or request.query_params.get("trackGUID")
        or request.query_params.get("trackGuid")
        or request.query_params.get("coverId")
        or request.query_params.get("id")
        or request.query_params.get("trackId")
        or ""
    )


async def extract_guid_from_body(request: Request) -> str:
    guid = extract_guid(request)
    if guid:
        return guid
    try:
        body = await request.json()
    except Exception:
        return ""
    if isinstance(body, dict):
        return str(
            body.get("guid")
            or body.get("trackGUID")
            or body.get("trackGuid")
            or body.get("id")
            or body.get("trackId")
            or ""
        )
    return ""


def empty_ok() -> JSONResponse:
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {}})


def build_lyric_list_payload(guid: str, lyric_text: str) -> dict:
    """对齐飞牛 $n.lyric.list → xr(list, preferred)。

    每条需有非空 content；source=2 表示 EXTERNAL_LRC（非内嵌，不强制 offset）。
    """
    text = normalize_timed_lyric(lyric_text)
    if not text:
        return {"code": 0, "msg": "ok", "data": {"list": [], "preferred": ""}}
    lyric_guid = f"{guid}:lyric"
    now = int(time.time())
    item = {
        "guid": lyric_guid,
        "content": text,
        "source": 2,
        "isLRC": True,
        "offset": 0,
        "createdAt": now,
        "updatedAt": now,
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {"list": [item], "preferred": lyric_guid},
    }


def stub_online_info(guid: str) -> dict:
    song_id = song_id_from_online_guid(guid)
    return {
        "id": song_id,
        "source": source_from_online_guid(guid),
        "title": "",
        "artist": "",
        "album": "",
        "duration_s": 0,
        "ext": "mp3",
        "file_size": 0,
        "cover_url": "",
        "lyric": "",
    }


def build_metadata_payload(guid: str, data: dict | None) -> dict:
    """飞牛 resolveTrackPlayback._h() 会无防护读取 data.track.genres.join / album / artists。

    缺 genres 或 album 不是对象时直接抛错，播放器跳过且不会请求 stream。
    """
    info = dict(data or {})
    info.setdefault("id", song_id_from_online_guid(guid))
    info.setdefault("source", source_from_online_guid(guid))
    lyric_text = normalize_timed_lyric(info.get("lyric") or info.get("lyrics"))
    info["lyric"] = lyric_text
    info["lyrics"] = lyric_text
    vo = build_online_track(info)
    vo["lyric"] = lyric_text
    vo["lyrics"] = lyric_text
    album_obj = vo["album"] if isinstance(vo.get("album"), dict) else {
        "name": str(vo.get("album") or ""),
        "guid": f"{guid}:album",
        "artists": vo.get("artists") or [],
        "coverId": guid,
    }
    track = {
        "guid": guid,
        "id": guid,
        "title": vo.get("title") or "",
        "artists": vo.get("artists") or [],
        "album": album_obj,
        "genres": list(vo.get("genres") or []),
        "duration": vo.get("duration") or 0,
        "coverId": guid,
        "coverUrl": vo.get("coverUrl") or "",
        "format": vo.get("format") or "mp3",
        "hasLyric": True,
        "lyric": lyric_text,
        "lyrics": lyric_text,
        "isFavorite": False,
        "isCue": False,
        "accessStatus": 0,
        "audioSpec": vo["audioSpec"],
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {
            **vo,
            "guid": guid,
            "id": guid,
            "album": album_obj,
            "audioSpec": vo["audioSpec"],
            "track": track,
        },
    }


def _conf_log_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(part in lowered for part in _REDACT_KEY_PARTS):
        return "***" if value else ""
    return value


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    logger.info("=== fnmusic-ext v%s configuration ===", get_version())
    for k, v in CONF.items():
        logger.info("  %s = %s", k, _conf_log_value(k, v))
    logger.info("  llm_enabled = %s", dailyrec.llm_enabled())
    logger.info("==================================")

    created_upstream = False
    created_musicdl = False
    created_musicbox = False
    created_qqmusic = False
    created_lx_source = False
    created_lx = False
    created_llm = False
    created_cover = False

    if getattr(fastapi_app.state, "upstream_client", None) is None:
        fastapi_app.state.upstream_client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"]),
            base_url="http://unix",
            timeout=30.0,
        )
        created_upstream = True

    if getattr(fastapi_app.state, "musicdl_client", None) is None:
        fastapi_app.state.musicdl_client = httpx.AsyncClient(
            base_url=CONF["musicdl_url"],
            timeout=45.0,
        )
        created_musicdl = True

    if getattr(fastapi_app.state, "musicbox_client", None) is None:
        fastapi_app.state.musicbox_client = httpx.AsyncClient(
            base_url=CONF["musicbox_url"],
            timeout=20.0,
        )
        created_musicbox = True

    if getattr(fastapi_app.state, "qqmusic_client", None) is None:
        fastapi_app.state.qqmusic_client = httpx.AsyncClient(
            base_url=CONF["qqmusic_url"], timeout=25.0
        )
        created_qqmusic = True

    if getattr(fastapi_app.state, "lx_source_client", None) is None:
        fastapi_app.state.lx_source_client = httpx.AsyncClient(
            base_url=CONF["lx_source_url"], timeout=25.0
        )
        created_lx_source = True

    if getattr(fastapi_app.state, "lx_client", None) is None:
        fastapi_app.state.lx_client = httpx.AsyncClient(
            base_url=CONF["lx_url"], timeout=25.0
        )
        created_lx = True

    if getattr(fastapi_app.state, "llm_client", None) is None:
        fastapi_app.state.llm_client = httpx.AsyncClient(timeout=dailyrec.LLM_TIMEOUT_S)
        created_llm = True

    if getattr(fastapi_app.state, "cover_client", None) is None:
        fastapi_app.state.cover_client = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        created_cover = True

    try:
        yield
    finally:
        if created_upstream and getattr(fastapi_app.state, "upstream_client", None):
            await fastapi_app.state.upstream_client.aclose()
            fastapi_app.state.upstream_client = None
        if created_musicdl and getattr(fastapi_app.state, "musicdl_client", None):
            await fastapi_app.state.musicdl_client.aclose()
            fastapi_app.state.musicdl_client = None
        if created_musicbox and getattr(fastapi_app.state, "musicbox_client", None):
            await fastapi_app.state.musicbox_client.aclose()
            fastapi_app.state.musicbox_client = None
        if created_qqmusic and getattr(fastapi_app.state, "qqmusic_client", None):
            await fastapi_app.state.qqmusic_client.aclose()
            fastapi_app.state.qqmusic_client = None
        if created_lx_source and getattr(fastapi_app.state, "lx_source_client", None):
            await fastapi_app.state.lx_source_client.aclose()
            fastapi_app.state.lx_source_client = None
        if created_lx and getattr(fastapi_app.state, "lx_client", None):
            await fastapi_app.state.lx_client.aclose()
            fastapi_app.state.lx_client = None
        if created_llm and getattr(fastapi_app.state, "llm_client", None):
            await fastapi_app.state.llm_client.aclose()
            fastapi_app.state.llm_client = None
        if created_cover and getattr(fastapi_app.state, "cover_client", None):
            await fastapi_app.state.cover_client.aclose()
            fastapi_app.state.cover_client = None


app = FastAPI(title="fnmusic-ext", lifespan=lifespan)


async def require_ext_access(request: Request, mutation: bool = False) -> None:
    """Require a valid fnOS Music session; mutations also require our same-origin header."""
    if mutation:
        if request.headers.get("x-fnmusic-ext") != "1":
            raise HTTPException(status_code=403, detail="missing extension request header")
        origin = request.headers.get("origin")
        host = request.headers.get("host")
        if origin and host:
            try:
                if httpx.URL(origin).host != httpx.URL(f"http://{host}").host:
                    raise HTTPException(status_code=403, detail="cross-origin request rejected")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=403, detail="invalid origin") from exc
    try:
        response = await get_upstream_client(request.app).get(
            "/music/api/v1/settings/server",
            headers=copy_incoming_headers(request),
            timeout=5.0,
        )
        payload = response.json() if response.status_code == 200 else None
        if response.status_code != 200 or not isinstance(payload, dict) or payload.get("code") != 0:
            raise HTTPException(status_code=401, detail="fnOS Music login required")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=401, detail="unable to verify fnOS Music session") from exc


async def _service_status(client: httpx.AsyncClient, path: str) -> str:
    try:
        response = await client.get(path, timeout=2.5)
        return "ok" if response.status_code == 200 else f"HTTP {response.status_code}"
    except Exception:
        return "unavailable"


async def _service_json(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    body: dict | None = None,
) -> tuple[int, dict]:
    try:
        response = await client.request(method, path, json=body, timeout=30.0)
        payload = response.json()
        if not isinstance(payload, dict):
            payload = {"ok": False, "error": "invalid service response"}
        return response.status_code, payload
    except Exception as exc:
        return 503, {"ok": False, "error": str(exc)}


@app.get("/music/api/v1/_ext/assets/settings.js")
async def ext_settings_asset():
    return Response(
        content=(_STATIC_DIR / "ext-settings.js").read_bytes(),
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/music/api/v1/_ext/assets/settings.css")
async def ext_settings_style():
    return Response(
        content=(_STATIC_DIR / "ext-settings.css").read_bytes(),
        media_type="text/css",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/music/api/v1/_ext/settings")
async def ext_settings_page():
    return HTMLResponse(
        content=(_STATIC_DIR / "ext-settings.html").read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/music/api/v1/_ext/sources")
async def ext_sources(request: Request):
    await require_ext_access(request)
    clients = {
        "musicdl": (get_musicdl_client(request.app), "/healthz"),
        "netease": (get_musicbox_client(request.app), "/healthz"),
        "qqmusic": (get_qqmusic_client(request.app), "/health"),
        "lx": (get_lx_source_client(request.app), "/healthz"),
        "lxmusic": (get_lx_client(request.app), "/healthz"),
    }
    statuses = await asyncio.gather(*(_service_status(*clients[key]) for key in clients))
    status_map = dict(zip(clients, statuses))
    descriptions = {
        "musicdl": "酷我、咪咕等公开曲库聚合搜索",
        "netease": "网易云搜索、歌词与高品质播放",
        "qqmusic": "QQ 搜索及登录账号的会员音质",
        "lx": "用已导入的洛雪脚本补充播放地址",
        "lxmusic": "洛雪聚合搜索、歌词与免登录播放解析",
    }
    builtins = [
        {
            "id": source_id,
            "enabled": source_enabled(source_id),
            "status": status_map[source_id],
            "description": descriptions[source_id],
        }
        for source_id in clients
    ]

    _, lx_payload = await _service_json(get_lx_source_client(request.app), "GET", "/api/v1/sources")
    _, qq_payload = await _service_json(get_qqmusic_client(request.app), "GET", "/login/status")
    qq_data = qq_payload.get("data") if isinstance(qq_payload.get("data"), dict) else {}
    qq_summary: dict[str, Any] = {
        "loggedIn": bool(qq_data.get("loggedIn")),
        "expired": bool(qq_data.get("expired")),
        "musicid": qq_data.get("musicid"),
        "nickname": "",
    }
    if qq_summary["loggedIn"]:
        _, user_payload = await _service_json(get_qqmusic_client(request.app), "GET", "/user/self")
        user_data = user_payload.get("data") if isinstance(user_payload.get("data"), dict) else {}
        qq_summary["nickname"] = str(
            user_data.get("nick") or user_data.get("nickname") or user_data.get("name") or ""
        )
    return {
        "ok": True,
        "builtins": builtins,
        "lxSources": lx_payload.get("sources") if isinstance(lx_payload.get("sources"), list) else [],
        "qq": qq_summary,
        "preferences": {
            "audioSource": SOURCE_REGISTRY.preference("audioSource"),
            "lyricSource": SOURCE_REGISTRY.preference("lyricSource"),
        },
    }


@app.patch("/music/api/v1/_ext/preferences")
async def ext_source_preferences(request: Request):
    await require_ext_access(request, mutation=True)
    body = await request.json()
    allowed = {
        "audioSource": {"auto", "qqmusic", "netease", "musicdl", "lx", "lxmusic"},
        "lyricSource": {"auto", "same", "qqmusic", "netease", "musicdl", "lx", "lxmusic"},
    }
    if not isinstance(body, dict) or not body:
        raise HTTPException(status_code=400, detail="preference is required")
    for key, value in body.items():
        if key not in allowed or value not in allowed[key]:
            raise HTTPException(status_code=400, detail=f"invalid {key}")
    for key, value in body.items():
        SOURCE_REGISTRY.set_preference(key, value)
    _SEARCH_CACHE.clear()
    return {"ok": True, "preferences": {
        "audioSource": SOURCE_REGISTRY.preference("audioSource"),
        "lyricSource": SOURCE_REGISTRY.preference("lyricSource"),
    }}


@app.post("/music/api/v1/_ext/resolve-track-source")
async def ext_resolve_track_source(request: Request):
    """Resolve the currently playing song on a user-selected source."""
    await require_ext_access(request, mutation=True)
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="invalid request")
    guid = str(body.get("guid") or "").strip()
    provider = str(body.get("source") or "").strip().lower()
    if provider not in {"qqmusic", "netease", "musicdl", "lx", "lxmusic"}:
        raise HTTPException(status_code=400, detail="invalid source")
    if not source_enabled(provider):
        raise HTTPException(status_code=400, detail="该音乐源未启用")

    current = dict(_ONLINE_ENTITY_CACHE.get(guid) or {})
    title = str(body.get("title") or current.get("title") or current.get("name") or "").strip()
    artist = str(body.get("artist") or current.get("artist") or "").strip()
    if not title and is_online_guid(guid):
        current.update(await _online_info(request, guid) or {})
        title = str(current.get("title") or current.get("name") or "").strip()
        artist = str(current.get("artist") or "").strip()
    if not title and guid and not is_online_guid(guid):
        # Cached online files are rescanned by fnOS and receive a normal local
        # GUID. Native clients still expect the source button to work for them.
        try:
            response = await get_upstream_client(request.app).get(
                "/music/api/v1/track/metadata",
                params={"guid": guid},
                headers=copy_incoming_headers(request),
                timeout=10.0,
            )
            payload = response.json() if response.status_code == 200 else {}
            data = payload.get("data") if isinstance(payload, dict) else {}
            track = data.get("track") if isinstance(data, dict) and isinstance(data.get("track"), dict) else data
            if isinstance(track, dict):
                title = str(track.get("title") or track.get("name") or "").strip()
                artists = track.get("artists")
                if isinstance(artists, list):
                    artist = " / ".join(
                        str(item.get("name") or "")
                        for item in artists
                        if isinstance(item, dict) and item.get("name")
                    ).strip()
                if not artist:
                    artist = str(track.get("artist") or "").strip()
                current.update(track)
                current["title"] = title
                current["artist"] = artist
        except Exception as exc:
            logger.warning("failed to resolve local track metadata for source switch: %s", exc)
    if not title:
        raise HTTPException(status_code=400, detail="无法识别当前歌曲")

    keyword = " ".join(part for part in (title, artist) if part)
    if provider == "lx":
        # LX scripts resolve platform song IDs but do not provide a standalone
        # metadata search API.  Retain the current online ID where possible;
        # for a rescanned local cache, first recover a matching platform ID.
        origin = dict(current)
        origin_guid = guid
        origin_source = str(origin.get("source") or source_from_online_guid(guid))
        if lx_source_key(origin_source) == "local":
            preferred = SOURCE_REGISTRY.preference("audioSource")
            candidates = ["netease", "qqmusic", "musicdl"]
            if preferred in candidates:
                candidates.remove(preferred)
                candidates.insert(0, preferred)
            origin = {}
            for candidate in candidates:
                if not source_enabled(candidate):
                    continue
                tracks = await search_source_tracks(request, candidate, keyword, 12)
                match = _pick_lyric_match(tracks, title, artist)
                if match and lx_source_key(str(match.get("source") or candidate)) != "local":
                    origin = dict(match)
                    origin_guid = online_guid_from_item(match)
                    origin_source = str(match.get("source") or candidate)
                    break
        if not origin or lx_source_key(origin_source) == "local":
            raise HTTPException(status_code=404, detail="洛雪需要先匹配到歌曲的平台 ID")
        origin_id = str(
            origin.get("qq_mid")
            or origin.get("id")
            or song_id_from_online_guid(origin_guid).split(":")[-1]
        )
        play_url = await resolve_lx_action(
            request,
            origin_guid,
            {
                **origin,
                "source": origin_source,
                "lx_music_id": origin_id,
            },
            quality="flac",
        )
        if not isinstance(play_url, str) or not play_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=404, detail="已启用的洛雪源无法解析这首歌")
        token = uuid4().hex
        lx_item = {
            **origin,
            "id": token,
            "source": "lxsource",
            "title": title,
            "artist": artist,
            "play_url": play_url,
            "lx_origin_guid": origin_guid,
            "lx_origin_source": origin_source,
            "lx_music_id": origin_id,
            "ext": origin.get("ext") or "flac",
        }
        track = build_online_track(lx_item)
        return {
            "ok": True,
            "track": track,
            "streamUrl": f"/music/api/v1/track/stream?guid={quote(track['guid'], safe='')}",
            "source": "lx",
            "sourceName": source_label("lxsource"),
        }

    tracks = await search_source_tracks(request, provider, keyword, 12)
    match = _pick_lyric_match(tracks, title, artist)
    if not match:
        raise HTTPException(status_code=404, detail="所选音乐源没有找到这首歌")
    resolved_guid = online_guid_from_item(match)
    _set_online_entity_cache(resolved_guid, {**match, "ts": time.time()})
    return {
        "ok": True,
        "track": build_online_track(match),
        "streamUrl": f"/music/api/v1/track/stream?guid={quote(resolved_guid, safe='')}",
        "source": provider,
        "sourceName": source_label(str(match.get("source") or provider)),
    }


@app.patch("/music/api/v1/_ext/sources/{source_id}")
async def ext_source_toggle(request: Request, source_id: str):
    await require_ext_access(request, mutation=True)
    body = await request.json()
    if not isinstance(body, dict) or not isinstance(body.get("enabled"), bool):
        raise HTTPException(status_code=400, detail="enabled must be boolean")
    searchable = ("musicdl", "netease", "qqmusic")
    if source_id in searchable and body["enabled"] is False:
        enabled_searchable = [item for item in searchable if source_enabled(item)]
        if enabled_searchable == [source_id]:
            raise HTTPException(status_code=400, detail="at least one searchable source must stay enabled")
    try:
        SOURCE_REGISTRY.set_enabled(source_id, body["enabled"])
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="unknown built-in source") from exc
    _SEARCH_CACHE.clear()
    return {"ok": True, "id": source_id, "enabled": body["enabled"]}


@app.post("/music/api/v1/_ext/lx-sources")
async def ext_lx_source_create(request: Request):
    await require_ext_access(request, mutation=True)
    body = await request.json()
    status, payload = await _service_json(
        get_lx_source_client(request.app), "POST", "/api/v1/sources", body
    )
    return JSONResponse(content=payload, status_code=status)


@app.patch("/music/api/v1/_ext/lx-sources/{source_id}")
async def ext_lx_source_update(request: Request, source_id: str):
    await require_ext_access(request, mutation=True)
    body = await request.json()
    status, payload = await _service_json(
        get_lx_source_client(request.app),
        "PATCH",
        f"/api/v1/sources/{quote(source_id, safe='')}",
        body,
    )
    return JSONResponse(content=payload, status_code=status)


@app.delete("/music/api/v1/_ext/lx-sources/{source_id}")
async def ext_lx_source_delete(request: Request, source_id: str):
    await require_ext_access(request, mutation=True)
    status, payload = await _service_json(
        get_lx_source_client(request.app),
        "DELETE",
        f"/api/v1/sources/{quote(source_id, safe='')}",
    )
    return JSONResponse(content=payload, status_code=status)


@app.post("/music/api/v1/_ext/qq/qrcode")
async def ext_qq_qrcode(request: Request):
    await require_ext_access(request, mutation=True)
    status, payload = await _service_json(
        get_qqmusic_client(request.app), "POST", "/login/qrcode", {"type": "qq"}
    )
    return JSONResponse(content={"ok": status == 200, "data": payload.get("data"), "error": payload.get("message")}, status_code=status)


@app.post("/music/api/v1/_ext/qq/qrcode/check")
async def ext_qq_qrcode_check(request: Request):
    await require_ext_access(request, mutation=True)
    body = await request.json()
    safe_body = {"identifier": str(body.get("identifier") or ""), "type": "qq"}
    status, payload = await _service_json(
        get_qqmusic_client(request.app), "POST", "/login/checkQrcode", safe_body
    )
    raw_data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    # QQMusicapi returns the newly issued credential on success.  It belongs only
    # in the dedicated service data store and must never cross the proxy boundary.
    public_data = {"event": raw_data.get("event")}
    return JSONResponse(content={"ok": status == 200, "data": public_data, "error": payload.get("message")}, status_code=status)


@app.post("/music/api/v1/_ext/qq/logout")
async def ext_qq_logout(request: Request):
    await require_ext_access(request, mutation=True)
    status, payload = await _service_json(get_qqmusic_client(request.app), "POST", "/login/logout", {})
    return JSONResponse(content={"ok": status == 200, "data": payload.get("data"), "error": payload.get("message")}, status_code=status)


@app.get("/_ext/healthz")
async def ext_healthz(request: Request):
    upstream_client = get_upstream_client(request.app)
    musicdl_client = get_musicdl_client(request.app)
    musicbox_client = get_musicbox_client(request.app)
    qqmusic_client = get_qqmusic_client(request.app)
    lx_source_client = get_lx_source_client(request.app)
    lx_client = get_lx_client(request.app)

    upstream_status = "fail"
    musicdl_status = "fail"
    musicbox_status = "fail"
    qqmusic_status = "fail"
    lx_source_status = "fail"
    lxmusic_status = "fail"

    try:
        r = await upstream_client.get("/music/api/v1/search/track?keyword=healthz_probe", timeout=2.0)
        if r.status_code < 500:
            upstream_status = "ok"
    except Exception as e:
        logger.debug("Upstream health check failed: %s", e)

    if not source_enabled("musicdl"):
        musicdl_status = "disabled"
    else:
        try:
            r = await musicdl_client.get("/healthz", timeout=2.0)
            if r.status_code == 200:
                musicdl_status = "ok"
        except Exception as e:
            logger.debug("Musicdl health check failed: %s", e)

    if not source_enabled("netease"):
        musicbox_status = "disabled"
    else:
        try:
            r = await musicbox_client.get("/healthz", timeout=2.0)
            if r.status_code == 200:
                musicbox_status = "ok"
        except Exception as e:
            logger.debug("Musicbox health check failed: %s", e)

    if not source_enabled("qqmusic"):
        qqmusic_status = "disabled"
    else:
        try:
            r = await qqmusic_client.get("/health", timeout=2.0)
            if r.status_code == 200:
                qqmusic_status = "ok"
        except Exception as e:
            logger.debug("QQ Music health check failed: %s", e)

    if not source_enabled("lx"):
        lx_source_status = "disabled"
    else:
        try:
            r = await lx_source_client.get("/healthz", timeout=2.0)
            if r.status_code == 200:
                lx_source_status = "ok"
        except Exception as e:
            logger.debug("LX source health check failed: %s", e)

    if not source_enabled("lxmusic"):
        lxmusic_status = "disabled"
    else:
        try:
            r = await lx_client.get("/healthz", timeout=2.0)
            if r.status_code == 200:
                lxmusic_status = "ok"
        except Exception as e:
            logger.debug("LX aggregation health check failed: %s", e)

    llm_status = "enabled" if dailyrec.llm_enabled() else "disabled"
    source_ok = any(
        status == "ok"
        for status in (musicdl_status, musicbox_status, qqmusic_status, lxmusic_status)
    )

    return {
        "ok": upstream_status == "ok" and source_ok,
        "version": get_version(),
        "upstream": upstream_status,
        "musicdl": musicdl_status,
        "musicbox": musicbox_status,
        "qqmusic": qqmusic_status,
        "lx_source": lx_source_status,
        "lxmusic": lxmusic_status,
        "llm": llm_status,
    }


@app.get("/music/api/v1/search/track")
@app.get("/music/api/v1/search/track/{subpath:path}")
async def search_track(request: Request):
    upstream_client = get_upstream_client(request.app)
    musicdl_client = get_musicdl_client(request.app)
    musicbox_client = get_musicbox_client(request.app)
    qqmusic_client = get_qqmusic_client(request.app)
    keyword = extract_keyword(request)

    page_str = request.query_params.get("page")
    try:
        page = int(page_str) if page_str else 1
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1

    size_str = request.query_params.get("size")
    try:
        size = int(size_str) if size_str else 50
    except (TypeError, ValueError):
        size = 50
    if size < 1:
        size = 50

    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)

    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    upstream_json, removed = prune_missing_local_tracks(upstream_json)
    if removed:
        logger.info("filtered %d missing local track(s) from search", removed)

    if not keyword:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    now = time.time()
    cached_entry = _SEARCH_CACHE.get(keyword)
    is_valid_cache = cached_entry is not None and (now - cached_entry.get("ts", 0) < CONF["search_cache_ttl"])

    if is_valid_cache and cached_entry is not None:
        task = cached_entry.get("task")
        if task and not task.done() and page >= 2:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=CONF["late_page_wait_s"])
            except Exception:
                pass
        online_all = cached_entry.get("items", [])
    else:
        entry: dict[str, Any] = {"items": [], "ts": time.time(), "task": None}
        tasks: dict[str, asyncio.Task] = {}
        if source_enabled("netease"):
            tasks["netease"] = asyncio.create_task(
                fetch_musicbox_search(musicbox_client, keyword, CONF["netease_search_limit"])
            )
        if source_enabled("musicdl"):
            tasks["musicdl"] = asyncio.create_task(
                fetch_musicdl_search(musicdl_client, keyword, CONF["musicdl_search_limit"])
            )
        if source_enabled("qqmusic"):
            tasks["qqmusic"] = asyncio.create_task(
                fetch_qqmusic_search(qqmusic_client, keyword, CONF["qqmusic_search_limit"])
            )
        if source_enabled("lxmusic"):
            tasks["lxmusic"] = asyncio.create_task(
                fetch_lx_search(get_lx_client(request.app), keyword, CONF["lx_search_limit"])
            )

        def _collect_source_items(results: list[Any]) -> list[dict]:
            merged_items: list[dict] = []
            for result in results:
                if isinstance(result, dict) and isinstance(result.get("items"), list):
                    merged_items.extend(result["items"])
                elif isinstance(result, list):
                    merged_items.extend(item for item in result if isinstance(item, dict))
            return order_online_items(deduplicate_online_items(merged_items))

        async def _bg_aggregator(e: dict) -> None:
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            for name, result in zip(tasks, results):
                if isinstance(result, Exception):
                    logger.warning("%s search bg failed: %s", name, result)
            e["items"] = _collect_source_items(results)

        agg_task = asyncio.create_task(_bg_aggregator(entry))
        entry["task"] = agg_task
        _set_search_cache(keyword, entry)

        if page == 1 and tasks:
            wait_budget = float(CONF.get("netease_wait_s", 3.0))
            done, pending = await asyncio.wait(
                list(tasks.values()), timeout=wait_budget, return_when=asyncio.ALL_COMPLETED
            )
            if done and not agg_task.done():
                completed = []
                for task in done:
                    if not task.cancelled() and task.exception() is None:
                        completed.append(task.result())
                entry["items"] = _collect_source_items(completed)
            if not done and pending:
                late_done, _ = await asyncio.wait(
                    pending,
                    timeout=float(CONF.get("late_page_wait_s", 5.0)),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if late_done and not agg_task.done():
                    completed = [
                        task.result()
                        for task in late_done
                        if not task.cancelled() and task.exception() is None
                    ]
                    entry["items"] = _collect_source_items(completed)
        elif page >= 2:
            try:
                await asyncio.wait_for(
                    asyncio.shield(agg_task), timeout=CONF["late_page_wait_s"]
                )
            except (asyncio.TimeoutError, Exception):
                pass

        online_all = entry.get("items", [])

    merged = merge_online_tracks(upstream_json, online_all, page=page, size=size)
    return JSONResponse(content=merged, status_code=upstream_resp.status_code, headers=resp_headers)


def merge_online_entities(
    upstream_json: dict, online_items: list[dict], page: int, size: int
) -> dict:
    target = ensure_search_list(upstream_json)
    existing = {
        str(item.get("name") or item.get("title") or "").strip().casefold()
        for item in target
        if isinstance(item, dict)
    }
    filtered = [
        item
        for item in online_items
        if str(item.get("name") or "").strip().casefold() not in existing
    ]
    page_size = min(max(size, 1), max(int(CONF["online_limit"]), 1))
    start = (max(page, 1) - 1) * page_size
    target.extend(filtered[start : start + page_size])
    data = upstream_json.get("data")
    if isinstance(data, dict):
        local_total = data.get("total")
        if not isinstance(local_total, int):
            local_total = len(target) - len(filtered[start : start + page_size])
        data["total"] = local_total + len(filtered)
    return upstream_json


async def _search_entity(request: Request, entity_type: str) -> Response:
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    keyword = extract_keyword(request)
    if not keyword or not source_enabled("netease"):
        return JSONResponse(content=envelope, headers=headers)
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 24)
    except (TypeError, ValueError):
        size = 24
    if size < 1:
        size = 24

    cache_key = (entity_type, keyword.casefold())
    cached = _ENTITY_SEARCH_CACHE.get(cache_key)
    now = time.time()
    if cached and now - float(cached.get("ts") or 0) < CONF["search_cache_ttl"]:
        built = list(cached.get("items") or [])
    else:
        raw_items = await fetch_musicbox_entity_search(
            get_musicbox_client(request.app), keyword, entity_type, CONF["online_limit"]
        ) or []
        builder = {
            "artist": build_online_artist,
            "album": build_online_album,
            "playlist": build_online_playlist,
        }[entity_type]
        built = []
        seen: set[tuple[str, str]] = set()
        for raw in raw_items:
            item = builder(raw)
            if not item:
                continue
            artist_names = ",".join(
                str(a.get("name") or "") for a in item.get("artists", []) if isinstance(a, dict)
            )
            dedupe_key = (str(item.get("name") or "").casefold(), artist_names.casefold())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            built.append(item)
        _ENTITY_SEARCH_CACHE[cache_key] = {"items": built, "ts": now}
        _clean_search_cache()
    return JSONResponse(
        content=merge_online_entities(envelope, built, page=page, size=size), headers=headers
    )


@app.get("/music/api/v1/search/artist")
async def search_artist(request: Request):
    return await _search_entity(request, "artist")


@app.get("/music/api/v1/search/album")
async def search_album(request: Request):
    return await _search_entity(request, "album")


@app.get("/music/api/v1/search/playlist")
async def search_playlist(request: Request):
    return await _search_entity(request, "playlist")


@app.get("/music/api/v1/search/suggest")
@app.get("/music/api/v1/search/suggest/{subpath:path}")
async def search_suggest(request: Request):
    if not CONF["merge_suggest"]:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    keyword = extract_keyword(request)

    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    if not keyword:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    musicbox_client = get_musicbox_client(request.app)
    tasks = [
        fetch_musicdl_search(get_musicdl_client(request.app), keyword, 5)
        if source_enabled("musicdl")
        else asyncio.sleep(0, result=None),
        fetch_musicbox_search(musicbox_client, keyword, 5)
        if source_enabled("netease")
        else asyncio.sleep(0, result=None),
        fetch_musicbox_entity_search(musicbox_client, keyword, "album", 5)
        if source_enabled("netease")
        else asyncio.sleep(0, result=None),
        fetch_musicbox_entity_search(musicbox_client, keyword, "artist", 5)
        if source_enabled("netease")
        else asyncio.sleep(0, result=None),
        fetch_musicbox_entity_search(musicbox_client, keyword, "playlist", 5)
        if source_enabled("netease")
        else asyncio.sleep(0, result=None),
    ]
    try:
        musicdl_data, musicbox_tracks, albums_raw, artists_raw, playlists_raw = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=12.0
        )
    except asyncio.TimeoutError:
        musicdl_data, musicbox_tracks, albums_raw, artists_raw, playlists_raw = None, None, None, None, None

    mdl_items = (
        musicdl_data.get("items", [])
        if isinstance(musicdl_data, dict) and isinstance(musicdl_data.get("items"), list)
        else []
    )
    track_items = deduplicate_online_items(
        (musicbox_tracks if isinstance(musicbox_tracks, list) else []) + mdl_items
    )[:5]

    data_field = upstream_json.get("data")
    if isinstance(data_field, list):
        for item in track_items:
            title = item.get("title")
            if title and title not in data_field:
                data_field.append(title)
    elif isinstance(data_field, dict):
        builders_and_raw = {
            "album": (build_online_album, albums_raw),
            "artist": (build_online_artist, artists_raw),
            "playlist": (build_online_playlist, playlists_raw),
        }
        online_groups: dict[str, list[dict]] = {
            "track": [build_online_track(item) for item in track_items]
        }
        for group, (builder, raw_items) in builders_and_raw.items():
            online_groups[group] = [
                built
                for item in (raw_items if isinstance(raw_items, list) else [])
                if isinstance(item, dict) and (built := builder(item)) is not None
            ][:5]
        for group, additions in online_groups.items():
            container = data_field.get(group)
            if not isinstance(container, dict):
                container = {"items": [], "total": 0}
                data_field[group] = container
            key = "items" if isinstance(container.get("items"), list) else "list"
            current = container.get(key)
            if not isinstance(current, list):
                current = []
            known = {
                str(item.get("guid") or item.get("name") or item.get("title") or "")
                for item in current
                if isinstance(item, dict)
            }
            for item in additions:
                identity = str(item.get("guid") or item.get("name") or item.get("title") or "")
                if identity and identity not in known:
                    current.append(item)
                    known.add(identity)
            container[key] = current
            container["total"] = max(int(container.get("total") or 0), len(current))

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


def stream_tee_response(
    resp: httpx.Response,
    guid: str,
    range_header: str | None,
    coro_factory: Callable[[], Coroutine[Any, Any, Any]] | None = None,
    client_to_close: httpx.AsyncClient | None = None,
    resolved_ext: str | None = None,
    pre_info: dict | None = None,
) -> Response:
    out_headers = {"Accept-Ranges": "bytes"}
    for k in ("content-type", "content-length", "content-range"):
        v = resp.headers.get(k)
        if v:
            out_headers[k] = v

    if resolved_ext:
        out_headers["content-type"] = media_type_for_ext(resolved_ext)

    status_code = resp.status_code
    content_length_str = resp.headers.get("content-length")
    content_length = (
        int(content_length_str) if content_length_str and content_length_str.isdigit() else None
    )

    ext = (resolved_ext or "").strip().lower() or ext_from_content_type(resp.headers.get("content-type") or "")
    store_dir = detect_library_dir()

    if should_cache(range_header):
        os.makedirs(store_dir, exist_ok=True)
        part_path = os.path.join(store_dir, f"{cache_safe_guid(guid)}.{uuid4().hex[:8]}.part")
        info_task: asyncio.Task | None = None
        if pre_info is None and coro_factory is not None:
            info_task = asyncio.create_task(coro_factory())

        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def _downloader():
            written = 0
            part_file = None
            try:
                part_file = open(part_path, "wb")
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        part_file.write(chunk)
                        written += len(chunk)
                        await queue.put(chunk)
            except Exception as e:
                logger.warning("tee download failed for %s: %s", guid, e)
            finally:
                if part_file:
                    try:
                        part_file.close()
                    except Exception:
                        pass
                await resp.aclose()
                if client_to_close:
                    await client_to_close.aclose()
                info: dict | None = pre_info
                if info is None and info_task:
                    try:
                        info = await asyncio.wait_for(asyncio.shield(info_task), timeout=8.0)
                    except Exception as e:
                        logger.warning("info/lyric wait failed for %s: %s", guid, e)
                title = str((info or {}).get("title") or "")
                artist = str((info or {}).get("artist") or "")
                album = str((info or {}).get("album") or "")
                lyric = str((info or {}).get("lyric") or "")
                if lyric:
                    write_lyric_cache(guid, lyric, title=title, artist=artist)
                complete = written >= 1024 and (content_length is None or written == content_length)
                if complete:
                    dest = library_media_path(guid, title, ext, artist=artist)
                    try:
                        os.replace(part_path, dest)
                        adopt_library_perms(dest)
                        remember_media_path(guid, dest)
                        write_audio_tags(dest, title=title, artist=artist, album=album)
                        if lyric.strip():
                            write_lyric_cache(guid, lyric, title=title, artist=artist)
                    except Exception as e:
                        logger.warning("Failed to rename cache file: %s", e)
                        if os.path.exists(part_path):
                            try:
                                os.remove(part_path)
                            except Exception:
                                pass
                elif os.path.exists(part_path):
                    try:
                        os.remove(part_path)
                    except Exception:
                        pass
                await queue.put(None)

        dl_task = asyncio.create_task(_downloader())

        async def stream_tee() -> AsyncGenerator[bytes, None]:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk

        return StreamingResponse(stream_tee(), status_code=status_code, headers=out_headers)

    async def stream_no_cache() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in resp.aiter_bytes():
                if chunk:
                    yield chunk
        finally:
            await resp.aclose()
            if client_to_close:
                await client_to_close.aclose()

    return StreamingResponse(stream_no_cache(), status_code=status_code, headers=out_headers)


async def stream_direct_online(
    request: Request,
    guid: str,
    play_url: str,
    range_header: str | None,
    info: dict | None,
    resolved_ext: str | None = None,
    allow_full_fallback: bool = True,
) -> Response:
    req_headers = {"Range": range_header} if range_header else {}
    stream_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    try:
        stream_req = stream_client.build_request("GET", play_url, headers=req_headers)
        response = await stream_client.send(stream_req, stream=True)
        if response.status_code >= 400:
            await response.aclose()
            await stream_client.aclose()
            raise RuntimeError(f"direct source returned HTTP {response.status_code}")
    except Exception as exc:
        logger.warning("Failed to stream direct URL for %s: %s", guid, exc)
        await stream_client.aclose()
        return JSONResponse(
            content={"code": 404, "msg": "online source unavailable", "data": None},
            status_code=404,
        )
    media_size = response_media_size(response.headers)
    if is_probable_audio_preview(info, media_size, play_url):
        logger.warning(
            "Rejected probable preview stream for %s: expected=%ss bytes=%s",
            guid,
            (info or {}).get("duration_s"),
            media_size,
        )
        await response.aclose()
        await stream_client.aclose()
        if allow_full_fallback:
            fallback = await stream_full_length_fallback(request, guid, info or {}, range_header)
            if fallback is not None:
                return fallback
        return JSONResponse(
            content={"code": 409, "msg": "only a short preview was available", "data": None},
            status_code=409,
        )
    return stream_tee_response(
        response,
        guid=guid,
        range_header=range_header,
        coro_factory=None if info is not None else (lambda: _online_info(request, guid)),
        client_to_close=stream_client,
        resolved_ext=resolved_ext,
        pre_info=info,
    )


async def stream_full_length_fallback(
    request: Request, original_guid: str, original_info: dict, range_header: str | None
) -> Response | None:
    """Try matching providers until one yields a plausibly complete stream."""
    title = str(original_info.get("title") or original_info.get("name") or "").strip()
    artist = str(original_info.get("artist") or "").strip()
    if not title:
        return None
    try:
        expected_duration = float(original_info.get("duration_s") or 0)
    except (TypeError, ValueError):
        expected_duration = 0.0
    keyword = " ".join(part for part in (title, artist) if part)
    current_family = source_family(source_from_online_guid(original_guid))
    preferred = SOURCE_REGISTRY.preference("audioSource")
    providers = ["netease", "qqmusic", "musicdl"]
    if preferred != "auto" and preferred in providers:
        providers.remove(preferred)
        providers.insert(0, preferred)

    for provider in providers:
        if provider == current_family or not source_enabled(provider):
            continue
        try:
            tracks = await search_source_tracks(request, provider, keyword, 15)
            match = _pick_playback_match(tracks, title, artist, expected_duration)
            if not match:
                continue
            candidate_guid = online_guid_from_item(match)
            try:
                candidate_duration = float(match.get("duration_s") or 0)
            except (TypeError, ValueError):
                candidate_duration = 0.0
            if expected_duration >= 90 and 0 < candidate_duration < expected_duration * 0.7:
                continue
            _set_online_entity_cache(candidate_guid, {**match, "ts": time.time()})

            if provider == "qqmusic":
                song_mid = song_id_from_online_guid(candidate_guid).split(":")[-1]
                url, ext, declared_size = await resolve_qq_url(
                    get_qqmusic_client(request.app), song_mid
                )
                if not url or is_probable_audio_preview(match, declared_size, url):
                    continue
                return await stream_direct_online(
                    request,
                    original_guid,
                    url,
                    range_header,
                    match,
                    ext,
                    allow_full_fallback=False,
                )

            if provider == "netease":
                song_id = song_id_from_online_guid(candidate_guid).split(":")[-1]
                url = await resolve_netease_url(get_musicbox_client(request.app), song_id)
                if not url:
                    continue
                return await stream_direct_online(
                    request,
                    original_guid,
                    url,
                    range_header,
                    match,
                    str(match.get("ext") or "mp3"),
                    allow_full_fallback=False,
                )

            req_headers = {"Range": range_header} if range_header else {}
            response = await get_musicdl_client(request.app).send(
                get_musicdl_client(request.app).build_request(
                    "GET",
                    "/stream",
                    params={"id": song_id_from_online_guid(candidate_guid), "proxy": "true"},
                    headers=req_headers,
                ),
                stream=True,
            )
            if response.status_code >= 400 or is_probable_audio_preview(
                match, response_media_size(response.headers), str(response.url)
            ):
                await response.aclose()
                continue
            return stream_tee_response(
                response,
                guid=original_guid,
                range_header=range_header,
                coro_factory=None,
                client_to_close=None,
                resolved_ext=str(match.get("ext") or "mp3"),
                pre_info=match,
            )
        except Exception as exc:
            logger.warning("full stream fallback %s failed for %s: %s", provider, original_guid, exc)
    return None


async def online_history_snapshot(request: Request, guid: str) -> dict:
    """Keep enough metadata for recent-play lists after the search cache expires."""
    info = dict(_ONLINE_ENTITY_CACHE.get(guid) or {})
    if not str(info.get("title") or info.get("name") or "").strip():
        resolved = await _online_info(request, guid)
        if isinstance(resolved, dict):
            info.update(resolved)
    album = info.get("album")
    if isinstance(album, dict):
        album = info.get("originalAlbum") or album.get("name") or ""
    return {
        "guid": guid,
        "id": song_id_from_online_guid(guid),
        "source": str(info.get("source") or source_from_online_guid(guid)),
        "title": str(info.get("title") or info.get("name") or ""),
        "artist": str(info.get("artist") or ""),
        "album": str(album or "").split(" 〔", 1)[0],
        "duration_s": info.get("duration_s") or 0,
        "ext": info.get("ext") or info.get("format") or "mp3",
        "file_size": info.get("file_size") or info.get("size") or 0,
        "cover_url": entity_cover_url(info),
    }


async def record_stream_play(request: Request, guid: str) -> None:
    """Record native-client playback even when it never sends event/report."""
    try:
        is_authed, user_guid, _ = await _probe_upstream_auth(
            request, get_upstream_client(request.app)
        )
        if not is_authed or not user_guid or user_guid == "shared":
            return
        snapshot = await online_history_snapshot(request, guid)
        if not snapshot.get("title"):
            return
        async with _HISTORY_LOCK:
            dailyrec.record_online_play(user_guid, guid, snapshot)
    except Exception as exc:
        logger.warning("failed to record stream play for %s: %s", guid, exc)


async def online_stream_head(request: Request, guid: str) -> Response:
    """Return a cheap media probe for native/third-party playback engines.

    ExoPlayer, mpv and several fnOS clients issue HEAD before their first range
    GET.  Do not turn that probe into a full upstream download/cache operation.
    """
    cached = find_cache_file(guid)
    if cached:
        cached = promote_cache_hit(guid, cached)
        ext = os.path.splitext(cached)[1].lstrip(".") or "mp3"
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(os.path.getsize(cached)),
                "Content-Type": media_type_for_ext(ext),
                "X-FnMusic-Ext-Source": source_from_online_guid(guid),
            },
        )

    info = await _online_info(request, guid) or _ONLINE_ENTITY_CACHE.get(guid) or {}
    ext = str(info.get("ext") or info.get("format") or "mp3").lower()
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": media_type_for_ext(ext),
        "X-FnMusic-Ext-Source": source_from_online_guid(guid),
    }
    try:
        file_size = int(info.get("file_size") or info.get("size") or 0)
    except (TypeError, ValueError):
        file_size = 0
    if file_size > 0:
        headers["Content-Length"] = str(file_size)
        return Response(status_code=200, headers=headers)
    # Plain Response would synthesize ``Content-Length: 0`` for an empty HEAD
    # body. Some media engines interpret that as a genuinely empty audio file.
    # StreamingResponse deliberately leaves the unknown size unspecified.
    return StreamingResponse(iter(()), status_code=200, headers=headers)


@app.api_route("/music/api/v1/track/stream", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/track/stream/{subpath:path}", methods=["GET", "HEAD"])
async def stream_track(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    if request.method == "HEAD":
        return await online_stream_head(request, guid)

    # Several native clients start playback but omit event/report. A real GET
    # is therefore the most reliable cross-client play signal (HEAD is ignored).
    await record_stream_play(request, guid)

    range_header = request.headers.get("range")
    cached = find_cache_file(guid)
    if cached:
        cached = promote_cache_hit(guid, cached)
        ext = os.path.splitext(cached)[1].lstrip(".") or "mp3"
        return serve_file_with_range(cached, range_header, media_type_for_ext(ext))

    src = source_from_online_guid(guid)
    cached_info = _ONLINE_ENTITY_CACHE.get(guid)
    if SOURCE_REGISTRY.preference("audioSource") == "lx" and src != "lx":
        info = cached_info or await _online_info(request, guid) or {}
        lx_url = await resolve_lx_action(request, guid, info, quality="flac")
        if isinstance(lx_url, str) and lx_url.startswith(("http://", "https://")):
            return await stream_direct_online(
                request,
                guid,
                lx_url,
                range_header,
                info,
                str(info.get("ext") or "flac"),
                allow_full_fallback=False,
            )
    if src == "lxsource":
        info = cached_info or {}
        play_url = str(info.get("play_url") or "")
        if not play_url.startswith(("http://", "https://")):
            return JSONResponse(
                content={"code": 404, "msg": "LX source URL expired; switch source again", "data": None},
                status_code=404,
            )
        return await stream_direct_online(
            request,
            guid,
            play_url,
            range_header,
            info,
            str(info.get("ext") or "flac"),
            allow_full_fallback=False,
        )

    if src == "qq":
        song_mid = song_id_from_online_guid(guid).split(":")[-1]
        play_url, resolved_ext, file_size = await resolve_qq_url(
            get_qqmusic_client(request.app), song_mid
        )
        info = await _online_info(request, guid)
        if isinstance(info, dict) and file_size:
            info["file_size"] = file_size
        if not play_url:
            play_url = await resolve_lx_action(
                request, guid, info or cached_info or {"id": song_mid, "source": "qq"}
            )
        if not isinstance(play_url, str) or not play_url.startswith(("http://", "https://")):
            return JSONResponse(
                content={"code": 404, "msg": "QQ Music account has no playable URL", "data": None},
                status_code=404,
            )
        return await stream_direct_online(
            request, guid, play_url, range_header, info, resolved_ext
        )

    if src == "netease":
        musicbox_client = get_musicbox_client(request.app)
        raw_song_id = song_id_from_online_guid(guid)
        song_id = raw_song_id.split(":")[-1]

        play_url_res, info_res = await asyncio.gather(
            resolve_netease_url(musicbox_client, song_id),
            _online_info(request, guid),
            return_exceptions=True,
        )
        play_url = None if isinstance(play_url_res, Exception) else play_url_res
        info = None if isinstance(info_res, Exception) else info_res

        if not play_url:
            play_url = await resolve_lx_action(
                request,
                guid,
                info or cached_info or {"id": song_id, "source": "netease"},
                quality="flac",
            )
        if not isinstance(play_url, str) or not play_url.startswith(("http://", "https://")):
            return JSONResponse(
                content={"code": 404, "msg": "online source unavailable", "data": None},
                status_code=404,
            )

        resolved_ext = str(info.get("ext")) if (isinstance(info, dict) and info.get("ext")) else None

        return await stream_direct_online(
            request, guid, play_url, range_header, info if isinstance(info, dict) else None, resolved_ext
        )

    if src == "lx":
        lx_client = get_lx_client(request.app)
        song_id = song_id_from_online_guid(guid)

        url_res, info_res = await asyncio.gather(
            resolve_lx_url(lx_client, song_id),
            _online_info(request, guid),
            return_exceptions=True,
        )
        url_info = None if isinstance(url_res, Exception) else url_res
        info = None if isinstance(info_res, Exception) else info_res

        play_url = str((url_info or {}).get("url") or "") if url_info else ""
        if not play_url:
            return JSONResponse(
                content={"code": 404, "msg": "online source unavailable", "data": None},
                status_code=404,
            )

        resolved_ext = (
            str(url_info.get("ext"))
            if url_info.get("ext")
            else (str(info.get("ext")) if (isinstance(info, dict) and info.get("ext")) else None)
        )

        req_headers = {}
        url_req_headers = {str(k).lower(): str(v) for k, v in (url_info.get("headers") or {}).items()}
        for hdr_key in ("user-agent", "referer"):
            hdr_val = url_req_headers.get(hdr_key, "")
            if hdr_val:
                req_headers[hdr_key] = hdr_val
        if range_header:
            req_headers["Range"] = range_header

        stream_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        try:
            stream_req = stream_client.build_request("GET", play_url, headers=req_headers)
            resp = await stream_client.send(stream_req, stream=True)
            content_type = (resp.headers.get("content-type") or "").lower()
            if resp.status_code >= 400 or "text/html" in content_type:
                await resp.aclose()
                await stream_client.aclose()
                return JSONResponse(
                    content={"code": 404, "msg": "online source unavailable", "data": None},
                    status_code=404,
                )
        except Exception as e:
            logger.warning("Failed to stream lx url %s for %s: %s", play_url, guid, e)
            await stream_client.aclose()
            return JSONResponse(
                content={"code": 404, "msg": "online source unavailable", "data": None},
                status_code=404,
            )

        return stream_tee_response(
            resp,
            guid=guid,
            range_header=range_header,
            coro_factory=None if info is not None else (lambda: _online_info(request, guid)),
            client_to_close=stream_client,
            resolved_ext=resolved_ext,
            pre_info=info if isinstance(info, dict) else None,
        )

    musicdl_client = get_musicdl_client(request.app)
    song_id = song_id_from_online_guid(guid)

    req_headers = {}
    if range_header:
        req_headers["Range"] = range_header

    req = musicdl_client.build_request(
        "GET",
        "/stream",
        params={"id": song_id, "proxy": "true"},
        headers=req_headers,
    )
    resp = await musicdl_client.send(req, stream=True)

    if resp.status_code in (404, 502) or resp.status_code >= 400:
        await resp.aclose()
        lx_url = await resolve_lx_action(
            request,
            guid,
            cached_info or {"id": song_id, "source": src},
            quality="flac",
        )
        if isinstance(lx_url, str) and lx_url.startswith(("http://", "https://")):
            return await stream_direct_online(
                request, guid, lx_url, range_header, cached_info, None
            )
        return JSONResponse(
            content={"code": 404, "msg": "online source unavailable", "data": None},
            status_code=404,
        )

    info = cached_info or await _online_info(request, guid) or {}
    if is_probable_audio_preview(info, response_media_size(resp.headers), str(resp.url)):
        await resp.aclose()
        fallback = await stream_full_length_fallback(request, guid, info, range_header)
        if fallback is not None:
            return fallback
        return JSONResponse(
            content={"code": 409, "msg": "only a short preview was available", "data": None},
            status_code=409,
        )

    return stream_tee_response(
        resp,
        guid=guid,
        range_header=range_header,
        coro_factory=lambda: cache_lyrics_from_musicdl(musicdl_client, guid),
        client_to_close=None,
        pre_info=info,
    )


@app.get("/music/api/v1/track/hls/{guid}/preset.m3u8")
@app.get("/music/api/v1/track/hls/{guid}/{filename}")
async def track_hls(request: Request, guid: str, filename: str = "preset.m3u8"):
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    info = await _online_info(request, guid)
    duration_s = 0
    if info:
        try:
            duration_s = int(float(info.get("duration_s") or 0))
        except (TypeError, ValueError):
            duration_s = 0
    if duration_s <= 0:
        duration_s = 240

    stream_url = f"/music/api/v1/track/stream?guid={quote(guid, safe='')}"
    playlist = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        f"#EXT-X-TARGETDURATION:{max(duration_s, 1)}\n"
        "#EXT-X-PLAYLIST-TYPE:VOD\n"
        "#EXT-X-MEDIA-SEQUENCE:0\n"
        f"#EXTINF:{duration_s:.3f},\n"
        f"{stream_url}\n"
        "#EXT-X-ENDLIST\n"
    )
    return Response(content=playlist, media_type="application/vnd.apple.mpegurl")


@app.api_route("/music/api/v1/track/transcode/heartbeat", methods=["GET", "POST"])
@app.api_route("/music/api/v1/track/transcode/quit", methods=["GET", "POST"])
async def track_transcode_session(request: Request):
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"guid": guid}})


@app.api_route("/music/api/v1/track/transcode", methods=["GET", "POST"])
async def track_transcode(request: Request):
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "status": "success",
            "data": {"guid": guid, "status": "ready"},
        }
    )


async def _online_info(request: Request, guid: str) -> dict | None:
    src = source_from_online_guid(guid)
    if src == "qq":
        song_mid = song_id_from_online_guid(guid).split(":")[-1]
        client = get_qqmusic_client(request.app)
        cached = _ONLINE_ENTITY_CACHE.get(guid)
        info = dict(cached) if isinstance(cached, dict) else {}
        try:
            response = await client.get("/song/detail", params={"mids": song_mid}, timeout=12.0)
            if response.status_code == 200:
                tracks = extract_qq_tracks(response.json())
                if tracks:
                    info.update(tracks[0])
        except Exception as exc:
            logger.warning("QQ Music detail failed for %s: %s", guid, exc)
        try:
            response = await client.get(
                "/song/lyric", params={"mid": song_mid, "decode": 1}, timeout=12.0
            )
            if response.status_code == 200:
                payload = response.json()
                data = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(data, dict):
                    lyric = data.get("lyric") or ""
                    if lyric:
                        info["lyric"] = str(lyric)
        except Exception as exc:
            logger.warning("QQ Music lyric failed for %s: %s", guid, exc)
        if info:
            info.setdefault("id", f"qq:{song_mid}")
            info.setdefault("source", "qq")
            info.setdefault("qq_mid", song_mid)
            _set_online_entity_cache(guid, {**info, "ts": time.time()})
            return info
        return None

    if src == "netease":
        musicbox_client = get_musicbox_client(request.app)
        raw_song_id = song_id_from_online_guid(guid)
        song_id = raw_song_id.split(":")[-1]
        try:
            r = await musicbox_client.get(f"/api/v1/song/{song_id}/info", timeout=10.0)
            if r.status_code == 200:
                res_data = r.json()
                if isinstance(res_data, dict) and res_data.get("ok") is not False:
                    data = res_data.get("data")
                    if isinstance(data, dict):
                        name = str(data.get("name") or "")
                        ar = data.get("ar") or []
                        ar_names = []
                        if isinstance(ar, list):
                            for x in ar:
                                if isinstance(x, dict) and x.get("name"):
                                    ar_names.append(str(x["name"]))
                                elif isinstance(x, str):
                                    ar_names.append(x)
                        artist = " / ".join(ar_names)
                        al = data.get("al") or {}
                        album_name = str(al.get("name") or "") if isinstance(al, dict) else ""
                        cover_url = str(al.get("picUrl") or "") if isinstance(al, dict) else ""
                        dt = data.get("dt") or 0
                        duration_s = float(dt) / 1000.0 if dt else 0.0
                        sq = data.get("sq")
                        hr = data.get("hr")
                        h = data.get("h") or {}
                        ext = "flac" if (sq or hr) else "mp3"
                        size_obj = sq or h or {}
                        file_size = int(size_obj.get("size", 0) or 0) if isinstance(size_obj, dict) else 0

                        lyric_text = ""
                        try:
                            lr = await musicbox_client.get(f"/api/v1/song/{song_id}/lyric", timeout=10.0)
                            if lr.status_code == 200:
                                l_res = lr.json()
                                if isinstance(l_res, dict) and l_res.get("ok") is not False:
                                    l_data = l_res.get("data")
                                    if isinstance(l_data, dict):
                                        lyric_text = str(l_data.get("lyric") or "").strip()
                        except Exception as l_err:
                            logger.warning("musicbox lyric fetch in _online_info failed for %s: %s", guid, l_err)

                        return {
                            "id": f"netease:{song_id}",
                            "source": "netease",
                            "title": name,
                            "artist": artist,
                            "album": album_name,
                            "cover_url": cover_url,
                            "duration_s": duration_s,
                            "ext": ext,
                            "file_size": file_size,
                            "lyric": lyric_text,
                        }
        except Exception as e:
            logger.warning("musicbox /info failed for %s: %s", guid, e)
        return None

    if src == "lx":
        lx_client = get_lx_client(request.app)
        song_id = song_id_from_online_guid(guid)
        try:
            r = await lx_client.get("/api/v1/track/info", params={"id": song_id}, timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data.get("ok") is not False:
                    inner = data.get("data")
                    if isinstance(inner, dict):
                        lyric_text = ""
                        if not inner.get("lyric"):
                            try:
                                lr = await lx_client.get(
                                    "/api/v1/track/lyric", params={"id": song_id}, timeout=10.0
                                )
                                if lr.status_code == 200:
                                    l_res = lr.json()
                                    if isinstance(l_res, dict) and l_res.get("ok") is not False:
                                        lyric_text = str((l_res.get("data") or {}).get("lyric") or "").strip()
                            except Exception as l_err:
                                logger.warning("lxmusic lyric fetch failed for %s: %s", guid, l_err)
                        else:
                            lyric_text = str(inner.get("lyric") or "").strip()
                        return {
                            "id": song_id,
                            "source": "lx",
                            "lx_source": str(inner.get("lx_source") or ""),
                            "title": str(inner.get("title") or ""),
                            "artist": str(inner.get("artist") or ""),
                            "album": str(inner.get("album") or ""),
                            "cover_url": str(inner.get("cover_url") or ""),
                            "duration_s": float(inner.get("duration_s") or 0),
                            "ext": str(inner.get("ext") or "mp3") or "mp3",
                            "file_size": int(inner.get("file_size") or 0),
                            "lyric": lyric_text,
                        }
        except Exception as e:
            logger.warning("lxmusic /info failed for %s: %s", guid, e)
        return None

    musicdl_client = get_musicdl_client(request.app)
    song_id = song_id_from_online_guid(guid)
    try:
        r = await musicdl_client.get("/info", params={"id": song_id}, timeout=10.0)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and data.get("ok") is not False:
                return data
    except Exception as e:
        logger.warning("musicdl /info failed for %s: %s", guid, e)
    return None


@app.get("/music/api/v1/lyric/list")
@app.get("/music/api/v1/lyric/list/{subpath:path}")
async def lyric_list(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    lyric_text = await resolve_online_lyric(request, guid)
    return JSONResponse(content=build_lyric_list_payload(guid, lyric_text))


@app.get("/music/api/v1/track/lyrics")
@app.get("/music/api/v1/track/lyrics/{subpath:path}")
@app.get("/music/api/v1/detail/lyrics/{subpath:path}")
async def track_lyrics(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    lyric_text = await resolve_online_lyric(request, guid)
    if lyric_text:
        res = {"code": 0, "msg": "ok", "data": {"guid": guid, "lyric": lyric_text}}
        set_by_path(res, CONF["lyric_field"], lyric_text)
        return JSONResponse(content=res)
    return empty_ok()


@app.get("/music/api/v1/track/metadata")
@app.get("/music/api/v1/track/metadata/{subpath:path}")
@app.get("/music/api/v1/track/audio-info")
async def track_metadata(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    data = await _online_info(request, guid) or stub_online_info(guid)
    cached_audio = find_cache_file(guid)
    if cached_audio and data.get("title") and data.get("artist"):
        organize_cached_media(
            guid,
            cached_audio,
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
    source_lyric = normalize_timed_lyric(data.get("lyric") or data.get("lyrics"))
    cached_lyric = read_lyric_cache(guid)
    if source_lyric:
        data = {**data, "lyric": source_lyric, "lyrics": source_lyric}
        write_lyric_cache(
            guid,
            source_lyric,
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
    elif cached_lyric:
        data = {**data, "lyric": cached_lyric, "lyrics": cached_lyric}
    return JSONResponse(content=build_metadata_payload(guid, data))


async def resolve_fallback_cover(request: Request, guid: str, info: dict | None) -> str:
    cached = _COVER_CACHE.get(guid)
    if cached and time.time() - float(cached.get("ts") or 0) < CONF["search_cache_ttl"]:
        return str(cached.get("url") or "")

    item = dict(info or _ONLINE_ENTITY_CACHE.get(guid) or {})
    title = str(item.get("title") or item.get("name") or "").strip()
    artist = str(item.get("artist") or "").strip()
    if not title:
        _COVER_CACHE[guid] = {"url": "", "ts": time.time()}
        return ""
    keyword = " ".join(part for part in (title, artist) if part)
    preferred = SOURCE_REGISTRY.preference("audioSource")
    providers = ["qqmusic", "netease"]
    if preferred in providers:
        providers.remove(preferred)
        providers.insert(0, preferred)

    url = ""
    for provider in providers:
        try:
            tracks = await search_source_tracks(request, provider, keyword, 10)
            match = _pick_lyric_match(tracks, title, artist)
            candidate = str((match or {}).get("cover_url") or "")
            if candidate.startswith(("http://", "https://")):
                url = candidate
                break
        except Exception as exc:
            logger.warning("fallback cover source %s failed for %s: %s", provider, guid, exc)
    _COVER_CACHE[guid] = {"url": url, "ts": time.time()}
    if url:
        _set_online_entity_cache(guid, {**item, "cover_url": url, "ts": time.time()})
    return url


async def search_source_tracks(
    request: Request, provider: str, keyword: str, limit: int = 10
) -> list[dict]:
    """Search one configured source using its canonical family name."""
    if provider == "qqmusic" and source_enabled("qqmusic"):
        return await fetch_qqmusic_search(get_qqmusic_client(request.app), keyword, limit) or []
    if provider == "netease" and source_enabled("netease"):
        return await fetch_musicbox_search(get_musicbox_client(request.app), keyword, limit) or []
    if provider == "musicdl" and source_enabled("musicdl"):
        payload = await fetch_musicdl_search(get_musicdl_client(request.app), keyword, limit) or {}
        return payload.get("items") if isinstance(payload.get("items"), list) else []
    if provider == "lxmusic" and source_enabled("lxmusic"):
        return await fetch_lx_search(get_lx_client(request.app), keyword, limit) or []
    return []


def placeholder_cover() -> Response:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="600" height="600" viewBox="0 0 600 600"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="#5b5ce2"/><stop offset="1" stop-color="#25273d"/></linearGradient></defs><rect width="600" height="600" rx="48" fill="url(#g)"/><circle cx="236" cy="410" r="72" fill="#fff" fill-opacity=".92"/><circle cx="426" cy="352" r="72" fill="#fff" fill-opacity=".92"/><path d="M294 164v246M484 108v244M294 164l190-56v82l-190 56" fill="none" stroke="#fff" stroke-width="38" stroke-linejoin="round"/></svg>"""
    return Response(content=svg, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=3600"})


def normalize_cover_guid(raw: str) -> str:
    """Normalize query/path forms used by Web, Android, iOS and car clients."""
    value = str(raw or "").strip().lstrip("/")
    if not value.startswith("online:"):
        return value
    value = value.split("/", 1)[0]
    value = re.sub(r"\.(?:jpe?g|png|webp|avif)$", "", value, flags=re.IGNORECASE)
    return value


async def proxy_remote_cover(request: Request, url: str) -> Response | None:
    """Relay source artwork so native clients never depend on CDN redirects.

    Some iOS image stacks reject an HTTPS NAS response that redirects to an
    HTTP artwork URL. Relaying also keeps the authenticated NAS URL stable for
    lock-screen artwork caches.
    """
    if not url.startswith(("http://", "https://")):
        return None
    headers = {
        "User-Agent": "Mozilla/5.0 fnmusic-ext/1.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    try:
        client = get_cover_client(request.app)
        if request.method == "HEAD":
            upstream = await client.head(url, headers=headers)
            if upstream.status_code >= 400:
                return None
            content_type = upstream.headers.get("content-type") or "image/jpeg"
            response_headers = {
                "Cache-Control": "public, max-age=86400",
                "Content-Type": content_type,
            }
            length = upstream.headers.get("content-length")
            if length and length.isdigit():
                response_headers["Content-Length"] = length
            return Response(status_code=200, headers=response_headers)

        upstream = await client.get(url, headers=headers)
        if upstream.status_code >= 400 or not upstream.content:
            return None
        if len(upstream.content) > 12 * 1024 * 1024:
            logger.warning("cover is unexpectedly large (%s bytes): %s", len(upstream.content), url)
            return None
        content_type = upstream.headers.get("content-type") or "image/jpeg"
        if not content_type.lower().startswith("image/"):
            content_type = "image/jpeg"
        return Response(
            content=upstream.content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception as exc:
        logger.warning("failed to proxy online cover %s: %s", url, exc)
        return None


@app.api_route("/music/api/v1/static/cover", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/static/cover/{subpath:path}", methods=["GET", "HEAD"])
async def static_cover(request: Request, subpath: str = ""):
    raw_guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not raw_guid and subpath.startswith("online:"):
        raw_guid = subpath
    guid = normalize_cover_guid(raw_guid)
    if dailyrec.is_daily_playlist_guid(guid):
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed and auth_resp is not None:
            return auth_resp
        cached = dailyrec.load_daily_cache(user_guid, dailyrec.today_key())
        tracks = (cached or {}).get("tracks") or []
        if tracks:
            first_guid = str(tracks[0].get("guid") or "")
            if is_online_guid(first_guid):
                guid = first_guid
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    parsed_entity = parse_online_entity_guid(guid)
    if parsed_entity and parsed_entity[0] in {
        "artist", "artist-name", "album", "album-name", "playlist"
    }:
        cached_entity = _ONLINE_ENTITY_CACHE.get(guid) or {}
        cover = entity_cover_url(cached_entity)
        bundle = None
        if not cover:
            bundle = await load_online_entity_bundle(request, guid)
            cover = entity_cover_url(bundle or {})
        if cover:
            proxied = await proxy_remote_cover(request, cover)
            if proxied is not None:
                return proxied
        entity_cover_id = str((bundle or cached_entity).get("coverId") or "")
        if is_online_guid(entity_cover_id) and entity_cover_id != guid:
            guid = entity_cover_id

    data = await _online_info(request, guid)
    cover = (data or {}).get("cover_url") or ""
    if not cover:
        cover = await resolve_fallback_cover(request, guid, data)
    if cover:
        proxied = await proxy_remote_cover(request, cover)
        if proxied is not None:
            return proxied
    return placeholder_cover()


# === online favorites ===

_FAV_LOCK = asyncio.Lock()


def sanitize_user_guid(guid: str | None) -> str:
    """过滤文件名合法字符 [A-Za-z0-9-_]，非法字符替换为 _；为空则返回 'shared'。"""
    raw = str(guid or "").strip()
    safe = re.sub(r"[^A-Za-z0-9\-_]", "_", raw)
    return safe or "shared"


def user_fav_path(user_guid: str) -> str:
    fav_dir = CONF.get("fav_dir") or os.path.join(_HOME, "online_favorites")
    safe_name = sanitize_user_guid(user_guid)
    return os.path.join(fav_dir, f"{safe_name}.json")


def load_online_favorites(user_guid: str) -> list[dict]:
    path = user_fav_path(user_guid)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("items"), list):
                return data["items"]
            if isinstance(data, list):
                return data
    except Exception as e:
        logger.warning("Failed to load online favorites for %s from %s: %s", user_guid, path, e)
    return []


def save_online_favorites(user_guid: str, items: list[dict]) -> bool:
    path = user_fav_path(user_guid)
    parent = os.path.dirname(path) or "."
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part_path, "w", encoding="utf-8") as f:
            json.dump({"items": items}, f, ensure_ascii=False, indent=2)
        os.replace(part_path, path)
        return True
    except Exception as e:
        logger.warning("Failed to save online favorites for %s to %s: %s", user_guid, path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass
        return False


def build_favorite_track_obj(guid: str, info: dict | None = None, created_at: int | None = None) -> dict:
    raw_info = dict(info or {})
    raw_info.setdefault("id", song_id_from_online_guid(guid))
    raw_info.setdefault("source", source_from_online_guid(guid))
    vo = build_online_track(raw_info)

    now = int(time.time())
    ts = created_at or now

    artist_name = vo.get("artist") or ""
    artists_list = [
        {
            "guid": f"online:netease:artist:name:{artist_name}",
            "name": artist_name,
            "coverId": guid,
            "createdAt": ts,
            "updatedAt": ts,
        }
    ] if artist_name else []

    album_name = vo.get("albumName") or (vo.get("album", {}).get("name") if isinstance(vo.get("album"), dict) else "") or ""
    album_entity_name = str(raw_info.get("album") or vo.get("originalAlbum") or album_name).split(" 〔", 1)[0]
    album_obj = {
        "guid": f"online:netease:album:name:{album_entity_name}",
        "name": album_name,
        "artists": artists_list,
        "coverId": guid,
        "releaseDate": 0,
        "barcode": "",
        "createdAt": ts,
        "updatedAt": ts,
    }

    audio_spec = vo.get("audioSpec") or {}

    return {
        "guid": guid,
        "title": vo.get("title") or "",
        "duration": vo.get("duration") or 0,
        "isFavorite": True,
        "isCue": False,
        "genres": [],
        "artists": artists_list,
        "album": album_obj,
        "audioSpec": audio_spec,
        "accessStatus": 0,
        "coverId": guid,
        "year": 0,
        "discNo": 1,
        "trackNo": 1,
        "isrc": "",
        "createdAt": ts,
        "updatedAt": ts,
    }


async def _probe_upstream_auth(request: Request, client: httpx.AsyncClient) -> tuple[bool, str, Response | None]:
    """向上游探测用户是否已登录。复用当前请求 headers。
    返回 (is_authed, user_guid, error_response)。
    """
    headers = copy_incoming_headers(request)
    try:
        probe_req = client.build_request("GET", "/music/api/v1/user/me", headers=headers)
        probe_resp = await client.send(probe_req)
        resp_headers = filter_headers(probe_resp.headers, exclude_keys={"content-length", "content-encoding"})

        if probe_resp.status_code == 401:
            return False, "", Response(
                content=probe_resp.content,
                status_code=401,
                headers=resp_headers,
                media_type=probe_resp.headers.get("content-type"),
            )

        if probe_resp.status_code == 200:
            try:
                probe_json = probe_resp.json()
                if isinstance(probe_json, dict) and probe_json.get("code") == 99999:
                    return False, "", JSONResponse(
                        content=probe_json,
                        status_code=200,
                        headers=resp_headers,
                    )
                if isinstance(probe_json, dict) and probe_json.get("code") == 0:
                    data = probe_json.get("data")
                    if isinstance(data, dict) and data.get("guid"):
                        return True, str(data["guid"]), None
                    logger.warning("user/me response missing data.guid, falling back to 'shared': %s", probe_json)
                    return True, "shared", None
            except Exception as e:
                logger.warning("Failed to parse user/me json response: %s", e)
                return True, "shared", None
            return True, "shared", None

        # 其他非 200/401 状态码，上游异常
        return True, "shared", None
    except Exception as e:
        logger.warning("Upstream auth probe failed: %s", e)
        # 探测异常时保守放行
        return True, "shared", None


@app.post("/music/api/v1/favorite-track/create")
async def favorite_track_create(request: Request):
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    if isinstance(body, dict):
        guid = str(body.get("trackGUID") or body.get("guid") or "").strip()

    if not is_online_guid(guid):
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    now = int(time.time())
    info = await _online_info(request, guid)
    if not info:
        cached_lyric = read_lyric_cache(guid)
        title = ""
        artist = ""
        cached_media = find_cache_file(guid)
        if cached_media:
            base = os.path.splitext(os.path.basename(cached_media))[0]
            if " - " in base:
                artist, title = base.split(" - ", 1)
            else:
                title = base
        info = {
            "id": song_id_from_online_guid(guid),
            "source": source_from_online_guid(guid),
            "title": title,
            "artist": artist,
            "lyric": cached_lyric,
        }

    track_obj = build_favorite_track_obj(guid, info, created_at=now)

    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            # 查重
            idx = next((i for i, it in enumerate(items) if it.get("guid") == guid), None)
            if idx is not None:
                # 幂等更新
                items[idx]["track"] = track_obj
            else:
                items.append({
                    "guid": guid,
                    "createdAt": now,
                    "track": track_obj,
                })
            save_online_favorites(user_guid, items)
        except Exception as e:
            logger.warning("Error updating online favorites for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/favorite-track/delete")
async def favorite_track_delete(request: Request):
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    if isinstance(body, dict):
        guid = str(body.get("trackGUID") or body.get("guid") or "").strip()

    if not is_online_guid(guid):
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            items = [it for it in items if it.get("guid") != guid]
            save_online_favorites(user_guid, items)
        except Exception as e:
            logger.warning("Error deleting from online favorites for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.get("/music/api/v1/favorite-track/list")
async def favorite_track_list(request: Request):
    upstream_client = get_upstream_client(request.app)
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    # 探测当前用户身份
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    # 成功获取官方列表，合并本地在线收藏
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        upstream_json["data"] = data

    official_list = data.get("list")
    if not isinstance(official_list, list):
        official_list = []
        data["list"] = official_list

    # 飞牛音乐前端收藏列表依赖 isFavorite=True 状态判断，遍历补齐官方列表中可能缺失的字段
    for item in official_list:
        if isinstance(item, dict):
            item["isFavorite"] = True

    official_total = data.get("total")
    if not isinstance(official_total, int):
        official_total = len(official_list)

    async with _FAV_LOCK:
        try:
            fav_items = load_online_favorites(user_guid)
        except Exception as e:
            logger.warning("Error reading online favorites for list for user %s: %s", user_guid, e)
            fav_items = []

    # 按 createdAt 倒序
    fav_items_sorted = sorted(fav_items, key=lambda x: x.get("createdAt", 0), reverse=True)
    online_tracks = []
    for it in fav_items_sorted:
        t = it.get("track")
        if isinstance(t, dict):
            # 确保关键属性为最新或格式完整
            t["isFavorite"] = True
            online_tracks.append(repair_track_entity_links(t))
        else:
            g = it.get("guid") or ""
            if g:
                online_tracks.append(build_favorite_track_obj(g, created_at=it.get("createdAt")))

    data["list"] = official_list + online_tracks
    data["total"] = official_total + len(online_tracks)

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


# === daily recommend + play history ===

_HISTORY_LOCK = asyncio.Lock()
_DAILY_TASKS: dict[str, asyncio.Task] = {}


def _prune_stale_daily_tasks(day: str) -> None:
    suffix = f":{day}"
    stale = [k for k in list(_DAILY_TASKS) if not str(k).endswith(suffix)]
    for k in stale:
        old = _DAILY_TASKS.pop(k, None)
        if old is not None and not old.done():
            old.cancel()


async def _ensure_daily_task(request: Request, user_guid: str) -> asyncio.Task:
    day = dailyrec.today_key()
    _prune_stale_daily_tasks(day)
    key = f"{user_guid}:{day}"
    task = _DAILY_TASKS.get(key)
    if task is not None and not task.done():
        return task
    if task is not None and task.done():
        try:
            if task.exception() is None:
                result = task.result()
                if isinstance(result, dict) and len(result.get("tracks") or []) >= dailyrec.PLAYLIST_SIZE:
                    return task
        except (asyncio.CancelledError, Exception):
            pass
    async with _FAV_LOCK:
        favs = load_online_favorites(user_guid)
    task = asyncio.create_task(
        dailyrec.get_or_build_daily(
            user_guid=user_guid,
            musicdl_client=get_musicdl_client(request.app) if source_enabled("musicdl") else None,
            musicbox_client=get_musicbox_client(request.app) if source_enabled("netease") else None,
            llm_http=get_llm_client(request.app) if dailyrec.llm_enabled() else None,
            build_track=build_online_track,
            netease_enabled=source_enabled("netease"),
            favorite_items=favs,
            lx_client=get_lx_client(request.app) if source_enabled("lxmusic") else None,
            lx_enabled=source_enabled("lxmusic"),
        )
    )
    _DAILY_TASKS[key] = task
    return task


async def _peek_daily_bundle(request: Request, user_guid: str) -> dict:
    """歌单列表用：有缓存立刻返回；否则后台生成，最多等 2s，超时仍返回占位歌单。"""
    day = dailyrec.today_key()
    dailyrec.purge_stale_daily_cache(user_guid, day)
    cached = dailyrec.load_daily_cache(user_guid, day)
    if cached and cached.get("tracks"):
        return cached
    task = await _ensure_daily_task(request, user_guid)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
    except asyncio.TimeoutError:
        cached = dailyrec.load_daily_cache(user_guid, day)
        if cached and cached.get("tracks"):
            return cached
        return dailyrec.empty_daily_bundle(user_guid)
    except Exception as e:
        logger.warning("daily recommend peek failed: %s", e)
        return dailyrec.empty_daily_bundle(user_guid)


async def _load_daily_bundle(request: Request, user_guid: str) -> dict:
    day = dailyrec.today_key()
    dailyrec.purge_stale_daily_cache(user_guid, day)
    cached = dailyrec.load_daily_cache(user_guid, day)
    if cached and cached.get("tracks"):
        return cached

    task = await _ensure_daily_task(request, user_guid)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=20.0)
    except asyncio.TimeoutError:
        cached = dailyrec.load_daily_cache(user_guid, day)
        if cached and cached.get("tracks"):
            return cached
        return dailyrec.empty_daily_bundle(user_guid)


def _online_entity_page(items: list[dict], request: Request) -> dict:
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size == -1:
        page_items = items
    else:
        size = max(size, 1)
        start = (page - 1) * size
        page_items = items[start : start + size]
    return {"list": page_items, "total": len(items), "sort": request.query_params.get("sort") or ""}


def _public_online_entity(bundle: dict, kind: str) -> dict:
    common = {
        "guid": bundle.get("guid"),
        "id": bundle.get("guid"),
        "name": bundle.get("name") or "在线内容",
        "coverId": bundle.get("coverId") or "",
        "trackCount": int(bundle.get("trackCount") or 0),
        "isOnline": True,
    }
    if kind == "artist":
        common["albumCount"] = int(bundle.get("albumCount") or 0)
        common["alias"] = bundle.get("alias") or ""
    elif kind == "album":
        common["artists"] = list(bundle.get("artists") or [])
        common["releaseYear"] = bundle.get("releaseYear")
    elif kind == "playlist":
        common["createdAt"] = int(bundle.get("createdAt") or time.time())
        common["updatedAt"] = int(bundle.get("updatedAt") or time.time())
        common["creatorName"] = bundle.get("creatorName") or ""
    return common


async def _online_entity_detail_response(request: Request, guid: str, kind: str) -> Response:
    parsed = parse_online_entity_guid(guid)
    if not parsed or parsed[0] not in {kind, f"{kind}-name"}:
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    is_authed, _, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await load_online_entity_bundle(request, guid)
    if not bundle:
        return JSONResponse(content={"code": 404, "msg": "online content unavailable", "data": None}, status_code=404)
    return JSONResponse(content={"code": 0, "msg": "ok", "data": _public_online_entity(bundle, kind)})


@app.get("/music/api/v1/artist/detail")
async def online_artist_detail(request: Request):
    guid = str(request.query_params.get("guid") or "").strip()
    return await _online_entity_detail_response(request, guid, "artist")


@app.get("/music/api/v1/album/detail")
async def online_album_detail(request: Request):
    guid = str(request.query_params.get("guid") or "").strip()
    return await _online_entity_detail_response(request, guid, "album")


async def _online_entity_tracks_response(
    request: Request, guid: str, accepted_kinds: set[str]
) -> Response:
    parsed = parse_online_entity_guid(guid)
    if not parsed or parsed[0] not in accepted_kinds:
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    is_authed, _, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await load_online_entity_bundle(request, guid)
    if not bundle:
        return JSONResponse(content={"code": 404, "msg": "online content unavailable", "data": None}, status_code=404)
    return JSONResponse(content={"code": 0, "msg": "ok", "data": _online_entity_page(bundle.get("tracks") or [], request)})


@app.get("/music/api/v1/track/artist-detail/list")
async def online_artist_track_list(request: Request):
    guid = str(
        request.query_params.get("artistGUID")
        or request.query_params.get("artistGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    return await _online_entity_tracks_response(request, guid, {"artist", "artist-name"})


@app.get("/music/api/v1/track/album-detail/list")
async def online_album_track_list(request: Request):
    guid = str(
        request.query_params.get("albumGUID")
        or request.query_params.get("albumGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    return await _online_entity_tracks_response(request, guid, {"album", "album-name"})


@app.get("/music/api/v1/album/artist-detail/list")
async def online_artist_album_list(request: Request):
    guid = str(
        request.query_params.get("artistGUID")
        or request.query_params.get("artistGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    parsed = parse_online_entity_guid(guid)
    if not parsed or parsed[0] not in {"artist", "artist-name"}:
        return await forward_to_upstream(request, get_upstream_client(request.app))
    upstream_client = get_upstream_client(request.app)
    is_authed, _, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await load_online_entity_bundle(request, guid)
    if not bundle:
        return JSONResponse(content={"code": 404, "msg": "online content unavailable", "data": None}, status_code=404)
    return JSONResponse(content={"code": 0, "msg": "ok", "data": _online_entity_page(bundle.get("albums") or [], request)})


def _playlist_public_fields(record: dict) -> dict:
    return {
        "guid": record.get("guid"),
        "name": record.get("name") or "每日推荐",
        "coverId": record.get("coverId") or record.get("guid"),
        "createdAt": int(record.get("createdAt") or time.time()),
        "updatedAt": int(record.get("updatedAt") or time.time()),
        "trackCount": int(record.get("trackCount") or 0),
        "isDaily": True,
    }


@app.get("/music/api/v1/playlist/list")
@app.get("/music/api/v1/playlist/list/{subpath:path}")
async def playlist_list(request: Request):
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed:
        return auth_resp or JSONResponse(content=envelope, headers=headers)

    try:
        bundle = await _peek_daily_bundle(request, user_guid)
    except Exception as e:
        logger.warning("daily recommend list inject failed: %s", e)
        return JSONResponse(content=envelope, headers=headers)

    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        envelope["data"] = data
    official = data.get("list")
    if not isinstance(official, list):
        official = []
        data["list"] = official
    rec = _playlist_public_fields(bundle.get("playlist") or {})
    rec["trackCount"] = len(bundle.get("tracks") or [])
    official = [
        it for it in official
        if not (isinstance(it, dict) and dailyrec.is_daily_playlist_guid(str(it.get("guid") or "")))
    ]
    data["list"] = [rec] + official
    total = data.get("total")
    data["total"] = (total if isinstance(total, int) else len(official)) + 1
    return JSONResponse(content=envelope, headers=headers)


@app.get("/music/api/v1/playlist/detail")
async def playlist_detail(request: Request):
    guid = str(request.query_params.get("guid") or "").strip()
    parsed = parse_online_entity_guid(guid)
    if parsed and parsed[0] == "playlist":
        return await _online_entity_detail_response(request, guid, "playlist")
    if not dailyrec.is_daily_playlist_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await _load_daily_bundle(request, user_guid)
    rec = _playlist_public_fields(bundle.get("playlist") or {})
    rec["trackCount"] = len(bundle.get("tracks") or [])
    return JSONResponse(content={"code": 0, "msg": "ok", "data": rec})


@app.get("/music/api/v1/playlist/batch-detail")
async def playlist_batch_detail(request: Request):
    raw = request.query_params.get("guids") or request.query_params.get("guid") or ""
    guids = [g.strip() for g in raw.split(",") if g.strip()]
    daily_ids = [g for g in guids if dailyrec.is_daily_playlist_guid(g)]
    if not daily_ids:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    rest = [g for g in guids if not dailyrec.is_daily_playlist_guid(g)]
    official_list: list = []
    if rest:
        headers = copy_incoming_headers(request)
        req = upstream_client.build_request(
            "GET",
            f"/music/api/v1/playlist/batch-detail?guids={quote(','.join(rest), safe=',')}",
            headers=headers,
        )
        resp = await upstream_client.send(req)
        if resp.status_code == 200:
            try:
                payload = resp.json()
                if isinstance(payload, dict) and payload.get("code") == 0:
                    data = payload.get("data") or {}
                    if isinstance(data, dict) and isinstance(data.get("list"), list):
                        official_list = data["list"]
                    elif isinstance(data, list):
                        official_list = data
            except Exception:
                official_list = []

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await _load_daily_bundle(request, user_guid)
    rec = _playlist_public_fields(bundle.get("playlist") or {})
    rec["trackCount"] = len(bundle.get("tracks") or [])
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"list": [rec] + official_list}})


@app.get("/music/api/v1/track/playlist-detail/list")
async def playlist_track_list(request: Request):
    guid = str(
        request.query_params.get("playlistGUID")
        or request.query_params.get("playlistGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    parsed = parse_online_entity_guid(guid)
    if parsed and parsed[0] == "playlist":
        return await _online_entity_tracks_response(request, guid, {"playlist"})
    if not dailyrec.is_daily_playlist_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await _load_daily_bundle(request, user_guid)
    tracks = [
        repair_track_entity_links(track)
        for track in dailyrec.stamp_playlist_tracks(list(bundle.get("tracks") or []))
        if isinstance(track, dict)
    ]
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size < 1:
        size = 50
    start = (page - 1) * size
    page_tracks = tracks[start:start + size] if size != -1 else tracks
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "data": {"list": page_tracks, "total": len(tracks), "sort": request.query_params.get("sort") or ""},
        }
    )


@app.post("/music/api/v1/event/report")
async def event_report(request: Request):
    upstream_client = get_upstream_client(request.app)
    raw = await request.body()
    try:
        body = json.loads(raw.decode("utf-8") or "{}") if raw else {}
    except Exception:
        body = {}
    events = body.get("events") if isinstance(body, dict) else None
    if not isinstance(events, list) and isinstance(body, dict) and (
        body.get("eventType") or body.get("type")
    ):
        events = [body]
    online_plays: list[str] = []
    other_events: list = []
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            et = str(ev.get("eventType") or ev.get("type") or "").casefold()
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
            if not payload and isinstance(ev.get("data"), dict):
                payload = ev["data"]
            guid = str(
                payload.get("trackGUID")
                or payload.get("trackGuid")
                or payload.get("trackId")
                or payload.get("guid")
                or ev.get("trackGUID")
                or ev.get("trackGuid")
                or ev.get("guid")
                or ""
            )
            if et in {"track_play", "trackplay", "track_played", "play"} and is_online_guid(guid):
                online_plays.append(guid)
            else:
                other_events.append(ev)
    else:
        return await forward_to_upstream(request, upstream_client)

    if online_plays:
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if is_authed:
            async with _HISTORY_LOCK:
                for guid in online_plays:
                    snapshot = await online_history_snapshot(request, guid)
                    if snapshot.get("title"):
                        dailyrec.record_online_play(user_guid, guid, snapshot)
        elif auth_resp is not None and not other_events:
            return auth_resp

    if other_events:
        headers = copy_incoming_headers(request)
        fwd = dict(body)
        fwd["events"] = other_events
        req = upstream_client.build_request(
            "POST",
            "/music/api/v1/event/report",
            headers=headers,
            content=json.dumps(fwd).encode("utf-8"),
        )
        resp = await upstream_client.send(req)
        resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    return JSONResponse(content={"code": 0, "msg": "ok", "data": None})


@app.get("/music/api/v1/play-history/list")
@app.get("/music/api/v1/play-history/list/{subpath:path}")
async def play_history_list(request: Request, subpath: str = ""):
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed:
        return auth_resp or JSONResponse(content=envelope, headers=headers)

    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        envelope["data"] = data
    official = data.get("list")
    if not isinstance(official, list):
        official = []
        data["list"] = official

    async with _HISTORY_LOCK:
        online_items = dailyrec.load_online_play_history(user_guid)
    online_tracks = []
    for it in reversed(online_items):
        guid = str(it.get("guid") or "")
        if not guid:
            continue
        track = it.get("track") if isinstance(it.get("track"), dict) else {}
        obj = build_favorite_track_obj(guid, track, created_at=int(it.get("playedAt") or time.time()))
        obj["isFavorite"] = False
        online_tracks.append(obj)

    seen = {str(x.get("guid")) for x in official if isinstance(x, dict)}
    merged_online = [t for t in online_tracks if t.get("guid") not in seen]
    data["list"] = merged_online + official
    official_total = data.get("total")
    if not isinstance(official_total, int):
        official_total = len(official)
    data["total"] = official_total + len(merged_online)
    return JSONResponse(content=envelope, headers=headers)


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def catch_all(request: Request, full_path: str):
    return await forward_to_upstream(request, get_upstream_client(request.app))
