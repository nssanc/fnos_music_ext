from proxy.online_sources import extract_qq_tracks, lx_music_info, lx_source_key, qq_play_url


def test_extract_qq_tracks_handles_nested_mobile_shape():
    payload = {
        "code": 0,
        "data": {
            "body": {
                "song": {
                    "list": [{
                        "mid": "0039MnYb0qxYhV",
                        "name": "晴天",
                        "interval": 269,
                        "singer": [{"name": "周杰伦"}],
                        "album": {"name": "叶惠美", "mid": "000MkMni19ClKG"},
                        "file": {"size_flac": 123},
                    }]
                }
            }
        },
    }
    tracks = extract_qq_tracks(payload)
    assert tracks[0]["id"] == "qq:0039MnYb0qxYhV"
    assert tracks[0]["artist"] == "周杰伦"
    assert tracks[0]["album"] == "叶惠美"
    assert tracks[0]["ext"] == "flac"
    assert tracks[0]["cover_url"].endswith("000MkMni19ClKG.jpg")


def test_qq_play_url_and_lx_aliases():
    assert qq_play_url({"data": {"mid1": {"url": "https://cdn.test/a.flac", "size": 9}}}, "mid1") == ("https://cdn.test/a.flac", 9)
    assert lx_source_key("qq") == "tx"
    info = lx_music_info({"source": "qq", "id": "qq:mid1", "qq_mid": "mid1", "title": "歌", "artist": "人"}, "online:qq:mid1")
    assert info["songmid"] == "mid1"
    assert info["source"] == "tx"
