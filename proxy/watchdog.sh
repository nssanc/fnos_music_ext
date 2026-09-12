#!/usr/bin/env bash
set -euo pipefail

# fnOS updates replace both the Music index.html and its Unix socket while the
# proxy process remains alive on an unlinked inode. Repair those two reversible
# integration points without changing any official application assets.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_SOCK="/var/run/trim_music.socket"

if [ -f "${BASE_DIR}/scripts/ui_hook.py" ]; then
    python3 "${BASE_DIR}/scripts/ui_hook.py" install \
        --state "${BASE_DIR}/backup/ui-hook-files.json" >/dev/null 2>&1 || true
fi

health="$(curl -s --max-time 3 --unix-socket "${TARGET_SOCK}" http://localhost/_ext/healthz 2>/dev/null || true)"
if echo "${health}" | grep -q '"upstream"'; then
    exit 0
fi

echo "[watchdog] 检测到 fnOS 更新或 Music socket 被替换，重新接管..."
systemctl restart fnmusic-ext.service

for _ in $(seq 1 65); do
    health="$(curl -s --max-time 3 --unix-socket "${TARGET_SOCK}" http://localhost/_ext/healthz 2>/dev/null || true)"
    if echo "${health}" | grep -q '"upstream"'; then
        echo "[watchdog] 页面入口与 Music socket 已恢复"
        exit 0
    fi
    sleep 1
done

echo "[watchdog] fnmusic-ext 未能在等待时间内恢复" >&2
exit 1
