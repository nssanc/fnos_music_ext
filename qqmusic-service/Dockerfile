FROM node:22-alpine AS build

ARG QQMUSIC_REPO=https://github.com/Suxiaoqinx/QQMusicapi.git
ARG QQMUSIC_REF=b6d748bb63fc65b6a98a383d8974fe3f3fd75d5b
WORKDIR /src
# install.sh exports the pinned commit into this build context on the host.
# This avoids requiring Docker's isolated build network to reach GitHub.
COPY vendor/ /src/
RUN test "$(cat .fnmusic-ref)" = "$QQMUSIC_REF" \
 && rm .fnmusic-ref \
 && npm pkg set dependencies.undici=6.28.1 overrides.qs=6.16.0 \
 && npm install --omit=dev --ignore-scripts

FROM node:22-alpine
RUN addgroup -S fnmusic && adduser -S -G fnmusic fnmusic
WORKDIR /app
COPY --from=build --chown=fnmusic:fnmusic /src /app
RUN mkdir -p /data && chown -R fnmusic:fnmusic /data
USER fnmusic
ENV PORT=8000 PLATFORM=android DEVICE_PATH=/data/device.json CREDENTIAL_PATH=/data/credential.json
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:8000/health').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
CMD ["npm", "start"]
