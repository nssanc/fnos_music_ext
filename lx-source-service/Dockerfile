FROM node:22-alpine

RUN addgroup -S fnmusic && adduser -S -G fnmusic fnmusic
WORKDIR /app
COPY package.json server.js ./
RUN mkdir -p /data/sources && chown -R fnmusic:fnmusic /data

USER fnmusic
ENV PORT=8000 LX_SOURCE_DATA_DIR=/data
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:8000/healthz').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
CMD ["node", "server.js"]
