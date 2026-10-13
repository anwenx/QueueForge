FROM node:22-alpine
ENV NODE_ENV=production QF_HOST=0.0.0.0 QF_PORT=8080 QF_DB_PATH=/data/queueforge.db
WORKDIR /app
COPY package.json ./
COPY src ./src
COPY bin ./bin
COPY public ./public
RUN mkdir -p /data && chown -R node:node /data /app
USER node
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.QF_PORT||8080)+'/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
STOPSIGNAL SIGTERM
CMD ["node", "bin/queueforge.js", "serve", "--with-worker"]
