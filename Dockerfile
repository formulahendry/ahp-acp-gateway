# syntax=docker/dockerfile:1

FROM node:24-bookworm-slim AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ahp-acp-gateway.default.json ahp-acp-gateway.example.json ./
COPY src ./src
RUN npm run build && npm prune --omit=dev

FROM node:24-bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    ripgrep \
  && rm -rf /var/lib/apt/lists/* \
  && npm install --global @opencode/cli@latest \
  && npm cache clean --force

WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/ahp-acp-gateway.default.json ./ahp-acp-gateway.default.json
COPY --from=build /app/ahp-acp-gateway.example.json ./ahp-acp-gateway.example.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY docker/entrypoint.sh /usr/local/bin/ahp-acp-gateway-entrypoint

# Node's ESM loader only strips paths when standard input comes from stdin;
# adjust entrypoint to pass --input-type=module to node.
RUN chmod +x /usr/local/bin/ahp-acp-gateway-entrypoint \
  && mkdir -p /data /workspace/demo \
  && printf '# Demo Workspace\n\nWelcome to the AHP-ACP Gateway sandbox workspace.\n' > /workspace/demo/README.md \
  && chown -R node:node /app /data /workspace

USER node
ENV HOME=/home/node \
    NODE_ENV=production \
    PORT=8765 \
    AHP_HOST=0.0.0.0 \
    AHP_PATH=/ahp \
    AHP_TLS_TERMINATED=true \
    AHP_DB_PATH=/data/ahp-acp-gateway.db \
    AHP_DEFAULT_CWD=/workspace/demo \
    AHP_ENABLED_PROVIDERS=opencode

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 8765) + '/healthz').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

ENTRYPOINT ["/usr/local/bin/ahp-acp-gateway-entrypoint"]
