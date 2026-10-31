# No credentials or model executable are needed for registered script workers.
ARG BASE=sdo-responder:v0.1.0
FROM ${BASE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends bubblewrap \
    && rm -rf /var/lib/apt/lists/* \
    && (npm uninstall --global @openai/codex @anthropic-ai/claude-code || true) \
    && rm -f /usr/local/bin/codex /usr/local/bin/claude
COPY benchmarks /opt/sdo/benchmarks
ENV PYTHONPATH=/opt/sdo
