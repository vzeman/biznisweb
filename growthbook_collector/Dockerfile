FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GROWTHBOOK_BIND_HOST=0.0.0.0 \
    GROWTHBOOK_COLLECTOR_PORT=8080

WORKDIR /app

COPY growthbook_collector/requirements.txt /app/growthbook_collector/requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r /app/growthbook_collector/requirements.txt && \
    addgroup --system --gid 10001 collector && \
    adduser --system --uid 10001 --ingroup collector --home /nonexistent --no-create-home collector

COPY --chown=10001:10001 growthbook_collector /app/growthbook_collector
RUN chmod 0555 /app/growthbook_collector/host_gate.sh

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["curl", "-fsS", "http://127.0.0.1:8080/health"]

CMD ["python", "-m", "growthbook_collector.server"]
