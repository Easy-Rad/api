FROM python:3-alpine AS base
COPY corporate.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/corporate.crt

FROM base AS builder
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

FROM builder AS service_api
RUN apk update && apk add curl
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN adduser -D app
WORKDIR /app
COPY --chown=app:app app/ ./app/
USER app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
HEALTHCHECK --interval=30s --start-period=10s --timeout=2s \
    CMD curl -f http://localhost:5000/health || exit 1
EXPOSE 5000
CMD ["hypercorn", "--bind", "0.0.0.0:5000", "app.app:app"]