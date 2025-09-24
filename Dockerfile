FROM python:3-alpine AS service_api
COPY corporate.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
RUN apk update && apk add curl
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/corporate.crt

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip install -r requirements.txt

WORKDIR /app
COPY app/ ./app/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --start-period=10s --timeout=2s \
    CMD curl -f http://localhost:5000/health || exit 1

EXPOSE 5000

CMD ["hypercorn", "--bind", "0.0.0.0:5000", "app.app:app"]