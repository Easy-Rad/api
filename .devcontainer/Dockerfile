#FROM python:3-slim
FROM mcr.microsoft.com/devcontainers/python:1-3-bookworm

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # build-essential \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the CA certificate from the context to the build container
COPY ../cdhb.crt /usr/local/share/ca-certificates/
# Update the CA certificates in the container
RUN update-ca-certificates

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy and install requirements
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CURL_CA_BUNDLE=/usr/local/share/ca-certificates/cdhb.crt