FROM python:3.12-slim

WORKDIR /app

# Install system dependencies needed for building Python packages and Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    python -m playwright install chromium 2>/dev/null || true

# Copy crawler code
COPY . .

# Run API server
CMD ["gunicorn", "--workers", "1", "--timeout", "300", "api_server:app"]
