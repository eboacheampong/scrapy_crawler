FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy crawler code
COPY . .

# Run API server
CMD ["gunicorn", "--workers", "1", "--timeout", "300", "api_server:app"]
