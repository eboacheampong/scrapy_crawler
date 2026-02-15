FROM python:3.13-slim

WORKDIR /app/scrapy_crawler

# Install dependencies
COPY scrapy_crawler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy crawler code
COPY scrapy_crawler/ .

# Run scheduled crawler
CMD ["python", "scheduled_runner.py"]
