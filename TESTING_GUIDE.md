# Testing Your Rendered Scraper - Quick Guide

## Quick Test Commands

### 1. Test Health (Should return 200)
```powershell
$url = "https://YOUR-SERVICE.onrender.com/health"
$response = Invoke-WebRequest -Uri $url
$response.Content | ConvertFrom-Json
```

### 2. Test Full Scrape (Returns articles)
```powershell
$url = "https://YOUR-SERVICE.onrender.com/api/scrape"
$response = Invoke-WebRequest -Uri $url -Method POST `
  -ContentType "application/json" `
  -Body '{}'
$response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
```

### 3. Test Specific URL Scrape
```powershell
$url = "https://YOUR-SERVICE.onrender.com/api/scrape/techcrunch.com"
$response = Invoke-WebRequest -Uri $url -Method POST `
  -ContentType "application/json" `
  -Body '{}'
$response.Content | ConvertFrom-Json
```

## Expected Responses

### Health Check ✓
```json
{
  "status": "healthy",
  "service": "scrapy-crawler",
  "timestamp": "2026-02-15T10:00:00.000000"
}
```

### Full Scrape ✓
```json
{
  "success": true,
  "message": "Scraped 30 articles from 3 sources",
  "stats": {
    "total_articles": 30,
    "sources": {
      "https://news.ycombinator.com": {
        "count": 5,
        "status": "success"
      },
      "https://techcrunch.com": {
        "count": 12,
        "status": "success"
      },
      "https://www.theverge.com": {
        "count": 13,
        "status": "success"
      }
    },
    "errors": []
  },
  "timestamp": "2026-02-15T10:01:23.000000"
}
```

### Specific URL Scrape ✓
```json
{
  "success": true,
  "url": "https://techcrunch.com",
  "articles_count": 12,
  "articles": [
    {
      "title": "Article title here",
      "url": "https://techcrunch.com/article",
      "description": "Article description",
      "source": "https://techcrunch.com",
      "industry": "technology",
      "scraped_at": "2026-02-15T10:01:20.000000"
    }
  ],
  "all_count": 12,
  "timestamp": "2026-02-15T10:01:23.000000"
}
```

## Response Status Codes

| Code | Meaning | Example |
|------|---------|---------|
| **200** | Success | Scrape completed, articles returned |
| **400** | Bad Request | Invalid URL or malformed JSON |
| **404** | Not Found | Wrong endpoint path |
| **500** | Server Error | Scraper crashed, check logs |

## Debugging

### Check Logs
1. Go to Render dashboard
2. Select your service
3. Click "Logs"
4. Look for errors in output

### Common Errors

**"Service failed to start"**
- Check requirements.txt has all dependencies
- Verify Procfile is correct
- Check Python version compatibility

**"ImportError: No module named 'flask'"**
- requirements.txt missing dependency
- Push changes: `git push`
- Render will redeploy

**"Timeout after 30 seconds"**
- Free tier cold start
- Service woke up from sleep
- Try again, will be faster

**"CORS error in browser"**
- Frontend can't reach API
- Check full URL in .env
- Verify CORS is enabled in api_server.py

## Local Testing (Before Deploying)

```bash
# 1. Install dependencies
cd scrapy_crawler
pip install -r requirements.txt

# 2. Test API server locally
python api_server.py

# 3. In another terminal, test endpoint
curl -X POST http://localhost:5000/api/scrape \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Integration with Next.js Frontend

Add to `.env.local`:
```
NEXT_PUBLIC_SCRAPER_API=https://YOUR-SERVICE.onrender.com
```

Then call from your API route:
```typescript
const scraperUrl = process.env.NEXT_PUBLIC_SCRAPER_API
const response = await fetch(`${scraperUrl}/api/scrape`, {
  method: 'POST',
  body: '{}'
})
```

## Service URL

Your service will be at:
```
https://YOUR-SERVICE-NAME.onrender.com
```

Example:
```
https://ovaview-scraper.onrender.com
```

Use this URL for:
- Health checks
- API calls
- Frontend integration
- Testing
- Monitoring

## Performance Expectations

| Metric | Expected |
|--------|----------|
| Cold start (after sleep) | 10-30 sec |
| Warm start | 1-5 sec |
| Single article scrape | 5-10 sec |
| All sources scrape | 20-30 sec |
| Response size | 50KB-500KB |
| Memory usage | 100-400 MB |

## Test Automation

Create a script to test periodically:

```powershell
# test-scraper.ps1
$url = "https://YOUR-SERVICE.onrender.com"

# Test health
Write-Host "Testing health..."
$health = Invoke-WebRequest -Uri "$url/health" | ConvertFrom-Json
Write-Host "Health: $($health.status)"

# Test scrape
Write-Host "Testing scrape..."
$scrape = Invoke-WebRequest -Uri "$url/api/scrape" -Method POST `
  -ContentType "application/json" -Body '{}' | ConvertFrom-Json
Write-Host "Articles scraped: $($scrape.stats.total_articles)"
```

Run daily to monitor service health!
