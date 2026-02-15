# Render Deployment Guide - OvaView Scraper

## Overview
This guide walks you through deploying the OvaView Scraper API to Render.com from GitHub.

## Prerequisites
1. ✅ GitHub account with the repository
2. ✅ Render.com account (free tier available)
3. ✅ All code committed and pushed to GitHub

## Step-by-Step Deployment

### Step 1: Prepare Your GitHub Repository
1. Make sure all files are committed:
   ```bash
   git add .
   git commit -m "Prepare for Render deployment"
   git push
   ```

2. Verify these files exist in `scrapy_crawler/` directory:
   - `api_server.py` ✓ (Flask API)
   - `crawler_runner.py` ✓ (Main scraper)
   - `requirements.txt` ✓ (Python dependencies)
   - `Procfile` ✓ (Deploy instructions)
   - `render.yaml` ✓ (Render config)
   - `.env.example` ✓ (Environment variables template)

### Step 2: Create Render Account
1. Go to [render.com](https://render.com)
2. Sign up with GitHub (easiest option)
3. Authorize Render to access your GitHub account
4. Confirm email

### Step 3: Create a New Web Service on Render

1. **Click "New +"** → **"Web Service"**

2. **Connect GitHub Repository:**
   - Search for your `ovaview-v2` repository
   - Click "Connect"
   - Choose deployment method: "Render"

3. **Configure Service Settings:**
   | Field | Value |
   |-------|-------|
   | **Name** | `ovaview-scraper` |
   | **Environment** | `Python 3` |
   | **Region** | Choose closest to you (e.g., `us-east-1`) |
   | **Branch** | `main` (or your default branch) |
   | **Root Directory** | `scrapy_crawler` |
   | **Build Command** | `pip install -r requirements.txt` |
   | **Start Command** | `gunicorn --workers 1 --timeout 300 api_server:app` |

4. **Choose Plan:**
   - Select **Free** tier (perfect for testing)
   - Limitations:
     - 1 free service
     - Auto-sleeps after 15 min of inactivity
     - Wakes up on request (5-30 second cold start)

5. **Environment Variables:**
   - Click "Advanced" → "Add Environment Variable"
   - Add:
     ```
     FLASK_ENV=production
     ```

6. **Click "Create Web Service"**

### Step 4: Monitor Initial Deployment

1. You'll see the deployment log in real-time
2. Expected log output:
   ```
   Building...
   Installing Python dependencies...
   ✓ Build successful
   ✓ Service deployed
   ✓ Live at: https://ovaview-scraper.onrender.com
   ```

3. If it fails, check:
   - Python version compatibility
   - Missing dependencies in requirements.txt
   - Root directory is set to `scrapy_crawler`

### Step 5: Test the Deployment

Once deployment is complete, test the API endpoints:

**1. Health Check:**
```bash
curl https://ovaview-scraper.onrender.com/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "scrapy-crawler",
  "timestamp": "2026-02-15T10:00:00.000000"
}
```

**2. Get Status:**
```bash
curl https://ovaview-scraper.onrender.com/api/status
```

**3. Trigger Scrape:**
```bash
curl -X POST https://ovaview-scraper.onrender.com/api/scrape \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected response:
```json
{
  "success": true,
  "message": "Scraped 30 articles from 3 sources",
  "stats": {
    "total_articles": 30,
    "sources": {
      "https://techcrunch.com": {"count": 12, "status": "success"},
      "https://www.theverge.com": {"count": 13, "status": "success"},
      "https://news.ycombinator.com": {"count": 5, "status": "success"}
    },
    "errors": []
  }
}
```

**4. Scrape Specific URL:**
```bash
curl -X POST https://ovaview-scraper.onrender.com/api/scrape/techcrunch.com \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Step 6: Enable Auto-Deploy on GitHub Changes

Render automatically deploys when you push to your main branch:

1. Make changes locally
2. Commit and push:
   ```bash
   git add .
   git commit -m "Update scraper"
   git push
   ```
3. Render automatically detects changes and redeploys
4. Monitor deployment in Render dashboard
5. Service updates without downtime

## Step 7: Connect to Your Next.js Frontend

In your Next.js app (`ovaview/src/app/api/daily-insights/scrape/route.ts`), you can now call:

```typescript
const scraperUrl = process.env.NEXT_PUBLIC_SCRAPER_API || 'http://localhost:5000'

const response = await fetch(`${scraperUrl}/api/scrape`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({})
})

const data = await response.json()
```

Add to `.env.local`:
```
NEXT_PUBLIC_SCRAPER_API=https://ovaview-scraper.onrender.com
```

## API Endpoints Reference

### Health Check
```
GET /health
```
Response: `{status, service, timestamp}`

### Service Status
```
GET /api/status
```
Response: `{status, version, endpoints}`

### Scrape All Sources
```
POST /api/scrape
Content-Type: application/json

{
  "sources": [  // optional, uses defaults if omitted
    ["https://example.com", "news", "technology"]
  ]
}
```

Response: `{success, message, stats}`

### Scrape Specific URL
```
POST /api/scrape/{url}
Content-Type: application/json

{}
```

Response: `{success, url, articles_count, articles}`

## Troubleshooting

### Service Won't Start
**Error:** `Timeout waiting for service to start`

**Solution:**
1. Check build logs for errors
2. Verify `Procfile` exists in `scrapy_crawler/`
3. Ensure `requirements.txt` has all dependencies
4. Check Python version is 3.10+

### 502 Bad Gateway
**Cause:** Service crashed or not running

**Solution:**
1. Check logs in Render dashboard
2. Restart service: "Settings" → "Restart"
3. Verify `api_server.py` runs locally first

### Imports Not Found
**Error:** `ModuleNotFoundError: No module named 'flask'`

**Solution:**
1. Verify all packages in `requirements.txt`
2. Push changes: `git add requirements.txt && git commit && git push`
3. Render will redeploy automatically

### CORS Issues
**Error:** `CORS policy blocked request`

**Solution:**
- Flask-CORS is configured for `*` (all origins)
- Should work with your Next.js frontend
- If specific domain needed, update `CORS()` in `api_server.py`

### Cold Start Delay
**Issue:** First request takes 10-30 seconds

**Cause:** Free tier auto-sleeps after 15 min of inactivity

**Solutions:**
1. Upgrade to Paid tier for 24/7 uptime
2. Use scheduled health checks: [https://betterstack.com/](https://betterstack.com/)
3. Accept slower initial response on free tier

## Monitoring & Logs

### View Logs
1. Go to Render Dashboard
2. Select your service
3. Click "Logs" tab
4. See real-time output

### Monitor Performance
- Dashboard shows:
  - Requests/day
  - Response times
  - Error rates
  - Memory usage
  - CPU usage

### Set Up Alerts (Paid Plans)
- Monitor uptime
- Get notifications on errors
- Track performance metrics

## Making Changes & Redeploying

### Workflow
```bash
# 1. Make changes locally
nano scrapy_crawler/crawler_runner.py

# 2. Test locally
python scrapy_crawler/crawler_runner.py

# 3. Commit changes
git add scrapy_crawler/
git commit -m "Improve article extraction"

# 4. Push to GitHub
git push

# 5. Render auto-deploys
# Check logs in Render dashboard
```

### Redeploy Manually (if needed)
1. Go to Render Dashboard
2. Select service
3. Click "Manual Deploy" or "Restart"

## Performance Tips

### Optimize Timeouts
- Web request timeout: 5 minutes (300s)
- Gunicorn workers: 1 (for free tier)
- Suitable for scraping 30+ articles

### Memory Optimization
- Free tier: 512 MB RAM
- Perfect for scraper operations
- Monitor in dashboard

### Build Speed
- First build: 2-5 minutes
- Subsequent builds: 1-2 minutes
- Faster if no dependencies change

## Upgrading Plan (Optional)

When ready for production:
1. Go to Render Dashboard
2. Select service
3. Click "Settings"
4. Choose paid plan ($4-7/month):
   - 24/7 uptime
   - 2 shared vCPU
   - 2GB RAM
   - Better performance

## Security Notes

1. **Never commit .env files** - Use environment variables in Render
2. **API has CORS enabled** - Restrict origins for production
3. **No authentication** - Add API keys if needed later
4. **Rate limiting** - Implement if service gets heavy use

## Success Checklist

- ✅ Repository pushed to GitHub
- ✅ All required files in `scrapy_crawler/`
- ✅ Service deployed on Render
- ✅ Health check responds 200
- ✅ Scrape endpoint returns articles
- ✅ Frontend can call the API
- ✅ Auto-deploy configured
- ✅ Monitoring set up

## Support & Resources

- **Render Docs:** https://render.com/docs
- **Python Deployment:** https://render.com/docs/deploy-python
- **GitHub Integration:** https://render.com/docs/github
- **Troubleshooting:** https://render.com/docs/troubleshooting

---

**You're all set!** Your scraper is now live and accessible from anywhere. 🚀
