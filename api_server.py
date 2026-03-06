"""
REST API wrapper for the Scrapy crawler
Runs the crawler via HTTP requests
Production-ready with CORS, error handling, and health checks
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import sys
import os
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from crawler_runner import ScrapyArticleCrawler

app = Flask(__name__)

# Enable CORS for all origins in development, restrict in production
CORS(app, resources={
    r"/api/*": {
        "origins": ["*"],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"],
        "supports_credentials": True
    }
})

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'scrapy-crawler',
        'timestamp': datetime.now().isoformat()
    }), 200


@app.route('/api/scrape', methods=['POST', 'GET', 'OPTIONS'])
def scrape():
    """Trigger a scrape immediately"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        # Get optional parameters from request body
        body = request.get_json() or {}
        sources = body.get('sources', [
            ("https://news.ycombinator.com", "news", "technology"),
        ])
        
        # Normalize sources - handle both lists and tuples
        normalized_sources = []
        for source in sources:
            if isinstance(source, (list, tuple)) and len(source) >= 2:
                url = source[0]
                spider_type = source[1] if len(source) > 1 else 'news'
                industry = source[2] if len(source) > 2 else 'general'
                normalized_sources.append((url, spider_type, industry))
            elif isinstance(source, str):
                normalized_sources.append((source, 'news', 'general'))
        
        sources = normalized_sources
        
        logger.info(f"Starting scrape with {len(sources)} sources")
        crawler = ScrapyArticleCrawler()
        
        all_articles = []
        errors = []
        source_stats = {}
        
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        GLOBAL_TIMEOUT = 90  # Must finish within 90s so Vercel gets the response before its 120s timeout
        start_time = time.time()
        
        def scrape_one_source(url, spider_type, industry):
            """Scrape a single source with a per-source time limit."""
            try:
                articles = crawler.scrape_with_scrapy(url, spider_type, industry)
                return url, articles, None
            except Exception as e:
                return url, [], str(e)
        
        # Run sources in parallel (max 4 concurrent to avoid overwhelming the server)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(scrape_one_source, url, st, ind): url
                for url, st, ind in sources
            }
            
            for future in as_completed(futures, timeout=GLOBAL_TIMEOUT):
                url = futures[future]
                elapsed = time.time() - start_time
                if elapsed > GLOBAL_TIMEOUT:
                    logger.warning(f"Global timeout reached ({elapsed:.0f}s), stopping")
                    break
                
                try:
                    src_url, articles, error = future.result(timeout=5)
                    if error:
                        logger.error(f"Error scraping {src_url}: {error}")
                        errors.append(f"Error scraping {src_url}: {error}")
                        source_stats[src_url] = {'count': 0, 'status': 'error', 'error': error}
                    else:
                        all_articles.extend(articles)
                        source_stats[src_url] = {'count': len(articles), 'status': 'success'}
                        logger.info(f"✓ Scraped {len(articles)} articles from {src_url}")
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    errors.append(f"Error scraping {url}: {str(e)}")
                    source_stats[url] = {'count': 0, 'status': 'error', 'error': str(e)}
        
        elapsed = time.time() - start_time
        logger.info(f"Scrape completed in {elapsed:.1f}s: {len(all_articles)} articles from {len(source_stats)} sources")
        
        return jsonify({
            'success': True,
            'message': f'Scraped {len(all_articles)} articles from {len(source_stats)} sources in {elapsed:.0f}s',
            'articles': all_articles,
            'stats': {
                'total_articles': len(all_articles),
                'sources': source_stats,
                'errors': errors,
                'elapsed_seconds': round(elapsed, 1)
            },
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Scrape failed: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/scrape/<path:url>', methods=['POST', 'OPTIONS'])
def scrape_url(url):
    """Scrape a specific URL"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        # Decode URL if needed
        full_url = f"https://{url}" if not url.startswith('http') else url
        
        logger.info(f"Scraping specific URL: {full_url}")
        crawler = ScrapyArticleCrawler()
        articles = crawler.scrape_with_scrapy(full_url, "news", "general")
        
        return jsonify({
            'success': True,
            'url': full_url,
            'articles_count': len(articles),
            'articles': articles[:10],
            'all_count': len(articles),
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Scrape failed for {url}: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/status', methods=['GET'])
def status():
    """Get API status and version"""
    return jsonify({
        'status': 'operational',
        'service': 'ovaview-scraper',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat(),
        'endpoints': {
            'GET /health': 'Health check',
            'GET /api/status': 'Service status',
            'POST /api/scrape': 'Trigger scrape (all sources)',
            'POST /api/scrape/<url>': 'Scrape specific URL'
        }
    }), 200


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'timestamp': datetime.now().isoformat()
    }), 404


@app.errorhandler(500)
def server_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}", exc_info=True)
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500


@app.errorhandler(Exception)
def handle_exception(e):
    """Handle all uncaught exceptions - always return JSON"""
    logger.error(f"Unhandled exception: {e}", exc_info=True)
    return jsonify({
        'success': False,
        'error': str(e),
        'timestamp': datetime.now().isoformat()
    }), 500


@app.route('/api/scrape/social', methods=['POST', 'OPTIONS'])
def scrape_social():
    """Scrape social media platforms for posts matching keywords"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        body = request.get_json() or {}
        keywords = body.get('keywords', [])
        platforms = body.get('platforms', ['twitter', 'tiktok', 'instagram', 'linkedin', 'facebook'])
        save_to_api = body.get('save', False)
        
        if not keywords:
            return jsonify({
                'success': False,
                'error': 'keywords array is required',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # Ensure keywords is a list
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(',')]
        
        logger.info(f"Scraping social media for keywords: {keywords} on platforms: {platforms}")
        crawler = ScrapyArticleCrawler()
        
        posts = crawler.scrape_social_media(keywords, platforms)
        
        saved_count = 0
        if save_to_api and posts:
            saved_count = crawler.save_social_posts(posts)
        
        return jsonify({
            'success': True,
            'message': f'Found {len(posts)} social media posts',
            'posts': posts[:50],  # Return first 50
            'total_count': len(posts),
            'saved_count': saved_count,
            'keywords': keywords,
            'platforms': platforms,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Social scrape failed: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get crawler configuration"""
    return jsonify({
        'success': True,
        'config': {
            'max_article_age_days': 7,
            'concurrent_requests': 4,
            'download_delay': 2,
            'supported_platforms': ['twitter', 'linkedin', 'facebook', 'instagram', 'tiktok'],
        },
        'timestamp': datetime.now().isoformat()
    }), 200


# Export app for Gunicorn
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info(f"Starting Scrapy Crawler API server on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)

