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
        
        for url, spider_type, industry in sources:
            try:
                logger.info(f"Scraping {url}...")
                articles = crawler.scrape_with_scrapy(url, spider_type, industry)
                all_articles.extend(articles)
                source_stats[url] = {
                    'count': len(articles),
                    'status': 'success'
                }
                logger.info(f"✓ Scraped {len(articles)} articles from {url}")
            except Exception as e:
                error_msg = f"Error scraping {url}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                source_stats[url] = {
                    'count': 0,
                    'status': 'error',
                    'error': error_msg
                }
        
        return jsonify({
            'success': True,
            'message': f'Scraped {len(all_articles)} articles from {len(source_stats)} sources',
            'articles': all_articles,
            'stats': {
                'total_articles': len(all_articles),
                'sources': source_stats,
                'errors': errors
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


# Export app for Gunicorn
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info(f"Starting Scrapy Crawler API server on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)

