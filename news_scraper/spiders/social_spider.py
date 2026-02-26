"""
Social Media Spider for crawling social media platforms
Extracts posts based on keywords and generates embeddable links
"""
import scrapy
import re
import json
from datetime import datetime
from urllib.parse import urlparse, quote


class SocialMediaSpider(scrapy.Spider):
    """
    Spider for crawling social media platforms
    Supports Twitter/X, Facebook, LinkedIn, YouTube
    """
    name = "social"
    
    # Platform-specific embed URL patterns
    EMBED_PATTERNS = {
        'twitter': 'https://publish.twitter.com/oembed?url={url}',
        'x': 'https://publish.twitter.com/oembed?url={url}',
        'facebook': 'https://www.facebook.com/plugins/post/oembed.json/?url={url}',
        'linkedin': None,  # LinkedIn doesn't have public oEmbed
        'youtube': 'https://www.youtube.com/oembed?url={url}&format=json',
        'instagram': 'https://api.instagram.com/oembed?url={url}',
        'tiktok': 'https://www.tiktok.com/oembed?url={url}',
    }
    
    def __init__(self, keywords=None, platforms=None, *args, **kwargs):
        super(SocialMediaSpider, self).__init__(*args, **kwargs)
        self.keywords = keywords.split(',') if keywords else []
        self.platforms = platforms.split(',') if platforms else ['twitter', 'youtube']
    
    def start_requests(self):
        """Generate search requests for each platform and keyword"""
        for keyword in self.keywords:
            keyword = keyword.strip()
            if not keyword:
                continue
                
            for platform in self.platforms:
                platform = platform.strip().lower()
                
                if platform in ['twitter', 'x']:
                    # Twitter/X search
                    url = f'https://twitter.com/search?q={quote(keyword)}&src=typed_query&f=live'
                    yield scrapy.Request(
                        url,
                        callback=self.parse_twitter,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'keyword': keyword,
                            'platform': 'TWITTER',
                        }
                    )
                
                elif platform == 'youtube':
                    # YouTube search
                    url = f'https://www.youtube.com/results?search_query={quote(keyword)}'
                    yield scrapy.Request(
                        url,
                        callback=self.parse_youtube,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'keyword': keyword,
                            'platform': 'YOUTUBE',
                        }
                    )
                
                elif platform == 'linkedin':
                    # LinkedIn posts search (limited without auth)
                    url = f'https://www.linkedin.com/search/results/content/?keywords={quote(keyword)}'
                    yield scrapy.Request(
                        url,
                        callback=self.parse_linkedin,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'keyword': keyword,
                            'platform': 'LINKEDIN',
                        }
                    )
    
    async def parse_twitter(self, response):
        """Parse Twitter/X search results"""
        page = response.meta.get('playwright_page')
        keyword = response.meta.get('keyword')
        
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
            await page.wait_for_timeout(2000)  # Wait for tweets to load
        except:
            pass
        
        # Extract tweets
        tweets = await page.query_selector_all('article[data-testid="tweet"]')
        
        for tweet in tweets[:20]:  # Limit to 20 tweets
            try:
                # Get tweet link
                time_elem = await tweet.query_selector('time')
                link_elem = await time_elem.query_selector('xpath=ancestor::a') if time_elem else None
                tweet_url = await link_elem.get_attribute('href') if link_elem else None
                
                if tweet_url and not tweet_url.startswith('http'):
                    tweet_url = f'https://twitter.com{tweet_url}'
                
                if not tweet_url:
                    continue
                
                # Extract tweet ID from URL
                tweet_id_match = re.search(r'/status/(\d+)', tweet_url)
                tweet_id = tweet_id_match.group(1) if tweet_id_match else None
                
                if not tweet_id:
                    continue
                
                # Get content
                content_elem = await tweet.query_selector('[data-testid="tweetText"]')
                content = await content_elem.text_content() if content_elem else ''
                
                # Get author info
                author_elem = await tweet.query_selector('[data-testid="User-Name"]')
                author_text = await author_elem.text_content() if author_elem else ''
                author_parts = author_text.split('@') if author_text else ['', '']
                author_name = author_parts[0].strip() if author_parts else ''
                author_handle = author_parts[1].split()[0] if len(author_parts) > 1 else ''
                
                # Get timestamp
                datetime_attr = await time_elem.get_attribute('datetime') if time_elem else None
                posted_at = datetime_attr or datetime.now().isoformat()
                
                # Get engagement metrics
                metrics = {}
                for metric in ['reply', 'retweet', 'like']:
                    metric_elem = await tweet.query_selector(f'[data-testid="{metric}"]')
                    if metric_elem:
                        metric_text = await metric_elem.text_content()
                        metrics[metric] = self._parse_count(metric_text)
                
                # Get media URLs
                media_urls = []
                images = await tweet.query_selector_all('img[src*="pbs.twimg.com/media"]')
                for img in images:
                    src = await img.get_attribute('src')
                    if src:
                        media_urls.append(src)
                
                # Extract hashtags and mentions
                hashtags = re.findall(r'#(\w+)', content)
                mentions = re.findall(r'@(\w+)', content)
                
                # Generate embed URL
                embed_url = self.EMBED_PATTERNS['twitter'].format(url=quote(tweet_url, safe=''))
                
                yield {
                    'platform': 'TWITTER',
                    'post_id': tweet_id,
                    'content': content,
                    'author_handle': author_handle,
                    'author_name': author_name,
                    'post_url': tweet_url,
                    'embed_url': embed_url,
                    'media_urls': media_urls,
                    'media_type': 'image' if media_urls else 'text',
                    'likes_count': metrics.get('like', 0),
                    'comments_count': metrics.get('reply', 0),
                    'shares_count': metrics.get('retweet', 0),
                    'hashtags': hashtags,
                    'mentions': mentions,
                    'keywords': keyword,
                    'posted_at': posted_at,
                    'scraped_at': datetime.now().isoformat(),
                }
            except Exception as e:
                self.logger.error(f"Error parsing tweet: {e}")
                continue
        
        await page.close()
    
    async def parse_youtube(self, response):
        """Parse YouTube search results"""
        page = response.meta.get('playwright_page')
        keyword = response.meta.get('keyword')
        
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
            await page.wait_for_timeout(2000)
        except:
            pass
        
        # Extract video results
        videos = await page.query_selector_all('ytd-video-renderer')
        
        for video in videos[:20]:
            try:
                # Get video link
                link_elem = await video.query_selector('a#video-title')
                video_url = await link_elem.get_attribute('href') if link_elem else None
                title = await link_elem.get_attribute('title') if link_elem else ''
                
                if video_url and not video_url.startswith('http'):
                    video_url = f'https://www.youtube.com{video_url}'
                
                if not video_url:
                    continue
                
                # Extract video ID
                video_id_match = re.search(r'[?&]v=([^&]+)', video_url)
                video_id = video_id_match.group(1) if video_id_match else None
                
                if not video_id:
                    continue
                
                # Get channel info
                channel_elem = await video.query_selector('ytd-channel-name a')
                channel_name = await channel_elem.text_content() if channel_elem else ''
                channel_url = await channel_elem.get_attribute('href') if channel_elem else ''
                
                # Get view count
                views_elem = await video.query_selector('#metadata-line span')
                views_text = await views_elem.text_content() if views_elem else ''
                views_count = self._parse_count(views_text)
                
                # Get thumbnail
                thumb_elem = await video.query_selector('img')
                thumbnail = await thumb_elem.get_attribute('src') if thumb_elem else ''
                
                # Generate embed URL and HTML
                embed_url = f'https://www.youtube.com/embed/{video_id}'
                embed_html = f'<iframe width="560" height="315" src="{embed_url}" frameborder="0" allowfullscreen></iframe>'
                
                yield {
                    'platform': 'YOUTUBE',
                    'post_id': video_id,
                    'content': title,
                    'author_handle': channel_url.split('/')[-1] if channel_url else '',
                    'author_name': channel_name,
                    'post_url': video_url,
                    'embed_url': embed_url,
                    'embed_html': embed_html,
                    'media_urls': [thumbnail] if thumbnail else [],
                    'media_type': 'video',
                    'views_count': views_count,
                    'hashtags': [],
                    'mentions': [],
                    'keywords': keyword,
                    'posted_at': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
            except Exception as e:
                self.logger.error(f"Error parsing YouTube video: {e}")
                continue
        
        await page.close()
    
    async def parse_linkedin(self, response):
        """Parse LinkedIn search results (limited without auth)"""
        page = response.meta.get('playwright_page')
        keyword = response.meta.get('keyword')
        
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
        except:
            pass
        
        # LinkedIn requires authentication for most content
        # This is a basic implementation that may have limited results
        posts = await page.query_selector_all('.feed-shared-update-v2')
        
        for post in posts[:10]:
            try:
                # Get post content
                content_elem = await post.query_selector('.feed-shared-text')
                content = await content_elem.text_content() if content_elem else ''
                
                # Get author info
                author_elem = await post.query_selector('.feed-shared-actor__name')
                author_name = await author_elem.text_content() if author_elem else ''
                
                # Get post URL (if available)
                link_elem = await post.query_selector('a.feed-shared-update-v2__permalink')
                post_url = await link_elem.get_attribute('href') if link_elem else ''
                
                if not post_url:
                    continue
                
                # Extract post ID from URL
                post_id_match = re.search(r'activity-(\d+)', post_url)
                post_id = post_id_match.group(1) if post_id_match else str(hash(post_url))
                
                yield {
                    'platform': 'LINKEDIN',
                    'post_id': post_id,
                    'content': content.strip(),
                    'author_name': author_name.strip(),
                    'post_url': post_url,
                    'embed_url': None,  # LinkedIn doesn't support public embeds
                    'media_urls': [],
                    'media_type': 'text',
                    'hashtags': re.findall(r'#(\w+)', content),
                    'mentions': [],
                    'keywords': keyword,
                    'posted_at': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
            except Exception as e:
                self.logger.error(f"Error parsing LinkedIn post: {e}")
                continue
        
        await page.close()
    
    def _parse_count(self, text):
        """Parse count strings like '1.2K', '5M' into integers"""
        if not text:
            return 0
        
        text = text.strip().upper()
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        for suffix, mult in multipliers.items():
            if suffix in text:
                try:
                    num = float(text.replace(suffix, '').replace(',', '').strip())
                    return int(num * mult)
                except:
                    return 0
        
        try:
            return int(text.replace(',', '').strip())
        except:
            return 0


def get_embed_html(platform: str, post_url: str, post_id: str = None) -> dict:
    """
    Generate embed HTML for different platforms
    Returns dict with embed_url and embed_html
    """
    platform = platform.upper()
    
    if platform in ['TWITTER', 'X']:
        return {
            'embed_url': f'https://publish.twitter.com/oembed?url={quote(post_url, safe="")}',
            'embed_html': f'''<blockquote class="twitter-tweet"><a href="{post_url}"></a></blockquote>
<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>''',
        }
    
    elif platform == 'YOUTUBE':
        video_id = post_id
        if not video_id:
            match = re.search(r'[?&]v=([^&]+)', post_url)
            video_id = match.group(1) if match else None
        
        if video_id:
            return {
                'embed_url': f'https://www.youtube.com/embed/{video_id}',
                'embed_html': f'<iframe width="100%" height="315" src="https://www.youtube.com/embed/{video_id}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',
            }
    
    elif platform == 'FACEBOOK':
        return {
            'embed_url': f'https://www.facebook.com/plugins/post.php?href={quote(post_url, safe="")}',
            'embed_html': f'<iframe src="https://www.facebook.com/plugins/post.php?href={quote(post_url, safe="")}&show_text=true&width=500" width="500" height="500" style="border:none;overflow:hidden" scrolling="no" frameborder="0" allowfullscreen="true"></iframe>',
        }
    
    elif platform == 'INSTAGRAM':
        return {
            'embed_url': f'https://api.instagram.com/oembed?url={quote(post_url, safe="")}',
            'embed_html': f'<blockquote class="instagram-media" data-instgrm-permalink="{post_url}"><a href="{post_url}"></a></blockquote><script async src="//www.instagram.com/embed.js"></script>',
        }
    
    elif platform == 'TIKTOK':
        return {
            'embed_url': f'https://www.tiktok.com/oembed?url={quote(post_url, safe="")}',
            'embed_html': f'<blockquote class="tiktok-embed" cite="{post_url}"><a href="{post_url}"></a></blockquote><script async src="https://www.tiktok.com/embed.js"></script>',
        }
    
    elif platform == 'LINKEDIN':
        # LinkedIn doesn't have public embeds, return iframe attempt
        return {
            'embed_url': None,
            'embed_html': f'<a href="{post_url}" target="_blank">View on LinkedIn</a>',
        }
    
    return {'embed_url': None, 'embed_html': None}
