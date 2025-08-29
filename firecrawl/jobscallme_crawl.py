import json
import os
import datetime
import argparse
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import html2text
from dateutil import parser
from dateutil.relativedelta import relativedelta


@dataclass
class CrawlerConfig:
    """Configuration for the JobscallMe crawler."""
    base_url: str = "http://localhost:3002"
    target_date: Optional[datetime.date] = None
    concurrency: int = 5
    max_jobs: int = 500
    target_site_url: str = "https://www.jobscall.me/job"
    excluded_urls: List[str] = None
    
    def __post_init__(self):
        if self.excluded_urls is None:
            self.excluded_urls = ['/job/jobscallmefb']
        self.concurrency = max(1, self.concurrency)


class DateUtils:
    """Utility functions for date operations."""
    
    @staticmethod
    def parse_target_date(target_date_str: str) -> Optional[datetime.date]:
        """Parse target date string into date object."""
        if not target_date_str:
            return None
        try:
            return datetime.date.fromisoformat(target_date_str)
        except ValueError:
            print(f"⚠️  Invalid --target-date '{target_date_str}'. Expected format YYYY-MM-DD. Ignoring it.")
            return None
    
    @staticmethod
    def is_job_too_old(time_value: str) -> bool:
        """Check if a job posting is older than 1 month."""
        try:
            job_date = parser.parse(time_value)
            current_date = datetime.datetime.now()
            cutoff_date = current_date - relativedelta(months=1)
            cutoff_desc = "the last month"
            
            is_old = job_date < cutoff_date
            
            if is_old:
                print(f"📅 Job date {job_date.strftime('%Y-%m-%d')} is older than cutoff {cutoff_date.strftime('%Y-%m-%d')} ({cutoff_desc})")
            else:
                print(f"📅 Job date {job_date.strftime('%Y-%m-%d')} is within {cutoff_desc}")
            
            return is_old
            
        except Exception as e:
            print(f"⚠️ Could not parse date '{time_value}': {e}")
            return False


class HtmlProcessor:
    """Handles HTML content processing and conversion."""
    
    def __init__(self):
        self.html2text_converter = self._setup_html2text()
    
    def _setup_html2text(self) -> html2text.HTML2Text:
        """Configure html2text converter."""
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.ignore_images = False
        h.ignore_tables = False
        h.body_width = 0
        return h
    
    def extract_article_content(self, html_content: str) -> Dict:
        """Extract article content and metadata from HTML."""
        result = {
            'html_content': None,
            'article_found': False,
            'time_value': None,
            'job_date': None
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            article = soup.find('article')
            
            if article:
                print(f"✅ Found <article> tag, extracting content")
                result['html_content'] = str(article)
                result['article_found'] = True
                
                # Extract time information
                time_tag = article.find('time')
                if time_tag:
                    time_value = time_tag.get('datetime', time_tag.text.strip())
                    result['time_value'] = time_value
                    print(f"✅ Found <time> tag: {time_value}")
                    
                    try:
                        parsed_dt = parser.parse(time_value)
                        result['job_date'] = parsed_dt.date().isoformat()
                    except Exception:
                        pass
                else:
                    print(f"⚠️ No <time> tag found within article")
            else:
                print(f"⚠️ No <article> tag found, returning full HTML")
                result['html_content'] = html_content
            
            return result
        except Exception as e:
            print(f"❌ Error parsing HTML: {str(e)}")
            return {'html_content': html_content, 'article_found': False, 'time_value': None}
    
    def html_to_markdown(self, html_content: str) -> str:
        """Convert HTML content to Markdown."""
        return self.html2text_converter.handle(html_content)


class FileManager:
    """Handles file operations and directory management."""
    
    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
    
    def create_date_directories(self, date_folder: str) -> Tuple[str, str, str]:
        """Create directory structure for a given date."""
        date_dir = os.path.join(self.base_dir, "job-data", date_folder)
        jobscallme_dir = os.path.join(date_dir, "jobscallme")
        html_dir = os.path.join(jobscallme_dir, "html")
        md_dir = os.path.join(jobscallme_dir, "markdown")
        
        for directory in [date_dir, jobscallme_dir, html_dir, md_dir]:
            os.makedirs(directory, exist_ok=True)
        
        return html_dir, md_dir, date_dir
    
    def save_job_files(self, job_url: str, html_content: str, markdown_content: str, 
                      html_dir: str, md_dir: str, index_hint: int) -> Dict:
        """Save job content to HTML and Markdown files."""
        url_path = job_url.split('/job/')[-1] if '/job/' in job_url else f"job_{index_hint}"
        html_filename = f"{url_path}.html"
        md_filename = f"{url_path}.md"
        
        # Save HTML content
        html_output_file = os.path.join(html_dir, html_filename)
        with open(html_output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Save Markdown content
        md_output_file = os.path.join(md_dir, md_filename)
        with open(md_output_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        return {
            "html_filename": html_filename,
            "md_filename": md_filename,
            "html_length": len(html_content),
            "md_length": len(markdown_content)
        }
    
    def save_results_summary(self, links: List[str], scraped_jobs: List[Dict], date_folder: str):
        """Save crawling results summary to JSON files."""
        date_dir = os.path.join(self.base_dir, "job-data", date_folder)
        os.makedirs(date_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Save discovered links
        links_file = os.path.join(date_dir, "jobscall_me_links.json")
        with open(links_file, 'w', encoding='utf-8') as f:
            json.dump({
                "url": "https://www.jobscall.me/job",
                "timestamp": timestamp,
                "total_links": len(links),
                "links": links
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Links saved to: {links_file}")
        
        # Save scraping summary
        if scraped_jobs:
            summary_file = os.path.join(date_dir, "scraping_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "source": "jobscall.me",
                    "timestamp": timestamp,
                    "total_jobs": len(scraped_jobs),
                    "scraped_jobs": scraped_jobs
                }, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Scraping summary saved to: {summary_file}")
            print(f"📁 HTML files saved in: {os.path.join(date_dir, 'jobscallme', 'html')}")
            print(f"📁 Markdown files saved in: {os.path.join(date_dir, 'jobscallme', 'markdown')}")


class AsyncFirecrawlClient:
    """Async wrapper for Firecrawl API calls."""
    
    def __init__(self, base_url: str, api_key: str = "localhost"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = None
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Authorization": f"Bearer {self.api_key}"}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def scrape_async(self, url: str) -> Optional[Dict]:
        """Async scrape method using direct HTTP calls."""
        try:
            scrape_url = f"{self.base_url}/v0/scrape"
            payload = {
                "url": url,
                "formats": ["html"]
            }
            
            async with self.session.post(scrape_url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    # Debug: Print response structure
                    print(f"🔍 API Response structure: {list(result.keys()) if isinstance(result, dict) else type(result)}")
                    
                    # Return the full result for debugging, not just 'data'
                    return result
                else:
                    print(f"⚠️ HTTP {response.status} for {url}")
                    return None
        except asyncio.TimeoutError:
            print(f"⏰ Timeout scraping {url}")
            return None
        except Exception as e:
            print(f"❌ Error scraping {url}: {str(e)}")
            return None


class JobscallMeCrawler:
    """Main crawler class for JobscallMe job postings."""
    
    def __init__(self, config: CrawlerConfig = None):
        self.config = config or CrawlerConfig()
        self.firecrawl = Firecrawl(api_key="localhost", api_url=self.config.base_url)
        self.html_processor = HtmlProcessor()
        self.file_manager = FileManager()
        self.date_utils = DateUtils()
    
    def map_website(self, url: str) -> Optional[Dict]:
        try:
            print(f"🔍 Mapping website: {url}")
            map_result = self.firecrawl.map(url=url)
            print(f"✅ Successfully mapped website")
            return map_result
        except ConnectionError as e:
            print(f"❌ Connection failed: {str(e)}")
            print("ℹ️  Make sure your local Firecrawl instance is running")
            print("ℹ️  Check if the base URL is correct")
            return None
        except Exception as e:
            print(f"❌ Request failed: {str(e)}")
            return None
    
    def extract_links(self, map_result) -> List[str]:
        all_links = []
        
        if hasattr(map_result, 'links'):
            for link_result in map_result.links:
                if hasattr(link_result, 'url') and link_result.url:
                    all_links.append(link_result.url)
        elif isinstance(map_result, dict):
            possible_keys = ['links', 'urls', 'data', 'results']
            for key in possible_keys:
                if key in map_result:
                    all_links = map_result[key]
                    break
        elif isinstance(map_result, list):
            all_links = map_result
        
        return all_links
    
    def filter_job_links(self, all_links: List[str]) -> List[str]:
        """Filter links to only include valid job posting URLs."""
        filtered_links = []
        
        for link in all_links:
            if '/job/' in link:
                job_part = link.split('/job/')[-1]
                
                # Check if this is a URL we want to exclude
                should_exclude = any(excluded_url in link for excluded_url in self.config.excluded_urls)
                if should_exclude:
                    print(f"🚫 Excluding specific URL: {link}")
                    continue
                
                # Include valid job URLs
                if (job_part and 
                    '/' not in job_part and 
                    not link.endswith('/job/') and
                    '/job/category/' not in link and
                    '/job/tag/' not in link):
                    filtered_links.append(link)
        
        return filtered_links
    
    def crawl_jobscall_me(self) -> Optional[List[str]]:
        """Discover job links from the main job listing page."""
        map_result = self.map_website(self.config.target_site_url)
        if not map_result:
            return None
        
        all_links = self.extract_links(map_result)
        filtered_links = self.filter_job_links(all_links)
        
        print(f"\n📊 Discovery Summary:")
        print(f"   Total links found: {len(filtered_links)}")
        
        return filtered_links
    
    def display_results(self, links: List[str]):
        if not links:
            print("❌ No links discovered")
            return
        
        print(f"\n🔗 Discovered Links ({len(links)} total):")
        print("=" * 60)
        
        for i, link in enumerate(links, 1):
            print(f"{i:3d}. {link}")
    
    def _extract_html_content(self, scrape_result) -> Optional[str]:
        """Extract HTML content from Firecrawl scrape result."""
        # Debug: Print the structure to understand the response format
        if isinstance(scrape_result, dict):
            print(f"🔍 Response keys: {list(scrape_result.keys())}")
        
        # Try different possible locations for HTML content
        if isinstance(scrape_result, dict) and 'html' in scrape_result:
            print(f"✅ Found html key in dict")
            return scrape_result['html']
        elif isinstance(scrape_result, dict) and 'content' in scrape_result:
            print(f"✅ Found content key in dict")
            return scrape_result['content']
        elif isinstance(scrape_result, dict) and 'data' in scrape_result and isinstance(scrape_result['data'], dict):
            data = scrape_result['data']
            if 'html' in data:
                print(f"✅ Found html in data object")
                return data['html']
            elif 'content' in data:
                print(f"✅ Found content in data object")
                return data['content']
        elif hasattr(scrape_result, 'html') and scrape_result.html:
            print(f"✅ Found html attribute with content")
            return scrape_result.html
        elif hasattr(scrape_result, 'raw_html') and scrape_result.raw_html:
            print(f"✅ Found raw_html attribute with content")
            return scrape_result.raw_html
        else:
            print(f"⚠️ No HTML content found")
            if isinstance(scrape_result, dict):
                print(f"🔍 Available keys: {list(scrape_result.keys())}")
                # Print first few characters of each value to debug
                for key, value in scrape_result.items():
                    if isinstance(value, str) and len(value) > 50:
                        print(f"🔍 {key}: {value[:100]}...")
                    else:
                        print(f"🔍 {key}: {value}")
            return None
    
    def _should_stop_crawling(self, time_value: str, job_date: datetime.date) -> Tuple[bool, bool]:
        """Determine if crawling should stop and if job matches criteria."""
        if self.config.target_date is not None:
            matches = (job_date == self.config.target_date) if job_date else False
            if not matches:
                print(f"⏭️  Skipping job not on target date {self.config.target_date} (found: {job_date})")
            return False, matches  # Never stop early in target date mode
        else:
            # Check age cutoff
            if DateUtils.is_job_too_old(time_value):
                print(f"🛑 Job posting is older than cutoff (1 month) - stopping crawl")
                return True, False
            return False, True
    
    async def scrape_job_page_async(self, executor, url: str) -> Optional[Dict]:
        """Async wrapper around sync scraping - more reliable than direct HTTP."""
        try:
            # Run the sync method in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(executor, self.scrape_job_page, url)
            return result
        except Exception as e:
            print(f"❌ Failed to scrape {url}: {str(e)}")
            return None
    
    def scrape_job_page(self, url: str) -> Optional[Dict]:
        """Synchronous wrapper for backward compatibility."""
        try:
            print(f"📄 Scraping job page: {url}")
            scrape_result = self.firecrawl.scrape(url=url, formats=['html'])
            
            if not scrape_result:
                print(f"⚠️ No content found for: {url}")
                return None
            
            html_content = self._extract_html_content(scrape_result)
            if not html_content:
                return None
            
            # Process HTML content
            processed_result = self.html_processor.extract_article_content(html_content)
            
            # Handle date logic if time value is found
            if processed_result.get('time_value'):
                try:
                    parsed_dt = parser.parse(processed_result['time_value'])
                    job_date = parsed_dt.date()
                    processed_result['job_date'] = job_date.isoformat()
                    
                    stop_crawling, matches_criteria = self._should_stop_crawling(
                        processed_result['time_value'], job_date
                    )
                    processed_result['stop_crawling'] = stop_crawling
                    processed_result['matches_target_date'] = matches_criteria
                except Exception:
                    processed_result['stop_crawling'] = False
                    processed_result['matches_target_date'] = self.config.target_date is None
            else:
                processed_result['stop_crawling'] = False
                processed_result['matches_target_date'] = self.config.target_date is None
            
            return processed_result
            
        except ConnectionError as e:
            print(f"❌ Connection failed for {url}: {str(e)}")
            print("ℹ️  Make sure your local Firecrawl instance is running")
            return None
        except Exception as e:
            print(f"❌ Failed to scrape {url}: {str(e)}")
            return None
    
    
    async def scrape_all_jobs_async(self, job_links: List[str]) -> Tuple[List[Dict], str]:
        """Async version using ThreadPoolExecutor - more reliable than direct HTTP."""
        scraped_jobs = []
        jobs_to_scrape = job_links[:self.config.max_jobs]
        
        print(f"\n🚀 Starting ASYNC scrape of {len(jobs_to_scrape)} job pages...")
        
        # Get current date for folder structure
        today = datetime.datetime.now()
        date_folder = today.strftime("%Y%m%d")
        
        # Create directory structure
        html_dir, md_dir, date_dir = self.file_manager.create_date_directories(date_folder)
        
        total = len(jobs_to_scrape)
        print(f"\n🚦 Processing {total} jobs with async concurrency={self.config.concurrency}...")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.config.concurrency)
        
        async def scrape_with_semaphore(executor, url: str, index: int):
            async with semaphore:
                result = await self.scrape_job_page_async(executor, url)
                if result:
                    print(f"\n[{index+1}/{total}] ✅ Completed: {url}")
                else:
                    print(f"\n[{index+1}/{total}] ❌ Failed: {url}")
                return url, result, index
        
        def save_result(job_url: str, result: Dict, index_hint: int):
            nonlocal scraped_jobs
            # If target date mode is active, skip saving non-matching jobs
            if self.config.target_date is not None and not result.get('matches_target_date', False):
                return
            
            html_content = result['html_content']
            markdown_content = self.html_processor.html_to_markdown(html_content)
            
            # Save files and get metadata
            file_info = self.file_manager.save_job_files(
                job_url, html_content, markdown_content, html_dir, md_dir, index_hint
            )
            
            # Create job info with metadata
            job_info = {
                "url": job_url,
                **file_info,
                "article_found": result.get('article_found', False)
            }
            
            # Add time value if available
            if result.get('time_value'):
                job_info["time_value"] = result['time_value']
            if result.get('job_date'):
                job_info["job_date"] = result['job_date']
            
            scraped_jobs.append(job_info)
            print(f"✅ Saved: {file_info['html_filename']} / {file_info['md_filename']}")
        
        # Use ThreadPoolExecutor for reliable sync method execution
        with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            # Create all tasks
            tasks = [
                scrape_with_semaphore(executor, url, idx) 
                for idx, url in enumerate(jobs_to_scrape)
            ]
            
            # Process results as they complete
            should_stop = False
            for coro in asyncio.as_completed(tasks):
                if should_stop:
                    break
                    
                try:
                    job_url, result, idx = await coro
                    
                    if result and isinstance(result, dict) and result.get('html_content'):
                        # Early stop only when not in target-date mode
                        if result.get('stop_crawling', False) and self.config.target_date is None:
                            print(f"🛑 Stopping crawl - encountered job older than 1 month")
                            should_stop = True
                            # Cancel remaining tasks
                            for task in tasks:
                                if not task.done():
                                    task.cancel()
                            break
                        
                        save_result(job_url, result, idx)
                        
                except Exception as e:
                    print(f"❌ Error processing result: {e}")
                    continue
        
        print(f"\n📊 Async Scraping Summary:")
        print(f"   Successfully scraped: {len(scraped_jobs)}/{len(jobs_to_scrape)} jobs")
        
        return scraped_jobs, date_folder
    
    def scrape_all_jobs(self, job_links: List[str]) -> Tuple[List[Dict], str]:
        """Main scraping method - uses async for better performance."""
        # Run the async version
        return asyncio.run(self.scrape_all_jobs_async(job_links))
    
    def scrape_all_jobs_sync(self, job_links: List[str]) -> Tuple[List[Dict], str]:
        """Synchronous fallback version (slower but more compatible)."""
        scraped_jobs = []
        jobs_to_scrape = job_links[:self.config.max_jobs]
        
        print(f"\n🔍 Starting SYNC scrape of {len(jobs_to_scrape)} job pages...")
        
        # Get current date for folder structure
        today = datetime.datetime.now()
        date_folder = today.strftime("%Y%m%d")
        
        # Create directory structure
        html_dir, md_dir, date_dir = self.file_manager.create_date_directories(date_folder)
        
        total = len(jobs_to_scrape)
        print(f"\n🚦 Submitting {total} jobs with concurrency={self.config.concurrency}...")
        processed = 0
        
        def save_result(job_url: str, result: Dict, index_hint: int):
            nonlocal scraped_jobs
            # If target date mode is active, skip saving non-matching jobs
            if self.config.target_date is not None and not result.get('matches_target_date', False):
                return
            
            html_content = result['html_content']
            markdown_content = self.html_processor.html_to_markdown(html_content)
            
            # Save files and get metadata
            file_info = self.file_manager.save_job_files(
                job_url, html_content, markdown_content, html_dir, md_dir, index_hint
            )
            
            # Create job info with metadata
            job_info = {
                "url": job_url,
                **file_info,
                "article_found": result.get('article_found', False)
            }
            
            # Add time value if available
            if result.get('time_value'):
                job_info["time_value"] = result['time_value']
            if result.get('job_date'):
                job_info["job_date"] = result['job_date']
            
            scraped_jobs.append(job_info)
            print(f"✅ Saved: {file_info['html_filename']} / {file_info['md_filename']}")
        
        with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            future_to_url = {executor.submit(self.scrape_job_page, url): url for url in jobs_to_scrape}
            for idx, future in enumerate(as_completed(future_to_url), 1):
                job_url = future_to_url[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"❌ Error scraping {job_url}: {e}")
                    continue
                processed += 1
                print(f"\n[{processed}/{total}] Completed: {job_url}")
                
                if result and isinstance(result, dict) and result.get('html_content'):
                    # Early stop only when not in target-date mode
                    if result.get('stop_crawling', False) and self.config.target_date is None:
                        print(f"🛑 Stopping crawl - encountered job older than 1 month")
                        # Cancel remaining futures
                        for f in future_to_url:
                            if not f.done():
                                f.cancel()
                        break
                    
                    save_result(job_url, result, idx)
                else:
                    print(f"❌ Failed to extract HTML content from: {job_url}")
        
        print(f"\n📊 Scraping Summary:")
        print(f"   Successfully scraped: {len(scraped_jobs)}/{len(jobs_to_scrape)} jobs")
        
        return scraped_jobs, date_folder
    
    def save_results(self, links: List[str], scraped_jobs: List[Dict] = None, date_folder: str = None):
        """Save results using the file manager."""
        if not date_folder:
            date_folder = datetime.datetime.now().strftime("%Y%m%d")
        
        self.file_manager.save_results_summary(links, scraped_jobs or [], date_folder)


def get_base_url() -> str:
    base_url = os.getenv('FIRECRAWL_BASE_URL', 'http://localhost:3002')
    
    if base_url == 'http://localhost:3002':
        print(f"ℹ️  Using default localhost URL: {base_url}")
        print("ℹ️  Set FIRECRAWL_BASE_URL environment variable to override")
    else:
        print(f"ℹ️  Using custom Firecrawl URL: {base_url}")
    
    return base_url


def main():
    """Main function to run the JobscallMe crawler."""
    parser_obj = argparse.ArgumentParser(description="Crawl jobscall.me and scrape jobs")
    parser_obj.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Only crawl jobs posted on this specific date (YYYY-MM-DD). When provided, does not stop early."
    )
    parser_obj.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent workers for scraping (default: 5). Increase to speed up scraping; be mindful of server limits."
    )
    args = parser_obj.parse_args()

    base_url = get_base_url()
    
    # Create configuration
    config = CrawlerConfig(
        base_url=base_url,
        target_date=DateUtils.parse_target_date(args.target_date),
        concurrency=args.concurrency
    )
    
    # Initialize crawler
    crawler = JobscallMeCrawler(config)
    
    # Display configuration
    if config.target_date is not None:
        print(f"ℹ️  Target date mode: only jobs from {config.target_date.isoformat()} will be saved")
    else:
        print("ℹ️  Using default cutoff: last 1 month")
    print(f"ℹ️  Concurrency: {config.concurrency} workers")
    
    # Step 1: Map and discover job links
    print("🚀 Step 1: Mapping website to discover job links...")
    links = crawler.crawl_jobscall_me()
    
    if not links:
        print("❌ Failed to discover job links")
        return
    
    crawler.display_results(links)
    
    # Step 2: Scrape individual job pages
    print("\n🚀 Step 2: Scraping individual job pages...")
    scraped_jobs, date_folder = crawler.scrape_all_jobs(links)
    
    # Step 3: Save results
    print("\n🚀 Step 3: Saving results...")
    crawler.save_results(links, scraped_jobs, date_folder)
    
    print(f"\n✅ Crawling completed successfully!")
    print(f"📊 Summary:")
    print(f"   - Discovered links: {len(links)}")
    print(f"   - Successfully scraped: {len(scraped_jobs)} jobs")
    print(f"   - HTML and Markdown files saved in: job-data/{date_folder}/jobscallme/")
    print(f"   - HTML files: job-data/{date_folder}/jobscallme/html/")
    print(f"   - Markdown files: job-data/{date_folder}/jobscallme/markdown/")


if __name__ == "__main__":
    main()