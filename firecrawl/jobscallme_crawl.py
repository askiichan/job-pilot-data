import json
import os
import datetime
from typing import Dict, List, Optional
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import html2text
from dateutil import parser
from dateutil.relativedelta import relativedelta


class JobscallMeCrawler:
    def __init__(self, base_url: str = None):
        # Set default localhost URL if not provided
        if not base_url:
            base_url = "http://localhost:3002"  # Default local Firecrawl port
        
        # Initialize Firecrawl with localhost URL and dummy API key
        # For localhost, the API key is not validated but still required by the SDK
        self.firecrawl = Firecrawl(api_key="localhost", api_url=base_url)
    
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
        filtered_links = []
        
        # URLs to specifically exclude
        excluded_urls = [
            '/job/jobscallmefb'  # Exclude the jobscallmefb URL
        ]
        
        for link in all_links:
            if '/job/' in link:
                job_part = link.split('/job/')[-1]
                
                # Check if this is a URL we want to exclude
                should_exclude = False
                for excluded_url in excluded_urls:
                    if excluded_url in link:
                        should_exclude = True
                        print(f"🚫 Excluding specific URL: {link}")
                        break
                
                if should_exclude:
                    continue
                
                if (job_part and 
                    '/' not in job_part and 
                    not link.endswith('/job/') and
                    '/job/category/' not in link and
                    '/job/tag/' not in link):
                    filtered_links.append(link)
        
        return filtered_links
    
    def is_job_too_old(self, time_value: str) -> bool:
        """
        Check if a job posting is over 1 month old
        
        Args:
            time_value (str): Time value from the job posting
            
        Returns:
            bool: True if job is over 1 month old, False otherwise
        """
        try:
            # Parse the time value
            job_date = parser.parse(time_value)
            
            # Get current date
            current_date = datetime.datetime.now()
            
            # Calculate 1 month ago
            one_month_ago = current_date - relativedelta(months=1)
            
            # Check if job date is older than 1 month
            is_old = job_date < one_month_ago
            
            if is_old:
                print(f"📅 Job date {job_date.strftime('%Y-%m-%d')} is older than {one_month_ago.strftime('%Y-%m-%d')}")
            else:
                print(f"📅 Job date {job_date.strftime('%Y-%m-%d')} is within the last month")
            
            return is_old
            
        except Exception as e:
            print(f"⚠️ Could not parse date '{time_value}': {e}")
            # If we can't parse the date, assume it's not too old and continue
            return False
    
    def crawl_jobscall_me(self) -> Optional[List[str]]:
        target_url = "https://www.jobscall.me/job"
        
        map_result = self.map_website(target_url)
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
    
    def scrape_job_page(self, url: str) -> Optional[str]:
        """
        Scrape individual job page to get raw HTML content
        
        Args:
            url (str): URL of the job page to scrape
            
        Returns:
            str: Raw HTML content from the page
        """
        try:
            print(f"📄 Scraping job page: {url}")
            
            # Use Firecrawl's scrape method to extract content with HTML format
            # According to docs: https://docs.firecrawl.dev/sdks/python#scraping-a-url
            scrape_result = self.firecrawl.scrape(url=url, formats=['html'])
            
            # Debug: Print available attributes
            print(f"🔍 Scrape result type: {type(scrape_result)}")
            
            # Get HTML content from the result
            if scrape_result:
                # Extract the full HTML content first
                html_content = None
                
                # Check if it's a dictionary with html key
                if isinstance(scrape_result, dict) and 'html' in scrape_result:
                    print(f"✅ Found html key in dict")
                    html_content = scrape_result['html']
                # Check if it has html attribute
                elif hasattr(scrape_result, 'html') and scrape_result.html:
                    print(f"✅ Found html attribute with content")
                    html_content = scrape_result.html
                # Check for raw_html attribute
                elif hasattr(scrape_result, 'raw_html') and scrape_result.raw_html:
                    print(f"✅ Found raw_html attribute with content")
                    html_content = scrape_result.raw_html
                else:
                    print(f"⚠️ No HTML content found")
                    print(f"🔍 Result structure: {type(scrape_result)}")
                    if isinstance(scrape_result, dict):
                        print(f"🔍 Available dict keys: {list(scrape_result.keys())}")
                    else:
                        print(f"🔍 Available attributes: {dir(scrape_result)}")
                    return None
                
                # Extract only the content within <article> tags and get time value
                if html_content:
                    try:
                        soup = BeautifulSoup(html_content, 'html.parser')
                        article = soup.find('article')
                        
                        # Create a result object with metadata
                        result = {
                            'html_content': None,
                            'article_found': False,
                            'time_value': None
                        }
                        
                        if article:
                            print(f"✅ Found <article> tag, extracting content")
                            # Store the article content
                            result['html_content'] = str(article)
                            result['article_found'] = True
                            
                            # Try to extract time value from within the article
                            time_tag = article.find('time')
                            if time_tag:
                                # Get the datetime attribute if available, otherwise use the text content
                                time_value = time_tag.get('datetime', time_tag.text.strip())
                                result['time_value'] = time_value
                                print(f"✅ Found <time> tag: {time_value}")
                                
                                # Check if the job posting is over 1 month old
                                if self.is_job_too_old(time_value):
                                    print(f"🛑 Job posting is over 1 month old - stopping crawl")
                                    result['stop_crawling'] = True
                                else:
                                    result['stop_crawling'] = False
                            else:
                                print(f"⚠️ No <time> tag found within article")
                                result['stop_crawling'] = False
                        else:
                            print(f"⚠️ No <article> tag found, returning full HTML")
                            result['html_content'] = html_content
                        
                        return result
                    except Exception as e:
                        print(f"❌ Error parsing HTML: {str(e)}")
                        return {'html_content': html_content, 'article_found': False, 'time_value': None}
                
                return html_content
            else:
                print(f"⚠️ No content found for: {url}")
                return None
                
        except ConnectionError as e:
            print(f"❌ Connection failed for {url}: {str(e)}")
            print("ℹ️  Make sure your local Firecrawl instance is running")
            return None
        except Exception as e:
            print(f"❌ Failed to scrape {url}: {str(e)}")
            return None
    
    def html_to_markdown(self, html_content: str) -> str:
        """
        Convert HTML content to Markdown
        
        Args:
            html_content (str): HTML content to convert
            
        Returns:
            str: Converted Markdown content
        """
        # Configure html2text
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.ignore_images = False
        h.ignore_tables = False
        h.body_width = 0  # Don't wrap text at a certain width
        
        # Convert HTML to Markdown
        markdown_content = h.handle(html_content)
        
        return markdown_content
    
    def scrape_all_jobs(self, job_links: List[str], max_jobs: int = 500) -> List[Dict]:
        """
        Scrape all discovered job pages and save as HTML and Markdown files
        
        Args:
            job_links (List[str]): List of job URLs to scrape
            max_jobs (int): Maximum number of jobs to scrape (default: 500)
            
        Returns:
            List[Dict]: List of scraped job information
        """
        scraped_jobs = []
        jobs_to_scrape = job_links[:max_jobs]
        
        print(f"\n🔍 Starting to scrape {len(jobs_to_scrape)} job pages...")
        
        # Get current date for folder structure
        today = datetime.datetime.now()
        date_folder = today.strftime("%Y%m%d")
        
        # Create directory structure (with new folder structure: job-data/{YYYYMMDD}/jobscallme/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        date_dir = os.path.join(script_dir, "job-data", date_folder)
        jobscallme_dir = os.path.join(date_dir, "jobscallme")
        html_dir = os.path.join(jobscallme_dir, "html")
        md_dir = os.path.join(jobscallme_dir, "markdown")
        
        # Create directories if they don't exist
        os.makedirs(date_dir, exist_ok=True)
        os.makedirs(jobscallme_dir, exist_ok=True)
        os.makedirs(html_dir, exist_ok=True)
        os.makedirs(md_dir, exist_ok=True)
        
        for i, job_url in enumerate(jobs_to_scrape, 1):
            print(f"\n[{i}/{len(jobs_to_scrape)}] Processing: {job_url}")
            
            result = self.scrape_job_page(job_url)
            if result and isinstance(result, dict) and result.get('html_content'):
                # Check if we should stop crawling due to old job
                if result.get('stop_crawling', False):
                    print(f"🛑 Stopping crawl - encountered job older than 1 month")
                    break
                
                # Extract filename from URL path
                url_path = job_url.split('/job/')[-1] if '/job/' in job_url else f"job_{i}"
                html_filename = f"{url_path}.html"
                md_filename = f"{url_path}.md"
                
                html_content = result['html_content']
                
                # Save HTML content
                html_output_file = os.path.join(html_dir, html_filename)
                with open(html_output_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                # Convert to Markdown and save
                markdown_content = self.html_to_markdown(html_content)
                md_output_file = os.path.join(md_dir, md_filename)
                with open(md_output_file, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                
                # Create job info with metadata
                job_info = {
                    "url": job_url,
                    "html_filename": html_filename,
                    "md_filename": md_filename,
                    "html_length": len(html_content),
                    "md_length": len(markdown_content),
                    "article_found": result.get('article_found', False)
                }
                
                # Add time value if available
                if result.get('time_value'):
                    job_info["time_value"] = result['time_value']
                
                scraped_jobs.append(job_info)
                
                print(f"✅ Saved HTML: {html_filename}")
                print(f"✅ Saved Markdown: {md_filename}")
            else:
                print(f"❌ Failed to extract HTML content from: {job_url}")
        
        print(f"\n📊 Scraping Summary:")
        print(f"   Successfully scraped: {len(scraped_jobs)}/{len(jobs_to_scrape)} jobs")
        
        return scraped_jobs, date_folder
    
    def save_results(self, links: List[str], scraped_jobs: List[Dict] = None, date_folder: str = None):
        """
        Save results to date-based directory structure
        
        Args:
            links (List[str]): List of discovered links
            scraped_jobs (List[Dict]): List of scraped job information
            date_folder (str): Date folder name (format: YYYYMMDD)
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # If date_folder is not provided, use today's date
        if not date_folder:
            today = datetime.datetime.now()
            date_folder = today.strftime("%Y%m%d")
        
        # Create date directory if it doesn't exist
        date_dir = os.path.join(script_dir, "job-data", date_folder)
        os.makedirs(date_dir, exist_ok=True)
        
        # Format current timestamp
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
        
        # Save scraping summary if available
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


def get_base_url() -> str:
    base_url = os.getenv('FIRECRAWL_BASE_URL', 'http://localhost:3002')
    
    if base_url == 'http://localhost:3002':
        print(f"ℹ️  Using default localhost URL: {base_url}")
        print("ℹ️  Set FIRECRAWL_BASE_URL environment variable to override")
    else:
        print(f"ℹ️  Using custom Firecrawl URL: {base_url}")
    
    return base_url


def main():
    base_url = get_base_url()
    
    # Initialize crawler with localhost URL (no API key needed)
    crawler = JobscallMeCrawler(base_url)
    
    # Step 1: Map and discover job links
    print("🚀 Step 1: Mapping website to discover job links...")
    links = crawler.crawl_jobscall_me()
    
    if not links:
        print("❌ Failed to discover job links")
        return
    
    crawler.display_results(links)
    
    # Step 2: Scrape individual job pages
    print("\n🚀 Step 2: Scraping individual job pages...")
    scraped_jobs, date_folder = crawler.scrape_all_jobs(links, max_jobs=500)
    
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