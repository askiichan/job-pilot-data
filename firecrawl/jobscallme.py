import json
import os
from typing import Dict, List, Optional
from firecrawl import Firecrawl
from bs4 import BeautifulSoup


class JobscallMeCrawler:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.firecrawl = Firecrawl(api_key=api_key)
    
    def map_website(self, url: str) -> Optional[Dict]:
        try:
            print(f"🔍 Mapping website: {url}")
            map_result = self.firecrawl.map(url=url, limit=5)
            print(f"✅ Successfully mapped website")
            return map_result
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
                            else:
                                print(f"⚠️ No <time> tag found within article")
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
                
        except Exception as e:
            print(f"❌ Failed to scrape {url}: {str(e)}")
            return None
    
    def scrape_all_jobs(self, job_links: List[str], max_jobs: int = 10) -> List[Dict]:
        """
        Scrape all discovered job pages and save as HTML files
        
        Args:
            job_links (List[str]): List of job URLs to scrape
            max_jobs (int): Maximum number of jobs to scrape (default: 10)
            
        Returns:
            List[Dict]: List of scraped job information
        """
        scraped_jobs = []
        jobs_to_scrape = job_links[:max_jobs]
        
        print(f"\n🔍 Starting to scrape {len(jobs_to_scrape)} job pages...")
        
        for i, job_url in enumerate(jobs_to_scrape, 1):
            print(f"\n[{i}/{len(jobs_to_scrape)}] Processing: {job_url}")
            
            result = self.scrape_job_page(job_url)
            if result and isinstance(result, dict) and result.get('html_content'):
                # Extract filename from URL path
                url_path = job_url.split('/job/')[-1] if '/job/' in job_url else f"job_{i}"
                filename = f"{url_path}.html"
                
                # Create job-data folder and save HTML content
                script_dir = os.path.dirname(os.path.abspath(__file__))
                job_data_dir = os.path.join(script_dir, "job-data")
                
                # Create job-data directory if it doesn't exist
                if not os.path.exists(job_data_dir):
                    os.makedirs(job_data_dir)
                
                output_file = os.path.join(job_data_dir, filename)
                
                html_content = result['html_content']
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                # Create job info with metadata
                job_info = {
                    "url": job_url,
                    "filename": filename,
                    "content_length": len(html_content),
                    "article_found": result.get('article_found', False)
                }
                
                # Add time value if available
                if result.get('time_value'):
                    job_info["time_value"] = result['time_value']
                
                scraped_jobs.append(job_info)
                
                print(f"✅ Saved: {filename}")
            else:
                print(f"❌ Failed to extract HTML content from: {job_url}")
        
        print(f"\n📊 Scraping Summary:")
        print(f"   Successfully scraped: {len(scraped_jobs)}/{len(jobs_to_scrape)} jobs")
        
        return scraped_jobs
    
    def save_results(self, links: List[str], scraped_jobs: List[Dict] = None):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Save discovered links
        links_file = os.path.join(script_dir, "jobscall_me_links.json")
        with open(links_file, 'w', encoding='utf-8') as f:
            json.dump({
                "url": "https://www.jobscall.me/job",
                "timestamp": "2024-01-01",
                "total_links": len(links),
                "links": links
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Links saved to: {links_file}")
        
        # Save scraping summary if available
        if scraped_jobs:
            summary_file = os.path.join(script_dir, "scraping_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "source": "jobscall.me",
                    "timestamp": "2024-01-01",
                    "total_jobs": len(scraped_jobs),
                    "scraped_jobs": scraped_jobs
                }, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Scraping summary saved to: {summary_file}")
            print(f"📁 HTML files saved in: {os.path.join(script_dir, 'job-data')}")


def get_api_key() -> Optional[str]:
    api_key = os.getenv('FIRECRAWL_API_KEY')
    
    if not api_key:
        print("⚠️  FIRECRAWL_API_KEY environment variable not found")
        api_key = input("Please enter your Firecrawl API key: ").strip()
        
        if not api_key:
            print("❌ API key is required. Exiting...")
            return None
    
    return api_key


def main():
    api_key = get_api_key()
    if not api_key:
        return
    
    crawler = JobscallMeCrawler(api_key)
    
    # Step 1: Map and discover job links
    print("🚀 Step 1: Mapping website to discover job links...")
    links = crawler.crawl_jobscall_me()
    
    if not links:
        print("❌ Failed to discover job links")
        return
    
    crawler.display_results(links)
    
    # Step 2: Scrape individual job pages
    print("\n🚀 Step 2: Scraping individual job pages...")
    scraped_jobs = crawler.scrape_all_jobs(links, max_jobs=10)
    
    # Step 3: Save results
    print("\n🚀 Step 3: Saving results...")
    crawler.save_results(links, scraped_jobs)
    
    print(f"\n✅ Crawling completed successfully!")
    print(f"📊 Summary:")
    print(f"   - Discovered links: {len(links)}")
    print(f"   - Successfully scraped: {len(scraped_jobs)} jobs")


if __name__ == "__main__":
    main()