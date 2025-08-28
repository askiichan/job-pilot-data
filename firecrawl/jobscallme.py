import json
import os
from typing import Dict, List, Optional
from firecrawl import FirecrawlApp

class JobscallMeCrawler:
    def __init__(self, api_key: str):
        """
        Initialize the Firecrawl crawler for jobscall.me
        
        Args:
            api_key (str): Your Firecrawl API key
        """
        self.api_key = api_key
        self.firecrawl = FirecrawlApp(api_key=api_key)
    
    def map_website(self, url: str) -> Optional[Dict]:
        """
        Use Firecrawl's map feature to discover all links on a website
        
        Args:
            url (str): The URL to map
            
        Returns:
            Dict: Response from Firecrawl API containing discovered links
        """
        try:
            print(f"🔍 Mapping website: {url}")
            
            # Use the SDK's map method
            map_result = self.firecrawl.map_url(url)
            
            print(f"✅ Successfully mapped website")
            return map_result
                
        except Exception as e:
            print(f"❌ Request failed: {str(e)}")
            return None
    
    def crawl_jobscall_me(self) -> Optional[List[str]]:
        """
        Crawl all links under https://www.jobscall.me/job
        
        Returns:
            List[str]: List of discovered URLs
        """
        target_url = "https://www.jobscall.me/job"
        
        # Map the website to discover all links
        map_result = self.map_website(target_url)
        
        if not map_result:
            return None
        
        # Extract links from the response
        all_links = []
        if map_result and 'links' in map_result:
            all_links = map_result['links']
        elif map_result and isinstance(map_result, list):
            all_links = map_result
        
        # Filter to only include direct job links (job/xxx) but not nested paths (job/xxx/xxx)
        # Also exclude category and tag links
        links = []
        for link in all_links:
            if '/job/' in link:
                # Extract the part after /job/
                job_part = link.split('/job/')[-1]
                
                # Only include if:
                # 1. It's a direct job link (no additional slashes after /job/xxx)
                # 2. It's not a category link (/job/category/...)
                # 3. It's not a tag link (/job/tag/...)
                # 4. It's not empty or just "job"
                if (job_part and 
                    '/' not in job_part and 
                    not link.endswith('/job/') and
                    '/job/category/' not in link and
                    '/job/tag/' not in link):
                    links.append(link)
        
        print(f"\n📊 Discovery Summary:")
        print(f"   Total links found: {len(links)}")
        
        return links
    
    def display_results(self, links: List[str]):
        """
        Display the discovered links in a formatted way
        
        Args:
            links (List[str]): List of URLs to display
        """
        if not links:
            print("❌ No links discovered")
            return
        
        print(f"\n🔗 Discovered Links ({len(links)} total):")
        print("=" * 60)
        
        for i, link in enumerate(links, 1):
            print(f"{i:3d}. {link}")
        
        # Group by type if possible
        job_links = [link for link in links if '/job/' in link or 'job' in link.lower()]
        if job_links:
            print(f"\n💼 Job-related links ({len(job_links)}):")
            print("-" * 40)
            for i, link in enumerate(job_links, 1):
                print(f"{i:3d}. {link}")

def main():
    """
    Main function to run the jobscall.me crawler
    """
    # Get API key from environment variable or prompt user
    api_key = os.getenv('FIRECRAWL_API_KEY')
    
    if not api_key:
        print("⚠️  FIRECRAWL_API_KEY environment variable not found")
        api_key = input("Please enter your Firecrawl API key: ").strip()
        
        if not api_key:
            print("❌ API key is required. Exiting...")
            return
    
    # Initialize crawler
    crawler = JobscallMeCrawler(api_key)
    
    # Crawl the website
    links = crawler.crawl_jobscall_me()
    
    if links:
        crawler.display_results(links)
        
        # Save results to file in the firecrawl folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "jobscall_me_links.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "url": "https://www.jobscall.me/job",
                "timestamp": "2024-01-01",  # You can use datetime.now()
                "total_links": len(links),
                "links": links
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: {output_file}")
    else:
        print("❌ Failed to crawl the website")

if __name__ == "__main__":
    main()
