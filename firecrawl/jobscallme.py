import json
import os
from typing import Dict, List, Optional
from firecrawl import Firecrawl


class JobscallMeCrawler:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.firecrawl = Firecrawl(api_key=api_key)
    
    def map_website(self, url: str) -> Optional[Dict]:
        try:
            print(f"🔍 Mapping website: {url}")
            map_result = self.firecrawl.map(url=url, limit=100)
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
        
        for link in all_links:
            if '/job/' in link:
                job_part = link.split('/job/')[-1]
                
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
    
    def save_results(self, links: List[str]):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "jobscall_me_links.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "url": "https://www.jobscall.me/job",
                "timestamp": "2024-01-01",
                "total_links": len(links),
                "links": links
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: {output_file}")


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
    links = crawler.crawl_jobscall_me()
    
    if links:
        crawler.display_results(links)
        crawler.save_results(links)
    else:
        print("❌ Failed to crawl the website")


if __name__ == "__main__":
    main()