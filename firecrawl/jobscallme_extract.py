import os
import argparse
from google import genai
from pydantic import BaseModel
from typing import Optional
import json
from datetime import datetime
from pathlib import Path

# Define the structured output model for a single job
class JobData(BaseModel):
    job_title: str
    company_name: str
    post_date: str
    job_description: Optional[str]
    job_requirement: Optional[str]

# Define the structured output model for multiple jobs
class JobsExtraction(BaseModel):
    jobs: list[JobData]
    total_jobs: int

def extract_job_data(markdown_file_path: str, api_key: str, save_json: bool = True) -> JobsExtraction:
    """
    Extract structured job data from markdown file using Gemini API
    
    Args:
        markdown_file_path: Path to the markdown file containing job posting(s)
        api_key: Gemini API key
        save_json: Whether to save the extracted data as JSON file (default: True)
        
    Returns:
        JobsExtraction: Structured job data containing list of jobs
    """
    # Read the markdown file
    with open(markdown_file_path, 'r', encoding='utf-8') as file:
        markdown_content = file.read()
    
    # Initialize Gemini client
    client = genai.Client(api_key=api_key)
    
    # Create the prompt for extraction
    prompt = f"""
    Extract job information from the following markdown content. The content may contain multiple job postings.
    
    For each job found, extract:
    - job_title: The main job title (include both English and Chinese if available)
    - company_name: The company name
    - post_date: The posting date in YYYY-MM-DD format
    - job_description: The job description section (if available)
    - job_requirement: All requirements listed for the job (preserve formatting with bullet points)
    
    Return a list of all jobs found in the document. If there's only one job, return a list with one item.
    Set total_jobs to the number of jobs found.
    
    Markdown content:
    {markdown_content}
    """
    
    # Generate structured response
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": JobsExtraction,
        },
    )
    
    # Parse the response
    jobs_data: JobsExtraction = response.parsed
    
    # Save as JSON if requested
    if save_json:
        save_job_data_as_json(jobs_data, markdown_file_path)
    
    return jobs_data

def save_job_data_as_json(jobs_data: JobsExtraction, markdown_file_path: str) -> str:
    """
    Save job data as JSON file in the specified directory structure
    
    Args:
        jobs_data: The extracted jobs data
        markdown_file_path: Original markdown file path
        
    Returns:
        str: Path to the saved JSON file
    """
    # Get today's date in YYYYMMDD format
    today = datetime.now().strftime("%Y%m%d")
    
    # Get the filename without extension from the markdown file
    md_filename = Path(markdown_file_path).stem
    
    # Create the output directory structure (relative to firecrawl directory)
    output_dir = Path("firecrawl/job-data") / today / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create the output file path
    json_filename = f"{md_filename}.json"
    output_path = output_dir / json_filename
    
    # Save the JSON data
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(jobs_data.model_dump(), f, indent=2, ensure_ascii=False)
    
    print(f"✅ JSON saved to: {output_path}")
    return str(output_path)

def process_multiple_files(markdown_dir: str, api_key: str) -> list[str]:
    """
    Process multiple markdown files from a directory
    
    Args:
        markdown_dir: Directory containing markdown files
        api_key: Gemini API key
        
    Returns:
        list[str]: List of JSON file paths that were created
    """
    markdown_path = Path(markdown_dir)
    if not markdown_path.exists():
        raise ValueError(f"Directory not found: {markdown_dir}")
    
    json_files = []
    md_files = list(markdown_path.glob("*.md"))
    
    if not md_files:
        print(f"⚠️  No markdown files found in {markdown_dir}")
        return json_files
    
    print(f"🔄 Processing {len(md_files)} markdown files...")
    
    for md_file in md_files:
        try:
            print(f"\n📄 Processing: {md_file.name}")
            job_data = extract_job_data(str(md_file), api_key)
            
            # The JSON file path is built in save_job_data_as_json
            today = datetime.now().strftime("%Y%m%d")
            json_path = f"job-data/{today}/json/{md_file.stem}.json"
            json_files.append(json_path)
            
        except Exception as e:
            print(f"❌ Error processing {md_file.name}: {e}")
    
    print(f"\n🎉 Successfully processed {len(json_files)} files!")
    return json_files

def process_batch_files(date_str: str = None, api_key: str = None) -> None:
    """
    Process all markdown files in a date directory
    
    Args:
        date_str: Date string in YYYYMMDD format (defaults to today)
        api_key: Gemini API key
    """
    if not api_key:
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            print("❌ Please set GEMINI_API_KEY environment variable")
            print("Example: set GEMINI_API_KEY=your_api_key_here")
            return
    
    # Use today's date if not specified
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    
    # Directory containing markdown files (relative to firecrawl directory)
    markdown_dir = f"firecrawl/job-data/{date_str}/markdown"
    
    # Check if the directory exists
    if not os.path.exists(markdown_dir):
        print(f"❌ Markdown directory not found: {markdown_dir}")
        print("📁 Available directories:")
        job_data_dir = "firecrawl/job-data"
        if os.path.exists(job_data_dir):
            for item in os.listdir(job_data_dir):
                item_path = os.path.join(job_data_dir, item)
                if os.path.isdir(item_path):
                    print(f"   📅 {item}")
        return
    
    # Find all markdown files in the directory
    markdown_path = Path(markdown_dir)
    md_files = list(markdown_path.glob("*.md"))
    
    if not md_files:
        print(f"❌ No markdown files found in: {markdown_dir}")
        return
    
    print("🚀 Starting batch job data extraction...")
    print(f"📁 Processing directory: {markdown_dir}")
    print(f"📄 Found {len(md_files)} markdown files")
    print("-" * 60)
    
    # Process all files
    successful_extractions = 0
    failed_extractions = 0
    
    for i, md_file in enumerate(md_files, 1):
        print(f"\n[{i}/{len(md_files)}] 📄 Processing: {md_file.name}")
        
        try:
            # Extract job data (this will automatically save JSON file)
            jobs_data = extract_job_data(str(md_file), api_key)
            
            print(f"   ✅ Success: Found {jobs_data.total_jobs} job(s)")
            if jobs_data.jobs:
                for j, job in enumerate(jobs_data.jobs, 1):
                    print(f"      {j}. {job.job_title} @ {job.company_name}")
            successful_extractions += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            failed_extractions += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("🎉 BATCH PROCESSING COMPLETE")
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"💾 JSON files saved to: firecrawl/job-data/{date_str}/json/")
    
    if successful_extractions > 0:
        print(f"\n📊 Check the JSON files in: firecrawl/job-data/{date_str}/json/")

def process_single_file(file_path: str, api_key: str = None) -> None:
    """
    Process a single markdown file
    
    Args:
        file_path: Path to the markdown file
        api_key: Gemini API key
    """
    if not api_key:
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            print("❌ Please set GEMINI_API_KEY environment variable")
            return
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    print("🚀 Processing single file...")
    print(f"📄 Processing file: {file_path}")
    
    try:
        # Extract job data (this will also save the JSON file automatically)
        jobs_data = extract_job_data(file_path, api_key)
        
        # Print the extracted data as JSON
        print(f"\n📊 Extracted {jobs_data.total_jobs} Job(s):")
        print("-" * 50)
        print(json.dumps(jobs_data.model_dump(), indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"❌ Error extracting job data: {e}")

def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(
        description="Extract structured job data from markdown files using Gemini API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples (run from project root directory):
  # Process all markdown files from today's directory
  python firecrawl/jobscallme_extract.py

  # Process all markdown files from a specific date
  python firecrawl/jobscallme_extract.py --date 20250828

  # Process a single file
  python firecrawl/jobscallme_extract.py --file firecrawl/job-data/20250828/markdown/bigfour-admin.md

  # Process with custom API key
  python firecrawl/jobscallme_extract.py --api-key YOUR_API_KEY
        """
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        help='Process a single markdown file'
    )
    
    parser.add_argument(
        '--date', '-d',
        type=str,
        help='Date in YYYYMMDD format (default: today)'
    )
    
    parser.add_argument(
        '--api-key', '-k',
        type=str,
        help='Gemini API key (default: from GEMINI_API_KEY env var)'
    )
    
    args = parser.parse_args()
    
    # Get API key from args or environment
    api_key = args.api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("❌ Please provide API key via --api-key or set GEMINI_API_KEY environment variable")
        return
    
    if args.file:
        # Process single file
        process_single_file(args.file, api_key)
    else:
        # Process batch files
        process_batch_files(args.date, api_key)

if __name__ == "__main__":
    main()
