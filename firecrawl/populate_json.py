#!/usr/bin/env python3
"""
Populate JSON files - Extract individual job objects from JSON arrays
and save them as separate files.

This script reads all JSON files from the jobscallme/json directory,
extracts each job object from the 'jobs' array, and saves them as
individual JSON files in the final directory.

Usage: python populate_json.py
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


def get_json_files(json_dir: str) -> List[Path]:
    """
    Get all JSON files from the specified directory
    
    Args:
        json_dir (str): Directory containing JSON files
        
    Returns:
        List[Path]: List of JSON file paths
    """
    json_path = Path(json_dir)
    if not json_path.exists():
        print(f"❌ Directory not found: {json_dir}")
        return []
    
    json_files = list(json_path.glob("*.json"))
    print(f"📄 Found {len(json_files)} JSON files")
    return json_files


def process_json_file(json_file: Path, output_dir: Path) -> int:
    """
    Process a single JSON file and extract individual job objects
    
    Args:
        json_file (Path): Path to the JSON file
        output_dir (Path): Output directory for individual files
        
    Returns:
        int: Number of jobs extracted
    """
    try:
        print(f"\n📄 Processing: {json_file.name}")
        
        # Read the JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if the file has a 'jobs' array
        if not isinstance(data, dict) or 'jobs' not in data:
            print(f"⚠️  No 'jobs' array found in {json_file.name}")
            return 0
        
        jobs = data['jobs']
        if not isinstance(jobs, list):
            print(f"⚠️  'jobs' is not an array in {json_file.name}")
            return 0
        
        if len(jobs) == 0:
            print(f"⚠️  No jobs found in {json_file.name}")
            return 0
        
        print(f"✅ Found {len(jobs)} jobs in {json_file.name}")
        
        # Extract base filename (without extension)
        base_name = json_file.stem
        
        # Process each job
        jobs_created = 0
        for i, job in enumerate(jobs, 1):
            # Create individual filename
            individual_filename = f"{base_name}-{i:03d}.json"
            individual_path = output_dir / individual_filename
            
            # Save individual job as JSON
            with open(individual_path, 'w', encoding='utf-8') as f:
                json.dump(job, f, indent=2, ensure_ascii=False)
            
            print(f"   💾 Created: {individual_filename}")
            jobs_created += 1
        
        return jobs_created
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in {json_file.name}: {e}")
        return 0
    except Exception as e:
        print(f"❌ Error processing {json_file.name}: {e}")
        return 0


def populate_individual_json_files(date_str: str = None) -> None:
    """
    Main function to populate individual JSON files
    
    Args:
        date_str (str): Date string in YYYYMMDD format (defaults to today)
    """
    # Use today's date if not specified
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    
    # Define directories
    script_dir = Path(__file__).parent
    json_dir = script_dir / "job-data" / date_str / "jobscallme" / "json"
    final_dir = script_dir / "job-data" / date_str / "jobscallme" / "final"
    
    print(f"🚀 Starting JSON population process...")
    print(f"📁 Source directory: {json_dir}")
    print(f"📁 Output directory: {final_dir}")
    
    # Check if source directory exists
    if not json_dir.exists():
        print(f"❌ Source directory not found: {json_dir}")
        print("📁 Available date directories:")
        job_data_dir = script_dir / "job-data"
        if job_data_dir.exists():
            for item in job_data_dir.iterdir():
                if item.is_dir():
                    print(f"   📅 {item.name}")
        return
    
    # Create output directory
    final_dir.mkdir(parents=True, exist_ok=True)
    print(f"✅ Output directory ready: {final_dir}")
    
    # Get all JSON files
    json_files = get_json_files(str(json_dir))
    if not json_files:
        return
    
    # Process each JSON file
    total_jobs_created = 0
    processed_files = 0
    
    for json_file in json_files:
        jobs_created = process_json_file(json_file, final_dir)
        total_jobs_created += jobs_created
        if jobs_created > 0:
            processed_files += 1
    
    # Summary
    print(f"\n" + "=" * 60)
    print(f"🎉 POPULATION COMPLETE")
    print(f"✅ Processed files: {processed_files}/{len(json_files)}")
    print(f"✅ Total individual job files created: {total_jobs_created}")
    print(f"📁 Individual files saved in: {final_dir}")
    
    if total_jobs_created > 0:
        print(f"\n📊 Check the individual JSON files in:")
        print(f"   {final_dir}")


def main():
    """Main function with command line support"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract individual job objects from JSON arrays",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process JSON files from today's directory
  python populate_json.py

  # Process JSON files from a specific date
  python populate_json.py --date 20250828
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        type=str,
        help='Date in YYYYMMDD format (default: today)'
    )
    
    args = parser.parse_args()
    
    # Run the population process
    populate_individual_json_files(args.date)


if __name__ == "__main__":
    main()
