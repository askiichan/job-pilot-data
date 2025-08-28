"""
Upload JSON files from job-data directory to Cloudflare R2 storage.

This script uploads all JSON files from the firecrawl/job-data/20250828/jobscallme/final
directory to Cloudflare R2 storage, organizing them in a folder named with today's date
in YYYYMMDD format.

Requirements:
- Cloudflare R2 credentials (Access Key ID, Secret Access Key, Account ID)
- boto3 library for S3-compatible API
"""

import os
import boto3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import sys
from botocore.exceptions import ClientError, NoCredentialsError


class R2Uploader:
    """Handles uploading files to Cloudflare R2 storage."""
    
    def __init__(self, account_id: str, access_key_id: str, secret_access_key: str, bucket_name: str):
        """
        Initialize R2 uploader.
        
        Args:
            account_id: Cloudflare account ID
            access_key_id: R2 access key ID
            secret_access_key: R2 secret access key
            bucket_name: R2 bucket name
        """
        self.account_id = account_id
        self.bucket_name = bucket_name
        
        # Create S3 client for R2
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name='auto'  # R2 uses 'auto' region
        )
    
    def test_connection(self) -> bool:
        """Test connection to R2 bucket."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✅ Successfully connected to R2 bucket: {self.bucket_name}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"❌ Bucket '{self.bucket_name}' not found")
            elif error_code == '403':
                print(f"❌ Access denied to bucket '{self.bucket_name}'")
            else:
                print(f"❌ Error accessing bucket: {e}")
            return False
        except NoCredentialsError:
            print("❌ No credentials found. Please check your access keys.")
            return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def get_today_folder(self) -> str:
        """Get today's date in YYYYMMDD format for folder naming."""
        return datetime.now().strftime("%Y%m%d")
    
    def find_json_files(self, directory_path: str) -> List[Path]:
        """
        Find all JSON files in the specified directory.
        
        Args:
            directory_path: Path to search for JSON files
            
        Returns:
            List of Path objects for JSON files
        """
        directory = Path(directory_path)
        if not directory.exists():
            print(f"❌ Directory not found: {directory_path}")
            return []
        
        json_files = list(directory.glob("*.json"))
        print(f"📁 Found {len(json_files)} JSON files in {directory_path}")
        return json_files
    
    def upload_file(self, file_path: Path, folder_name: str) -> bool:
        """
        Upload a single file to R2 storage.
        
        Args:
            file_path: Local file path
            folder_name: Remote folder name (date folder)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create the S3 key (remote path)
            s3_key = f"{folder_name}/{file_path.name}"
            
            # Upload the file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'Metadata': {
                        'source': 'jobscallme-firecrawl',
                        'upload_date': datetime.now().isoformat()
                    }
                }
            )
            
            print(f"✅ Uploaded: {file_path.name} -> {s3_key}")
            return True
            
        except ClientError as e:
            print(f"❌ Failed to upload {file_path.name}: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error uploading {file_path.name}: {e}")
            return False
    
    def upload_all_json_files(self, source_directory: str) -> Tuple[int, int]:
        """
        Upload all JSON files from source directory to R2.
        
        Args:
            source_directory: Local directory containing JSON files
            
        Returns:
            Tuple of (successful_uploads, total_files)
        """
        # Get today's folder name
        folder_name = self.get_today_folder()
        print(f"📅 Using folder name: {folder_name}")
        
        # Find all JSON files
        json_files = self.find_json_files(source_directory)
        if not json_files:
            print("❌ No JSON files found to upload")
            return 0, 0
        
        # Upload each file
        successful_uploads = 0
        total_files = len(json_files)
        
        print(f"\n🚀 Starting upload of {total_files} files...")
        print("-" * 50)
        
        for i, file_path in enumerate(json_files, 1):
            print(f"[{i}/{total_files}] ", end="")
            if self.upload_file(file_path, folder_name):
                successful_uploads += 1
        
        print("-" * 50)
        print(f"📊 Upload Summary:")
        print(f"   ✅ Successful: {successful_uploads}")
        print(f"   ❌ Failed: {total_files - successful_uploads}")
        print(f"   📁 Remote folder: {folder_name}")
        
        return successful_uploads, total_files


def get_credentials_from_env() -> Tuple[str, str, str, str]:
    """
    Get R2 credentials from environment variables.
    
    Returns:
        Tuple of (account_id, access_key_id, secret_access_key, bucket_name)
    """
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME')
    
    missing_vars = []
    if not account_id:
        missing_vars.append('R2_ACCOUNT_ID')
    if not access_key_id:
        missing_vars.append('R2_ACCESS_KEY_ID')
    if not secret_access_key:
        missing_vars.append('R2_SECRET_ACCESS_KEY')
    if not bucket_name:
        missing_vars.append('R2_BUCKET_NAME')
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables and try again.")
        print("Example:")
        print("set R2_ACCOUNT_ID=your_account_id")
        print("set R2_ACCESS_KEY_ID=your_access_key")
        print("set R2_SECRET_ACCESS_KEY=your_secret_key")
        print("set R2_BUCKET_NAME=your_bucket_name")
        return None, None, None, None
    
    return account_id, access_key_id, secret_access_key, bucket_name


def main():
    """Main function to run the upload process."""
    print("🚀 Cloudflare R2 JSON File Uploader")
    print("=" * 40)
    
    # Get credentials
    account_id, access_key_id, secret_access_key, bucket_name = get_credentials_from_env()
    if not all([account_id, access_key_id, secret_access_key, bucket_name]):
        sys.exit(1)
    
    # Initialize uploader
    uploader = R2Uploader(account_id, access_key_id, secret_access_key, bucket_name)
    
    # Test connection
    if not uploader.test_connection():
        sys.exit(1)
    
    # Define source directory
    source_dir = "firecrawl/job-data/20250828/jobscallme/final"
    
    # Check if source directory exists
    if not os.path.exists(source_dir):
        print(f"❌ Source directory not found: {source_dir}")
        print("Please ensure the directory exists and contains JSON files.")
        sys.exit(1)
    
    # Upload files
    successful, total = uploader.upload_all_json_files(source_dir)
    
    if successful == total:
        print(f"\n🎉 All {total} files uploaded successfully!")
        sys.exit(0)
    elif successful > 0:
        print(f"\n⚠️  Partial success: {successful}/{total} files uploaded")
        sys.exit(1)
    else:
        print(f"\n💥 Upload failed: 0/{total} files uploaded")
        sys.exit(1)


if __name__ == "__main__":
    main()
