# Cloudflare R2 Upload Setup Guide

This guide will help you set up and use the `upload.py` script to upload JSON files from your job data to Cloudflare R2 storage.

## Prerequisites

1. **Cloudflare Account**: You need a Cloudflare account with R2 enabled
2. **R2 Bucket**: Create an R2 bucket in your Cloudflare dashboard
3. **API Credentials**: Generate R2 API tokens with appropriate permissions

## Step 1: Install Dependencies

First, install the required Python packages:

```bash
pip install -r requirements.txt
```

## Step 2: Create R2 Bucket and Get Credentials

### Creating an R2 Bucket
1. Log in to your [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to **R2 Object Storage**
3. Click **Create bucket**
4. Choose a unique bucket name (e.g., `job-pilot-data`)
5. Select your preferred location
6. Click **Create bucket**

### Getting Your Account ID
1. In the Cloudflare dashboard, look at the right sidebar
2. Your **Account ID** is displayed there
3. Copy this value - you'll need it for `R2_ACCOUNT_ID`

### Creating API Tokens
1. In the Cloudflare dashboard, go to **My Profile** > **API Tokens**
2. Click **Create Token**
3. Choose **Custom token**
4. Configure the token:
   - **Token name**: `R2 Upload Token`
   - **Permissions**: 
     - `Cloudflare R2:Edit` (for your account)
   - **Account resources**: 
     - Include your account
   - **Zone resources**: 
     - Not needed for R2
5. Click **Continue to summary**
6. Click **Create Token**
7. **Important**: Copy the token immediately - you won't see it again!

### Getting Access Keys
1. Go to **R2 Object Storage** in your dashboard
2. Click **Manage R2 API tokens**
3. Click **Create API token**
4. Configure:
   - **Token name**: `Upload Token`
   - **Permissions**: `Object Read & Write`
   - **Specify bucket**: Select your bucket
   - **TTL**: Choose appropriate expiration
5. Click **Create API token**
6. Copy both the **Access Key ID** and **Secret Access Key**

## Step 3: Set Environment Variables

Set the following environment variables with your credentials:

### Windows (Command Prompt)
```cmd
set R2_ACCOUNT_ID=your_account_id_here
set R2_ACCESS_KEY_ID=your_access_key_id_here
set R2_SECRET_ACCESS_KEY=your_secret_access_key_here
set R2_BUCKET_NAME=your_bucket_name_here
```

### Windows (PowerShell)
```powershell
$env:R2_ACCOUNT_ID="your_account_id_here"
$env:R2_ACCESS_KEY_ID="your_access_key_id_here"
$env:R2_SECRET_ACCESS_KEY="your_secret_access_key_here"
$env:R2_BUCKET_NAME="your_bucket_name_here"
```

### Linux/Mac
```bash
export R2_ACCOUNT_ID="your_account_id_here"
export R2_ACCESS_KEY_ID="your_access_key_id_here"
export R2_SECRET_ACCESS_KEY="your_secret_access_key_here"
export R2_BUCKET_NAME="your_bucket_name_here"
```

### Using .env file (Alternative)
Create a `.env` file in the project root:
```env
R2_ACCOUNT_ID=your_account_id_here
R2_ACCESS_KEY_ID=your_access_key_id_here
R2_SECRET_ACCESS_KEY=your_secret_access_key_here
R2_BUCKET_NAME=your_bucket_name_here
```

## Step 4: Run the Upload Script

Navigate to the project directory and run:

```bash
cd firecrawl
python upload.py
```

## What the Script Does

1. **Connects to R2**: Tests connection to your R2 bucket
2. **Finds JSON files**: Scans `firecrawl/job-data/20250828/jobscallme/final/` for JSON files
3. **Creates date folder**: Uses today's date in YYYYMMDD format as the folder name
4. **Uploads files**: Uploads each JSON file to R2 with progress tracking
5. **Reports results**: Shows upload summary with success/failure counts

## Expected Output

```
🚀 Cloudflare R2 JSON File Uploader
========================================
✅ Successfully connected to R2 bucket: your-bucket-name
📁 Found 450 JSON files in firecrawl/job-data/20250828/jobscallme/final
📅 Using folder name: 20250128

🚀 Starting upload of 450 files...
--------------------------------------------------
[1/450] ✅ Uploaded: 369cooptown-admin-001.json -> 20250128/369cooptown-admin-001.json
[2/450] ✅ Uploaded: adreamvet-001.json -> 20250128/adreamvet-001.json
[3/450] ✅ Uploaded: adreamvet-002.json -> 20250128/adreamvet-002.json
...
--------------------------------------------------
📊 Upload Summary:
   ✅ Successful: 450
   ❌ Failed: 0
   📁 Remote folder: 20250128

🎉 All 450 files uploaded successfully!
```

## File Organization in R2

Your files will be organized in R2 as:
```
your-bucket-name/
├── 20250128/           # Today's date folder
│   ├── 369cooptown-admin-001.json
│   ├── adreamvet-001.json
│   ├── adreamvet-002.json
│   └── ... (all JSON files)
├── 20250129/           # Next day's uploads (if any)
│   └── ...
```

## Troubleshooting

### Connection Issues
- **403 Forbidden**: Check your access keys and bucket permissions
- **404 Not Found**: Verify your bucket name and account ID
- **Invalid credentials**: Ensure environment variables are set correctly

### Upload Issues
- **No files found**: Verify the source directory path exists
- **Permission denied**: Check your API token has write permissions
- **Network errors**: Check your internet connection

### Common Solutions
1. **Double-check credentials**: Ensure all environment variables are correct
2. **Bucket permissions**: Verify your API token has access to the bucket
3. **File paths**: Ensure the source directory contains JSON files
4. **Network**: Check firewall/proxy settings if having connection issues

## Security Best Practices

1. **Rotate tokens**: Regularly rotate your API tokens
2. **Minimal permissions**: Only grant necessary permissions to tokens
3. **Environment variables**: Never commit credentials to version control
4. **Token expiry**: Set appropriate expiration dates for tokens

## Next Steps

After successful upload, you can:
1. View your files in the Cloudflare R2 dashboard
2. Set up custom domains for public access
3. Configure lifecycle policies for cost optimization
4. Integrate with other applications using the R2 API

For more information, visit the [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/).
