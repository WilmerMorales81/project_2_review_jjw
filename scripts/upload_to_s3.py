# Upload data files to S3，boto 3 is the AWS SDK for Python, which allows you to interact with AWS services like S3. Make sure you have your AWS credentials configured properly (e.g., via environment variables or AWS CLI) before running this script.
# Run: python scripts/upload_to_s3.py

import boto3
import os
from dotenv import load_dotenv

# S3 bucket name
S3_BUCKET = (os.getenv('AWS_BUCKET'))

print("=" * 60)
print("UPLOADING TO S3")
print("=" * 60)

# Files to upload, raw data files and the ZIP-County mapping file we created, mapping to organized paths in S3 for better structure. Make sure these local files exist before running the script.
files = {
    "data/raw/npidata_pfile.csv": "raw/nppes/npidata_pfile.csv",
    "data/raw/nucc_taxonomy_250.csv": "raw/reference/nucc_taxonomy_250.csv",
    "data/raw/ssa_fips_state_county_2025.csv": "raw/reference/ssa_fips_state_county.csv",
    "data/raw/ZIP_COUNTY_032025.xlsx": "raw/geographic/zip_county.xlsx",
}

s3 = boto3.client('s3')

for local, s3_path in files.items():
    if os.path.exists(local):
        print(f"\nUploading: {local}")
        try:
            s3.upload_file(local, S3_BUCKET, s3_path)
            print(f"  ✅ Uploaded to s3://{S3_BUCKET}/{s3_path}")
        except Exception as e:
            print(f"  ❌ Error: {e}")
    else:
        print(f"\n  File not found: {local}")

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
