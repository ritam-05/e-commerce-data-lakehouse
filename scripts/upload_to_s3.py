import os
import sys
import boto3
import logging
from botocore.exceptions import ClientError
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.paths import GOLD_DIR

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Replace with the exact name you used when creating the bucket in the console
BUCKET_NAME = "ritam-ecommerce-lakehouse-gold"
REGION = "ap-southeast-2"  # Locked region based on AWS Sandbox SCP

def upload_directory_to_s3(s3_client, local_dir, s3_prefix=""):
    """Recursively uploads a local directory to S3."""
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            # Skip Spark success flags and hidden files
            if file == "_SUCCESS" or file.startswith("."):
                continue
                
            local_path = os.path.join(root, file)
            
            # Create the S3 key (path inside the bucket)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            logger.info(f"Uploading: {relative_path} -> s3://{BUCKET_NAME}/{s3_key}")
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            except ClientError as e:
                logger.error(f"❌ Upload failed for {file}: {e}")
                sys.exit(1)

def main():
    # Initialize the S3 client for the Sydney region
    s3_client = boto3.client('s3', region_name=REGION)
    
    # We skip create_bucket() here because the SCP blocks it via API.
    # The bucket must be created manually in the AWS Console first.
    
    logger.info(f"\nStarting Gold layer S3 upload to {BUCKET_NAME} in {REGION}...")
    if not os.path.exists(GOLD_DIR):
        logger.error(f"❌ Gold directory not found at {GOLD_DIR}")
        sys.exit(1)
        
    upload_directory_to_s3(s3_client, GOLD_DIR)
    logger.info("\n🎉 S3 Upload Complete! Data is safely in the cloud.")

if __name__ == "__main__":
    main()