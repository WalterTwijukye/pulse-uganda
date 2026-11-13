"""
init_storage.py: Initializes and verifies the required S3/RustFS buckets for the Medallion Lakehouse.
Idempotent: If a bucket already exists, it reports it safely without throwing an error.
"""
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# 1. Load credentials from .env in project root
load_dotenv()

# 2. Retrieve credentials and target endpoint
ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# 3. Initialize S3 client using boto3
# Note: region_name='us-east-1' is a required dummy region for S3 client compatibility
s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

# 4. Define our standard Medallion architecture buckets
REQUIRED_BUCKETS = ["de-bronze", "de-silver", "de-gold", "de-quarantine"]


def ensure_buckets():
    print(f"Connecting to S3 storage at {ENDPOINT_URL}...")
    try:
        response = s3_client.list_buckets()
        existing_buckets = [b["Name"] for b in response.get("Buckets", [])]
        print(f"Existing buckets currently on server: {existing_buckets}\n")
    except Exception as e:
        print(f"❌ Failed to connect to S3 endpoint: {e}")
        return

    for bucket in REQUIRED_BUCKETS:
        if bucket not in existing_buckets:
            try:
                s3_client.create_bucket(Bucket=bucket)
                print(f"✅ Created bucket: {bucket}")
            except ClientError as e:
                print(f"❌ Failed to create bucket {bucket}: {e}")
        else:
            print(f"ℹ️ Bucket already exists: {bucket}")

    print("\nStorage initialization check complete.")


if __name__ == "__main__":
    ensure_buckets()
