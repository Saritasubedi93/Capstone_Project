# config/settings.py

# S3 bucket + prefixes
BUCKET_NAME = "capstoneproj-sarita"

S3_BASE = f"s3a://{BUCKET_NAME}"
INPUT_PREFIX = f"{S3_BASE}/input-data"
BRONZE_PREFIX = f"{S3_BASE}/bronze"
SILVER_PREFIX = f"{S3_BASE}/silver"
GOLD_PREFIX = f"{S3_BASE}/gold"

# Spark app names (these are what you're importing)
APP_NAME_BRONZE = "Capstone-Bronze-Ingestion"
APP_NAME_SILVER = "Capstone-Silver-Cleaning"



