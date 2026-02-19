from google.cloud import storage
import os

BUCKET_NAME = "sqlserver-bq-raw"
LOCAL_DIR = "parquet_output"

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

for root, _, files in os.walk(LOCAL_DIR):
    for file in files:
        if file.endswith(".parquet"):
            local_path = os.path.join(root, file)

            gcs_path = local_path.replace("\\", "/")
            blob = bucket.blob(gcs_path)

            blob.upload_from_filename(local_path)

            print(f"✓ Subido a GCS: {gcs_path}")
