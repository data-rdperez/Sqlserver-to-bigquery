from google.cloud import bigquery
import os
from config import DATABASE_TABLES

PROJECT_ID = "data-zinergia"
BUCKET = "sqlserver-bq-raw"

client = bigquery.Client(project=PROJECT_ID)

def ensure_dataset(dataset_id):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

for db, tables in DATABASE_TABLES.items():
    dataset_id = db.lower()  # dwh_splgnmx
    ensure_dataset(dataset_id)

    for table in tables:
        table_name = table.replace(".", "_").lower()
        uri = f"gs://{BUCKET}/parquet_output/{db}/{table.replace('.', '_')}.parquet"

        table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE"
        )

        print(f"→ Cargando {uri} a {table_id}")

        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        load_job.result()
        print(f"✓ Cargado {table_id}")
from google.cloud import bigquery
import os

PROJECT_ID = "data-zinergia"
BUCKET_NAME = "sqlserver-bq-raw"

client = bigquery.Client(project=PROJECT_ID)

def ensure_dataset(dataset_name):
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

for root, _, files in os.walk("parquet_output"):
    for file in files:
        if not file.endswith(".parquet"):
            continue

        # parquet_output/DWH_SPLGNMX/dbo_xxx.parquet
        parts = root.replace("parquet_output\\", "").split("\\")
        sql_db = parts[0]                      # DWH_SPLGNMX
        dataset_name = sql_db.lower()

        ensure_dataset(dataset_name)

        table_name = file.replace(".parquet", "")
        table_id = f"{PROJECT_ID}.{dataset_name}.{table_name}"

        gcs_uri = f"gs://{BUCKET_NAME}/parquet_output/{sql_db}/{file}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        load_job.result()

        print(f"✓ Cargado: {table_id}")
