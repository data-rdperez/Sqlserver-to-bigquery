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
