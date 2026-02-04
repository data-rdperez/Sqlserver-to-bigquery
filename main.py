import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from config import (
    SERVER,
    USER,
    PASSWORD,
    DATABASE_TABLES,
    OUTPUT_BASE
)

os.makedirs(OUTPUT_BASE, exist_ok=True)

def connect(database):
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
    )

for db, tables in DATABASE_TABLES.items():
    print(f"\n=== Base de datos: {db} ===")

    conn = connect(db)

    for table in tables:
        print(f"  → Extrayendo {table}")

        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)

            db_dir = os.path.join(OUTPUT_BASE, db)
            os.makedirs(db_dir, exist_ok=True)

            parquet_path = os.path.join(
                db_dir,
                f"{table.replace('.', '_')}.parquet"
            )

            pq.write_table(pa.Table.from_pandas(df), parquet_path)

            print(f"    ✓ Guardado {parquet_path}")

        except Exception as e:
            print(f"    ✗ Error en {table}: {e}")

    conn.close()
