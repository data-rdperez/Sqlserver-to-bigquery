import os
import pandas as pd
import pyodbc
from config import OUTPUT_BASE

SERVER = os.environ["SQL_SERVER"]
USER = os.environ["SQL_USER"]
PASSWORD = os.environ["SQL_PASSWORD"]

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};UID={USER};PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)

def get_dwh_databases(conn):
    query = """
    SELECT name
    FROM sys.databases
    WHERE name LIKE 'DWH%'
      AND state_desc = 'ONLINE'
    """
    return pd.read_sql(query, conn)["name"].tolist()

def get_tables_and_views(conn, db):
    query = f"""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM [{db}].INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    """
    return pd.read_sql(query, conn)

def extract_table(conn, db, schema, table):
    full_name = f"[{db}].[{schema}].[{table}]"
    print(f"→ Extrayendo {full_name}")

    df = pd.read_sql(f"SELECT * FROM {full_name}", conn)

    out_dir = f"{OUTPUT_BASE}/{db}"
    os.makedirs(out_dir, exist_ok=True)

    file_path = f"{out_dir}/{schema}_{table}.parquet"
    df.to_parquet(file_path, index=False)

    print(f"✓ Guardado {file_path}")

def main():
    with pyodbc.connect(CONN_STR) as conn:
        databases = get_dwh_databases(conn)

        for db in databases:
            print(f"\n=== Base de datos: {db} ===")

            objects = get_tables_and_views(conn, db)

            for _, row in objects.iterrows():
                try:
                    extract_table(
                        conn,
                        db,
                        row["TABLE_SCHEMA"],
                        row["TABLE_NAME"]
                    )
                except Exception as e:
                    print(f"⚠️ Error en {db}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")
                    print(str(e))
                    continue   # 🔑 CLAVE: no rompe el pipeline

    print("\n✔ Extracción completa (con posibles omisiones)")

if __name__ == "__main__":
    main()