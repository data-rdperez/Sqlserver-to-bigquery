import pyodbc
import pandas as pd
import os
from pathlib import Path

SERVER = os.environ["SQL_SERVER"]
USER = os.environ["SQL_USER"]
PASSWORD = os.environ["SQL_PASSWORD"]


BASE_OUTPUT = Path("parquet_output")
BASE_OUTPUT.mkdir(exist_ok=True)

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};UID={USER};PWD={PASSWORD}",
    autocommit=True
)
cursor = conn.cursor()

def get_databases(cursor):

    cursor.execute("""
        SELECT name
        FROM sys.databases
        WHERE name NOT IN ('master','tempdb','model','msdb')
    """)
    all_dbs = [row[0] for row in cursor.fetchall()]
    return [db for db in all_dbs if db.startswith("DWH_")]

def get_tables(cursor):

    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')
    """)
    return cursor.fetchall()

def get_date_columns(cursor, schema, table):

    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
          AND DATA_TYPE IN ('date','datetime','datetime2','smalldatetime')
    """, schema, table)
    return [row[0] for row in cursor.fetchall()]

def cast_date_columns(df, date_columns):

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


for db in get_databases(cursor):
    print(f"\n=== Base de datos: {db} ===")
    cursor.execute(f"USE [{db}]")

    output_dir = BASE_OUTPUT / db
    output_dir.mkdir(exist_ok=True)

    for schema, table in get_tables(cursor):
        try:
            print(f"→ Extrayendo {schema}.{table}")

            df = pd.read_sql(
                f"SELECT * FROM [{schema}].[{table}]",
                conn
            )

            date_columns = get_date_columns(cursor, schema, table)
            df = cast_date_columns(df, date_columns)

            file_path = output_dir / f"{schema}_{table}.parquet"
            df.to_parquet(file_path, index=False)

            print(f"✓ Guardado {file_path}")

        except Exception as e:
            print(f"✗ Error en {db}.{schema}.{table}: {e}")

print("\n✔ Extracción completa")