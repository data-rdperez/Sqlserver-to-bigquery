import os
import pandas as pd
import pyodbc
from config import SERVER, USER, PASSWORD, OUTPUT_BASE
from config import DATABASE_TABLES

def get_connection(database):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def extract_table(conn, db, table):
    schema, table_name = table.split(".")
    query = f"SELECT * FROM {schema}.{table_name}"

    print(f"→ Extrayendo {table}")
    df = pd.read_sql(query, conn)

    output_dir = f"{OUTPUT_BASE}/{db}"
    os.makedirs(output_dir, exist_ok=True)

    output_path = f"{output_dir}/{schema}_{table_name}.parquet"
    df.to_parquet(output_path, index=False)

    print(f"✓ Guardado {output_path}")

def main():
    errores = []

    for db, tables in DATABASE_TABLES.items():
        print(f"\n=== Base de datos: {db} ===")

        try:
            conn = get_connection(db)
        except Exception as e:
            print(f"❌ No se pudo conectar a {db}: {e}")
            errores.append(f"{db}: conexión")
            continue

        for table in tables:
            try:
                extract_table(conn, db, table)
            except Exception as e:
                print(f"⚠ Error en {db}.{table}: {e}")
                errores.append(f"{db}.{table}")
                continue

        conn.close()

    print("\n==============================")
    if errores:
        print("⚠ Pipeline terminó con errores en:")
        for e in errores:
            print(f" - {e}")
    else:
        print("✓ Pipeline completado sin errores")
    print("==============================")

if __name__ == "__main__":
    main()