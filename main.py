import os
import pandas as pd
import pyodbc
import sys
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
    errores = []

    try:
        with pyodbc.connect(CONN_STR) as conn:
            databases = get_dwh_databases(conn)

            for db in databases:
                print(f"\n=== Base de datos: {db} ===")

                try:
                    objects = get_tables_and_views(conn, db)
                except Exception as e:
                    print(f"❌ No se pudo listar objetos en {db}")
                    errores.append((db, "LISTADO", str(e)))
                    continue

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
                        errores.append((db, row["TABLE_NAME"], str(e)))
                        continue

    except Exception as e:
        print("❌ Error crítico no controlado")
        print(str(e))
        errores.append(("CRITICO", "-", str(e)))

    print("\n================ RESUMEN ================")
    print(f"Errores totales: {len(errores)}")

    for err in errores[:20]:
        print(err)

    print("\n✔ Proceso finalizado (con o sin errores)")
    return 0   # 🔑 CLAVE ABSOLUTA

if __name__ == "__main__":
    sys.exit(main())