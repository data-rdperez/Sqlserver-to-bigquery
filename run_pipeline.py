import subprocess
import sys

def run(step):
    print(f"\n=== Ejecutando {step} ===")
    result = subprocess.run(
        [sys.executable, step],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Error en {step}")

run("main.py")            # extrae TODAS las BD DWH
run("upload_to_gcs.py")   # sube parquet a GCS
run("load_to_bigquery.py")# carga a BigQuery

print("\n✔ Pipeline completado correctamente")