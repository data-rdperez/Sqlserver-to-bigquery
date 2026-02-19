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

run("extract_all_sqlserver.py")
run("upload_to_gcs.py")
run("load_to_bigquery.py")

print("\n✔ Pipeline completado correctamente")
