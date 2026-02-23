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

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        print(f"⚠️ {step} terminó con errores, continuando pipeline")

run("main.py")
run("upload_to_gcs.py")
run("load_to_bigquery.py")

print("\n✔ Pipeline finalizado")