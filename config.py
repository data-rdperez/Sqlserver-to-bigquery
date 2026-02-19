import os

SERVER = os.environ.get("SQL_SERVER")
USER = os.environ.get("SQL_USER")
PASSWORD = os.environ.get("SQL_PASSWORD")

DATABASE_TABLES = {
    "DWH_SPLGNMX": [
        "dbo.DimTractorHistorico",
        "dbo.ReporteCartasPorte"
    ],
    "DWH_SPLGNYC": [
        "dbo.DimTractorHistorico",
        "dbo.ReporteCartasPorte"
    ]
}

OUTPUT_BASE = "parquet_output"
