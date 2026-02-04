
SERVER = "34.149.160.16"
USER = "rperez"
PASSWORD = "r<1VP8jD5/0#+ZF6"


DATABASE_TABLES = {
    "DWH_SPLGNMX": [
        "dbo.DimTractorHistorico",
        "dbo.DimOperador",
        "dbo.FactViajes"
        "dbo.ReporteCartasPorte"
    ],
    "DWH_SPLGNYC": [
        "dbo.DimTractorHistorico",
        "dbo.ReporteCartasPorte"
    ]
}


OUTPUT_BASE = "parquet_output"
