"""
autoloader.py — Lectura con Databricks Auto Loader (cloudFiles).

SOLO disponible en Databricks. En local PC lanzará ImportError al instanciar.
El SourceReaderFactory nunca instancia este reader en entorno local.

Auto Loader ventajas vs spark.read():
  - Tracking exacto de archivos procesados (no re-lee lo ya ingestado)
  - Schema inference incremental con schema evolution
  - Soporte para checkpoints de schema en ADLS
  - Eficiente con millones de archivos (file notification API)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.ingestion.readers.base import BaseSourceReader
from DKOps.ingestion.readers._schema_helper import build_spark_schema


class AutoLoaderReader(BaseSourceReader):
    """
    Lee archivos desde Landing usando Databricks Auto Loader (cloudFiles).
    Produce un streaming DataFrame con trigger=availableNow para comportamiento
    batch (procesa todos los archivos pendientes y para).
    """

    def __init__(
        self,
        contract:        IngestionContract,
        spark:           SparkSession,
        schema_location: str,
    ) -> None:
        super().__init__(contract)
        self._spark          = spark
        self._schema_location = schema_location

    def read(self) -> DataFrame:
        src = self.contract.source
        self.log.info(
            f"[{self.contract.name}] AutoLoaderReader | "
            f"format={src.format} | path={src.path}"
        )

        reader = (
            self._spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format",          src.format)
                .option("cloudFiles.schemaLocation",  self._schema_location)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("mergeSchema",                "true")
        )

        for key, val in src.options.items():
            reader = reader.option(key, val)

        if src.schema:
            reader = reader.schema(build_spark_schema(list(src.schema)))

        return reader.load(src.path)
