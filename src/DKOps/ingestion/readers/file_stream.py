"""
file_stream.py — Lectura streaming desde directorio de archivos.

Alternativa local a Auto Loader. Usa spark.readStream.format(fmt)
para procesar archivos nuevos que aparezcan en un directorio.

Funciona en local PC y Databricks. En producción Databricks se prefiere
AutoLoaderReader por su mayor eficiencia y tracking robusto.
Útil para: tests de streaming, CI/CD, entornos sin Databricks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.ingestion.readers.base import BaseSourceReader
from DKOps.ingestion.readers._schema_helper import build_spark_schema


class FileStreamReader(BaseSourceReader):
    """
    Streaming reader basado en el file source estándar de Spark.
    Monitorea un directorio y procesa archivos nuevos de forma incremental.
    """

    def __init__(self, contract: IngestionContract, spark: SparkSession) -> None:
        super().__init__(contract)
        self._spark = spark

    def read(self) -> DataFrame:
        src = self.contract.source
        self.log.info(
            f"[{self.contract.name}] FileStreamReader | "
            f"format={src.format} | path={src.path}"
        )

        reader = self._spark.readStream.format(src.format)

        for key, val in src.options.items():
            reader = reader.option(key, val)

        if src.schema:
            reader = reader.schema(build_spark_schema(list(src.schema)))
        else:
            # Sin schema explícito: inferir del primer archivo (requiere al menos uno)
            reader = reader.option("inferSchema", "true")

        return reader.load(src.path)
