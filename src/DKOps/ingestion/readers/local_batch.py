"""local_batch.py — Lectura batch con spark.read(). Funciona en local y Databricks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.ingestion.readers.base import BaseSourceReader
from DKOps.ingestion.readers._schema_helper import build_spark_schema


class LocalBatchReader(BaseSourceReader):
    """
    Lee archivos desde Landing usando spark.read() estándar.
    Compatible con local PC y Databricks.

    En Databricks, para uso en producción considera AutoLoaderReader
    que ofrece tracking de archivos procesados y schema evolution automática.
    """

    def __init__(self, contract: IngestionContract, spark: SparkSession) -> None:
        super().__init__(contract)
        self._spark = spark

    def read(self) -> DataFrame:
        src = self.contract.source
        self.log.info(
            f"[{self.contract.name}] LocalBatchReader | "
            f"format={src.format} | path={src.path}"
        )

        reader = self._spark.read.format(src.format)

        for key, val in src.options.items():
            reader = reader.option(key, val)

        if src.schema:
            reader = reader.schema(build_spark_schema(list(src.schema)))

        return reader.load(src.path)
