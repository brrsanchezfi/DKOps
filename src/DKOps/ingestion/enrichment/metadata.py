"""
metadata.py — MetadataEnricher: columnas técnicas de trazabilidad.

Añade columnas estándar al DataFrame antes de escribirlo en Bronze/Silver.
Las columnas se controlan desde MetadataConfig en el IngestionContract.

Columnas estándar (batch/streaming de archivos):
  _ingested_at       TIMESTAMP   — momento exacto de ingesta
  _ingested_date     DATE        — fecha de ingesta (para particionado)
  _source_file       STRING      — path del archivo fuente

Columnas Kafka (solo con add_kafka_metadata=True):
  _kafka_topic       STRING
  _kafka_partition   INTEGER
  _kafka_offset      LONG
  _kafka_ts          TIMESTAMP
  _raw_value         STRING      — payload JSON crudo

Columnas Silver (solo con add_silver_timestamps=True):
  _silver_created_at    TIMESTAMP   — primera vez que se escribió en Silver
  _silver_modified_at   TIMESTAMP   — última modificación en Silver
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from DKOps.ingestion.contracts.ingestion_contract import MetadataConfig
from DKOps.logger_config import LoggableMixin


class MetadataEnricher(LoggableMixin):
    """Añade columnas técnicas a un DataFrame según la MetadataConfig del contrato."""

    def enrich(
        self,
        df:            DataFrame,
        config:        MetadataConfig,
        source_format: str = "",
    ) -> DataFrame:
        """
        Aplica enriquecimiento de metadata al DataFrame.

        Parámetros
        ----------
        df            : DataFrame fuente (batch o streaming).
        config        : MetadataConfig del IngestionContract.
        source_format : formato de la fuente para determinar qué columnas añadir.
        """
        is_kafka = source_format.lower() == "kafka"

        if config.add_kafka_metadata and is_kafka:
            df = self._add_kafka_cols(df)

        if config.add_ingested_at:
            df = df.withColumn("_ingested_at", F.current_timestamp())

        if config.add_ingested_date:
            df = df.withColumn("_ingested_date", F.current_date())

        if config.add_source_file and not is_kafka:
            df = df.withColumn("_source_file", F.input_file_name())

        if config.add_silver_timestamps:
            df = (
                df.withColumn("_silver_created_at",  F.current_timestamp())
                  .withColumn("_silver_modified_at", F.current_timestamp())
            )

        return df

    @staticmethod
    def _add_kafka_cols(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_kafka_topic",     F.col("topic"))
              .withColumn("_kafka_partition", F.col("partition").cast("integer"))
              .withColumn("_kafka_offset",    F.col("offset").cast("long"))
              .withColumn("_kafka_ts",        F.col("timestamp"))
              .withColumn("_raw_value",       F.col("value").cast("string"))
        )
