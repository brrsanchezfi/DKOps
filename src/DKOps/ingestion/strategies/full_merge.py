"""
full_merge.py — SCD Type 1: mantiene una fila por clave de negocio.

Algoritmo:
  1. Lee Bronze completo (o con filtro del contrato)
  2. Deduplica por merge_keys, quedándose con el registro más reciente
     según watermark_col (si se define)
  3. MERGE INTO Silver: actualiza existentes, inserta nuevos

Cuándo usarlo:
  - Catálogos de productos, dimensiones que cambian con SCD1
  - Cuando solo importa el estado actual (no el historial de cambios)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from DKOps.ingestion.strategies.base import BasePromotionStrategy


class FullMergeStrategy(BasePromotionStrategy):
    """MERGE INTO con deduplicación — mantiene el registro más reciente por clave."""

    def execute(self) -> int:
        self.log.info(
            f"[{self._contract.name}] FullMerge | "
            f"keys={list(self._contract.merge_keys)} | "
            f"watermark={self._contract.watermark_col}"
        )

        bronze_df = self._read_bronze()

        # Deduplicar: quedarse con el registro más reciente por merge_keys
        deduped = self._dedup(bronze_df)

        # Añadir timestamps Silver si el contrato los pide
        if self._contract.metadata.add_silver_timestamps:
            deduped = (
                deduped
                .withColumn("_silver_modified_at", F.current_timestamp())
            )

        # MERGE INTO Silver
        self._writer.upsert(
            deduped,
            keys           = list(self._contract.merge_keys),
        )

        count = self._reader.read().count()
        self.log.info(f"[{self._contract.name}] FullMerge completado | silver_rows={count:,}")
        return count

    def _dedup(self, df: DataFrame) -> DataFrame:
        keys = list(self._contract.merge_keys)
        wcol = self._contract.watermark_col

        if wcol and wcol in df.columns:
            window = Window.partitionBy(*keys).orderBy(F.col(wcol).desc())
            return (
                df.withColumn("_row_num", F.row_number().over(window))
                  .filter(F.col("_row_num") == 1)
                  .drop("_row_num")
            )

        # Sin watermark: dedup simple por clave
        return df.dropDuplicates(keys)
