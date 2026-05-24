"""
incremental_replace.py — Reemplaza partición con el snapshot más reciente.

Algoritmo:
  1. Lee Bronze filtrando por la última partición disponible (max _ingested_date)
  2. Hace overwrite_partition en Silver para esa fecha

Cuándo usarlo:
  - Snapshots diarios de inventario, stock, precios
  - Cuando cada partición es autocontenida y no hay historial
  - Tablas que se regeneran completamente cada día por partición
"""

from __future__ import annotations

from pyspark.sql import functions as F

from DKOps.ingestion.strategies.base import BasePromotionStrategy


class IncrementalReplaceStrategy(BasePromotionStrategy):
    """Reemplaza la partición más reciente de Silver con el snapshot de Bronze."""

    def execute(self) -> int:
        partition_col = (
            self._contract.watermark_col
            or (self._dst_contract.partition_columns[0] if self._dst_contract.partition_columns else "_ingested_date")
        )

        self.log.info(
            f"[{self._contract.name}] IncrementalReplace | partition_col={partition_col}"
        )

        bronze_df = self._read_bronze()

        # Fecha de la última partición disponible
        max_val_row = bronze_df.select(F.max(F.col(partition_col)).alias("max_val")).collect()
        if not max_val_row or max_val_row[0]["max_val"] is None:
            self.log.warning(f"[{self._contract.name}] Bronze vacío, nada que procesar")
            return 0

        max_val   = max_val_row[0]["max_val"]
        latest_df = bronze_df.filter(F.col(partition_col) == max_val)

        # Añadir timestamps Silver antes de filtrar columnas
        if self._contract.metadata.add_silver_timestamps:
            latest_df = latest_df.withColumn("_silver_modified_at", F.current_timestamp())

        # Seleccionar solo columnas Silver (excluir metadata Bronze)
        latest_df = self._select_for_silver(latest_df)

        self._writer.overwrite_partition(
            latest_df,
            partition = {partition_col: str(max_val)},
        )

        count = latest_df.count()
        self.log.info(
            f"[{self._contract.name}] IncrementalReplace completado | "
            f"partition={max_val} | rows={count:,}"
        )
        return count
