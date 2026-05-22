"""
append_dedup.py — Append deduplicado: inserta solo registros nuevos.

Algoritmo:
  1. Lee Bronze con los nuevos registros
  2. Filtra los que ya existen en Silver por merge_keys (anti-join)
  3. Inserta solo los nuevos (append)

Cuándo usarlo:
  - Imágenes, archivos binarios donde no hay updates
  - Eventos de auditoría inmutables
  - Tablas de hechos donde cada fila es única e irrepetible

Nota: Para volúmenes muy grandes, considera habilitar change_data_feed
en Bronze y filtrar por versión en lugar de hacer anti-join completo.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from DKOps.ingestion.strategies.base import BasePromotionStrategy


class AppendDedupStrategy(BasePromotionStrategy):
    """Inserta registros de Bronze que no existen en Silver (por merge_keys)."""

    def execute(self) -> int:
        keys = list(self._contract.merge_keys)
        self.log.info(
            f"[{self._contract.name}] AppendDedup | keys={keys}"
        )

        bronze_df = self._read_bronze()
        silver_df = self._reader.read()  # lectura del destino para anti-join

        new_records = self._filter_new(bronze_df, silver_df, keys)
        count       = new_records.count()

        if count == 0:
            self.log.info(f"[{self._contract.name}] AppendDedup: sin registros nuevos")
            return 0

        self._writer.append(new_records)
        self.log.info(f"[{self._contract.name}] AppendDedup completado | new_rows={count:,}")
        return count

    @staticmethod
    def _filter_new(
        source: DataFrame,
        target: DataFrame,
        keys:   list[str],
    ) -> DataFrame:
        """Anti-join: retorna filas de source que no existen en target por keys."""
        target_keys = target.select(*keys).withColumn("_exists", F.lit(True))
        joined = source.join(target_keys, on=keys, how="left")
        return joined.filter(F.col("_exists").isNull()).drop("_exists")
