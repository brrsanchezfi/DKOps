"""
cdc_merge.py — CDC Merge: aplica eventos Insert/Update/Delete desde Bronze.

Algoritmo:
  1. Lee todos los eventos CDC de Bronze
  2. Por cada merge_key, toma el evento más reciente (op_type I/U/D)
  3. MERGE INTO Silver:
     - INSERT/UPDATE → upsert (merge)
     - DELETE        → soft delete (is_deleted=True) o hard delete

Espera columna op_type con valores: "I" | "U" | "D"
y opcionalmente una columna is_deleted BOOLEAN.

Cuándo usarlo:
  - Tablas con eventos CDC de bases de datos OLTP
  - Pedidos, transacciones con historial de cambios
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.ingestion.strategies.base import BasePromotionStrategy
from DKOps.table_governance.contracts.loader import TableContract


class CdcMergeStrategy(BasePromotionStrategy):
    """
    Aplica eventos CDC (I/U/D) desde Bronze hacia Silver.
    Mantiene el estado actual de cada entidad con soft deletes.
    """

    def __init__(
        self,
        spark:        SparkSession,
        contract:     IngestionContract,
        src_contract: TableContract,
        dst_contract: TableContract,
        op_col:       str  = "op_type",
        soft_delete:  bool = True,
    ) -> None:
        super().__init__(spark, contract, src_contract, dst_contract)
        self._op_col      = op_col
        self._soft_delete = soft_delete

    def execute(self) -> int:
        self.log.info(
            f"[{self._contract.name}] CdcMerge | "
            f"keys={list(self._contract.merge_keys)} | "
            f"op_col={self._op_col} | soft_delete={self._soft_delete}"
        )

        bronze_df = self._read_bronze()
        latest_df = self._keep_latest_per_key(bronze_df)

        # Separar inserts/updates de deletes
        deletes = latest_df.filter(F.col(self._op_col) == "D")
        upserts = latest_df.filter(F.col(self._op_col).isin("I", "U"))

        if upserts.count() > 0:
            # Añadir timestamps Silver
            upserts = self._add_silver_timestamps(upserts)

            # Garantizar is_deleted no nulo en los eventos I/U
            upserts = self._ensure_is_deleted(upserts, deleted=False)

            # Seleccionar solo columnas Silver (excluir metadata Bronze)
            upserts = self._select_for_silver(upserts)

            self._writer.upsert(
                upserts,
                keys                = list(self._contract.merge_keys),
                insert_only_columns = self._silver_insert_only_columns(),
            )

        if deletes.count() > 0:
            self._apply_deletes(deletes)

        count = self._dst_reader.read().count()
        self.log.info(f"[{self._contract.name}] CdcMerge completado | silver_rows={count:,}")
        return count

    def _ensure_is_deleted(self, df: DataFrame, deleted: bool) -> DataFrame:
        """
        Garantiza que `is_deleted` llegue a Silver como true o false, nunca NULL.

        Antes solo se rellenaba cuando la columna **faltaba** en el DataFrame.
        Bastaba con que llegara desde Bronze —por ejemplo porque Auto Loader la
        incorporó al esquema— para que el default dejara de aplicarse y los
        nulos se propagaran.

        Un NULL aquí no rompe nada de forma visible, y ese es el problema: el
        filtro natural aguas abajo es `WHERE NOT is_deleted`, y con lógica
        ternaria `NOT NULL` no es `TRUE`, así que esas filas desaparecen del
        resultado sin error alguno.

        Parámetros
        ----------
        deleted : True  → evento D. La baja se marca pase lo que pase en origen.
                  False → evento I/U. Se respeta el valor del origen si lo trae,
                          y solo se rellenan los nulos.
        """
        if "is_deleted" not in self._dst_contract.column_names:
            return df

        if deleted:
            return df.withColumn("is_deleted", F.lit(True))

        if "is_deleted" not in df.columns:
            return df.withColumn("is_deleted", F.lit(False))

        return df.withColumn(
            "is_deleted", F.coalesce(F.col("is_deleted"), F.lit(False))
        )

    def _keep_latest_per_key(self, df: DataFrame) -> DataFrame:
        """Retiene solo el evento más reciente por clave de negocio."""
        keys  = list(self._contract.merge_keys)
        wcol  = self._contract.watermark_col

        if wcol and wcol in df.columns:
            window = Window.partitionBy(*keys).orderBy(F.col(wcol).desc())
            return (
                df.withColumn("_row_num", F.row_number().over(window))
                  .filter(F.col("_row_num") == 1)
                  .drop("_row_num")
            )
        return df.dropDuplicates(keys)

    def _apply_deletes(self, deletes: DataFrame) -> None:
        keys      = list(self._contract.merge_keys)
        dst_table = self._dst_contract.effective_name

        if self._soft_delete and "is_deleted" in self._dst_contract.column_names:
            # Soft delete: marcar is_deleted=True vía upsert
            soft = self._ensure_is_deleted(deletes, deleted=True)

            # Añadir timestamps Silver también en soft-deletes
            soft = self._add_silver_timestamps(soft)

            # Seleccionar solo columnas Silver (excluir metadata Bronze)
            soft = self._select_for_silver(soft)

            self._writer.upsert(
                soft,
                keys                = keys,
                insert_only_columns = self._silver_insert_only_columns(),
            )
        else:
            # Hard delete: DELETE WHERE key IN (...)
            key_col   = keys[0]
            key_vals  = [str(row[key_col]) for row in deletes.select(key_col).collect()]
            quoted    = ", ".join(f"'{v}'" for v in key_vals)
            condition = f"{key_col} IN ({quoted})"
            self._writer.delete(condition)
