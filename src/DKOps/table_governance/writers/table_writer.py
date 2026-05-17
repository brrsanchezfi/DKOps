"""
table_writer.py
===============
Fachada unificada para escribir tablas Delta.

Simplifica la API del framework exponiendo métodos con nombres descriptivos
en lugar de requerir instanciar writers individuales:

    writer = TableWriter(contract)

    writer.overwrite(df)                          # CREATE OR REPLACE
    writer.append(df)                             # INSERT INTO
    writer.upsert(df, keys=["id"])               # MERGE INTO
    writer.overwrite_partition(df, {"fecha": "2024-01-15"})
    writer.delete("fecha < '2020-01-01'")        # DELETE WHERE

Parámetros del constructor
--------------------------
contract        : TableContract cargado por ContractLoader o load_contract().
strict_columns  : si True, columnas extra en el DF generan WARNING (default True).
fail_on_warning : si True, los WARNINGs también bloquean la escritura (default False).
dry_run         : si True, valida y loguea pero no escribe ni modifica nada.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from DKOps.table_governance.contracts.loader import TableContract
from DKOps.table_governance.writers.create_writer    import CreateWriter
from DKOps.table_governance.writers.append_writer    import AppendWriter
from DKOps.table_governance.writers.upsert_writer    import UpsertWriter
from DKOps.table_governance.writers.partition_writer import PartitionWriter
from DKOps.table_governance.writers.delete_writer    import DeleteWriter


class TableWriter:
    """
    Fachada unificada para todas las operaciones de escritura sobre una tabla Delta.

    Uso rápido
    ----------
        from DKOps.table_governance import TableWriter, load_contract

        contract = load_contract("tables/vuelos_raw.json")
        writer   = TableWriter(contract)

        writer.overwrite(df)
        writer.append(df_nuevo)
        writer.upsert(df_correcciones, keys=["vuelo_id"])
        writer.overwrite_partition(df_reproc, {"fecha": "2024-01-15"})
        writer.delete("distancia_km = 0")
    """

    def __init__(
        self,
        contract:        TableContract,
        strict_columns:  bool = True,
        fail_on_warning: bool = False,
        dry_run:         bool = False,
    ) -> None:
        self._contract = contract
        self._writer_kwargs = dict(
            strict_columns  = strict_columns,
            fail_on_warning = fail_on_warning,
            dry_run         = dry_run,
        )
        self._dry_run = dry_run

    # ── Operaciones de escritura ──────────────────────────────────────────

    def overwrite(self, df: DataFrame) -> None:
        """
        Reemplaza la tabla completa (CREATE OR REPLACE TABLE).

        Idempotente. Equivalente a: ``CreateWriter(contract).write(df)``
        """
        CreateWriter(self._contract, **self._writer_kwargs).write(df)

    def append(self, df: DataFrame) -> None:
        """
        Inserta filas al final de la tabla sin tocar las existentes.

        Si el contrato define ``merge_schema: true``, columnas nuevas del DF
        se agregan automáticamente al schema de la tabla.

        Equivalente a: ``AppendWriter(contract).write(df)``
        """
        AppendWriter(self._contract, **self._writer_kwargs).write(df)

    def upsert(
        self,
        df:             DataFrame,
        keys:           list[str],
        update_columns: list[str] | None = None,
    ) -> None:
        """
        MERGE INTO — actualiza filas existentes e inserta las nuevas.

        Parámetros
        ----------
        df             : DataFrame con los datos a sincronizar.
        keys           : columnas que identifican univocamente cada fila.
                         Ej: ``keys=["vuelo_id"]``
        update_columns : columnas a actualizar en filas existentes.
                         Si se omite, se actualizan todas las columnas que no son key.

        Equivalente a: ``UpsertWriter(contract).write(df, merge_keys=keys)``
        """
        UpsertWriter(self._contract, **self._writer_kwargs).write(
            df,
            merge_keys     = keys,
            update_columns = update_columns,
        )

    def overwrite_partition(
        self,
        df:        DataFrame,
        partition: dict[str, str],
    ) -> None:
        """
        Reemplaza una partición específica sin tocar el resto de la tabla.

        Parámetros
        ----------
        df        : DataFrame con los datos nuevos de la partición.
        partition : dict con la columna de partición y su valor.
                    Ej: ``partition={"fecha": "2024-01-15"}``

        Equivalente a: ``PartitionWriter(contract).write(df, partition={"fecha": "..."})``
        """
        PartitionWriter(self._contract, **self._writer_kwargs).write(
            df,
            partition = partition,
        )

    def delete(self, condition: str, preview: bool = False) -> int:
        """
        Elimina filas que cumplan la condición SQL dada.

        Parámetros
        ----------
        condition : expresión SQL WHERE (sin la palabra WHERE).
                    Ej: ``"fecha < '2023-01-01'"``
        preview   : si True, muestra las filas a eliminar antes de borrar.

        Devuelve
        --------
        Número de filas eliminadas.

        Equivalente a: ``DeleteWriter(contract).delete(condition)``
        """
        return DeleteWriter(self._contract, dry_run=self._dry_run).delete(
            condition,
            preview = preview,
        )

    def __repr__(self) -> str:
        return (
            f"TableWriter({self._contract.full_name!r}, "
            f"dry_run={self._dry_run})"
        )
