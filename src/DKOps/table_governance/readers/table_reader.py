"""
table_reader.py
===============
Fachada unificada para leer tablas Delta usando el contrato como fuente
de configuración (tabla efectiva, columnas de partición, CDF, etc.).

Todos los métodos retornan un ``pyspark.sql.DataFrame`` real, por lo que
todas las transformaciones, acciones y métodos de escritura nativos de
PySpark funcionan sin ningún wrapper adicional.

    reader = TableReader(contract)

    df = reader.read()                              # tabla completa
    df = reader.read(filter="estado = 'ACTIVE'")    # con predicado
    df = reader.read_partition({"fecha": "2024-01"})# una partición
    df = reader.read_stream()                       # streaming DataFrame
    df = reader.read_cdf(starting_version=5)        # Change Data Feed

El Spark y el EnvironmentConfig se obtienen automáticamente del
``Launcher.current()`` activo — igual que los writers.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from DKOps.launcher import Launcher
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import TableContract

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class TableReader(LoggableMixin):
    """
    Fachada de lectura para tablas Delta gobernadas por contrato.

    Parámetros
    ----------
    contract : TableContract cargado por ``load_contract()``.

    Notas
    -----
    - Todos los métodos retornan un ``DataFrame`` de PySpark estándar,
      por lo que se pueden seguir encadenando ``.filter()``, ``.join()``,
      ``.groupBy()``, ``.write``, etc. directamente.
    - ``read_cdf()`` requiere ``change_data_feed: true`` en el contrato.
      Si no está habilitado lanza ``ValueError`` con un mensaje claro.
    """

    def __init__(self, contract: TableContract) -> None:
        launcher = Launcher.current()
        self._spark    = launcher.spark
        self._env      = launcher.env
        self._contract = contract
        self._table_name: str = (
            contract.full_name
            if self._env._is_databricks
            else f"{contract.schema}.{contract.name}"
        )
        self.log.debug(
            f"TableReader listo | tabla='{self._table_name}' | "
            f"runtime={'databricks' if self._env._is_databricks else 'local-pc'}"
        )

    # ── Lectura completa ──────────────────────────────────────────────────────

    def read(
        self,
        filter:  str | None        = None,
        columns: list[str] | None  = None,
        limit:   int | None        = None,
    ) -> "DataFrame":
        """
        Lee la tabla completa y retorna un DataFrame de PySpark.

        Los parámetros opcionales son atajos de conveniencia; cualquier
        transformación adicional puede aplicarse directamente sobre el DF
        retornado usando la API nativa de Spark.

        Parámetros
        ----------
        filter  : expresión SQL WHERE (sin la palabra WHERE).
                  Ej: ``"estado = 'ACTIVE' AND fecha > '2024-01-01'"``
        columns : lista de columnas a seleccionar. Si ``None``, se retornan
                  todas.
        limit   : número máximo de filas a retornar.

        Devuelve
        --------
        ``pyspark.sql.DataFrame``
        """
        t0 = time.monotonic()
        self.log.info(
            f"▶ read | tabla='{self._table_name}'"
            + (f" | filter='{filter}'" if filter else "")
            + (f" | columns={columns}" if columns else "")
            + (f" | limit={limit}" if limit is not None else "")
        )

        df = self._spark.read.table(self._table_name)

        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Columnas no encontradas en '{self._table_name}': {missing}\n"
                    f"Disponibles: {df.columns}"
                )
            df = df.select(*columns)

        if filter:
            df = df.filter(filter)

        if limit is not None:
            if limit < 0:
                raise ValueError(f"limit debe ser >= 0, recibido: {limit}")
            df = df.limit(limit)

        elapsed = time.monotonic() - t0
        self.log.success(
            f"✔ read | tabla='{self._table_name}' | "
            f"elapsed={elapsed:.2f}s"
        )
        return df

    # ── Lectura por partición ─────────────────────────────────────────────────

    def read_partition(self, partition: dict[str, str]) -> "DataFrame":
        """
        Lee una partición específica de la tabla.

        Valida que todas las claves del dict sean columnas de partición
        declaradas en el contrato antes de consultar.

        Parámetros
        ----------
        partition : dict con columna → valor de partición.
                    Ej: ``{"fecha": "2024-01-15"}``
                    Ej: ``{"anio": "2024", "mes": "01"}``

        Devuelve
        --------
        ``pyspark.sql.DataFrame`` filtrado a la partición indicada.

        Lanza
        -----
        ValueError si ``partition`` está vacío o contiene columnas que no
        son de partición en el contrato.
        """
        if not partition:
            raise ValueError("partition no puede estar vacío.")

        contract_parts = set(self._contract.partition_columns)
        invalid = [c for c in partition if c not in contract_parts]
        if invalid:
            raise ValueError(
                f"Columnas no son de partición en '{self._table_name}': {invalid}\n"
                f"Particiones del contrato: {self._contract.partition_columns}"
            )

        conditions = " AND ".join(
            f"`{col}` = '{val}'" for col, val in partition.items()
        )
        self.log.info(
            f"▶ read_partition | tabla='{self._table_name}' | "
            f"filtro='{conditions}'"
        )

        t0 = time.monotonic()
        df  = self._spark.read.table(self._table_name).filter(conditions)
        elapsed = time.monotonic() - t0

        self.log.success(
            f"✔ read_partition | tabla='{self._table_name}' | "
            f"partition={partition} | elapsed={elapsed:.2f}s"
        )
        return df

    # ── Streaming ─────────────────────────────────────────────────────────────

    def read_stream(self) -> "DataFrame":
        """
        Lee la tabla como streaming DataFrame (Delta log incremental).

        El DataFrame retornado es un ``readStream`` estándar de Spark
        Structured Streaming — se puede procesar con ``writeStream``,
        ``.foreachBatch()``, etc.

        Devuelve
        --------
        ``pyspark.sql.DataFrame`` con ``isStreaming == True``.
        """
        self.log.info(f"▶ read_stream | tabla='{self._table_name}'")

        stream = (
            self._spark.readStream
            .format("delta")
            .table(self._table_name)
        )

        self.log.success(
            f"✔ read_stream | tabla='{self._table_name}' | streaming=True"
        )
        return stream

    # ── Change Data Feed ──────────────────────────────────────────────────────

    def read_cdf(
        self,
        starting_version:   int | None = None,
        starting_timestamp: str | None = None,
        ending_version:     int | None = None,
    ) -> "DataFrame":
        """
        Lee el Change Data Feed (CDF) de la tabla.

        El CDF registra cada fila insertada, actualizada o eliminada como
        una entrada separada con columna ``_change_type`` (``insert``,
        ``update_preimage``, ``update_postimage``, ``delete``).

        Requiere ``"change_data_feed": true`` en el contrato —
        el ``CreateWriter`` activa ``delta.enableChangeDataFeed`` en
        ``TBLPROPERTIES`` automáticamente al crear la tabla.

        Parámetros
        ----------
        starting_version   : versión Delta desde la que leer (inclusiva).
        starting_timestamp : timestamp ISO desde el que leer.
                             Solo uno de ``starting_version`` /
                             ``starting_timestamp`` debe pasarse.
        ending_version     : versión Delta hasta la que leer (inclusiva).
                             Si se omite, lee hasta la versión más reciente.

        Devuelve
        --------
        ``pyspark.sql.DataFrame`` con columnas adicionales:
        ``_change_type``, ``_commit_version``, ``_commit_timestamp``.

        Lanza
        -----
        ValueError  si el contrato no tiene ``change_data_feed=True``.
        ValueError  si no se pasa ni ``starting_version`` ni
                    ``starting_timestamp``.
        """
        if not self._contract.change_data_feed:
            raise ValueError(
                f"La tabla '{self._table_name}' no tiene change_data_feed "
                "habilitado en el contrato.\n"
                "Agrega '\"change_data_feed\": true' al contrato JSON y "
                "vuelve a crear la tabla."
            )

        if starting_version is None and starting_timestamp is None:
            raise ValueError(
                "read_cdf requiere 'starting_version' o 'starting_timestamp'."
            )

        if starting_version is not None and starting_timestamp is not None:
            raise ValueError(
                "Pasa solo uno: 'starting_version' o 'starting_timestamp', no ambos."
            )

        self.log.info(
            f"▶ read_cdf | tabla='{self._table_name}' | "
            + (f"starting_version={starting_version}" if starting_version is not None
               else f"starting_timestamp='{starting_timestamp}'")
            + (f" | ending_version={ending_version}" if ending_version is not None else "")
        )

        t0     = time.monotonic()
        reader = (
            self._spark.read
            .format("delta")
            .option("readChangeFeed", "true")
        )

        if starting_version is not None:
            reader = reader.option("startingVersion", starting_version)
        else:
            reader = reader.option("startingTimestamp", starting_timestamp)

        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)

        df      = reader.table(self._table_name)
        elapsed = time.monotonic() - t0

        self.log.success(
            f"✔ read_cdf | tabla='{self._table_name}' | elapsed={elapsed:.2f}s"
        )
        return df

    def __repr__(self) -> str:
        return f"TableReader({self._contract.full_name!r})"
