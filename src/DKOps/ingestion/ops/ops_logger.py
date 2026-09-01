"""
ops_logger.py — IngestionOpsLogger: registro operativo de ejecuciones.

Escribe en una tabla Delta de control el ciclo de vida de cada ingesta:
  STARTED → SUCCESS | FAILED

La tabla se crea automáticamente si no existe (Delta auto-create).
En local: escribe en filesystem (path físico).
En Databricks: escribe en ADLS. Si se quiere registrar en Unity Catalog,
usa `register_in_catalog()` tras la primera ejecución.

Schema de la tabla de control:
  run_id        STRING       — UUID corto de la ejecución
  pipeline      STRING       — nombre del pipeline
  dataset       STRING       — nombre del dataset
  status        STRING       — STARTED | SUCCESS | FAILED
  rows_read     LONG
  rows_written  LONG
  started_at    TIMESTAMP    — inicio de la ejecucion
  finished_at   TIMESTAMP    — solo en las filas de cierre
  notes         STRING       — detalles o traceback en caso de error

Cada ejecucion deja DOS filas: una STARTED al abrir y una SUCCESS o FAILED al
cerrar. Es un log de eventos, no una tabla de estado — cualquier agregacion
debe filtrar por `status`.

Las dos filas llevan el mismo `started_at`, de modo que la duracion sale de una
resta sobre la fila de cierre y no hace falta un self-join por `run_id`:

    SELECT dataset,
           avg(unix_timestamp(finished_at) - unix_timestamp(started_at)) AS seg
    FROM   ops
    WHERE  status = 'SUCCESS'
    GROUP  BY dataset

`started_at` es nullable porque el logger guarda ese valor en memoria: si el
proceso se reinicia entre la apertura y el cierre, la fila de cierre se escribe
igualmente con `started_at` a NULL en vez de perderse.
"""

from __future__ import annotations

import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

from DKOps.logger_config import LoggableMixin

_OPS_SCHEMA = StructType([
    StructField("run_id",       StringType(),    nullable=False),
    StructField("pipeline",     StringType(),    nullable=False),
    StructField("dataset",      StringType(),    nullable=False),
    StructField("status",       StringType(),    nullable=False),
    StructField("rows_read",    LongType(),      nullable=True),
    StructField("rows_written", LongType(),      nullable=True),
    # nullable: un cierre (SUCCESS/FAILED) no reabre el inicio. Normalmente el
    # logger repite aqui el started_at de su run_id, pero si no lo conoce
    # —proceso reiniciado, reintento desde otro worker— la fila debe poder
    # escribirse igualmente. Ver issue #28.
    StructField("started_at",   TimestampType(), nullable=True),
    StructField("finished_at",  TimestampType(), nullable=True),
    StructField("notes",        StringType(),    nullable=True),
])


class IngestionOpsLogger(LoggableMixin):
    """
    Registro operativo de ejecuciones de ingesta en tabla Delta de control.

    Uso
    ---
        ops = IngestionOpsLogger(spark, ops_path="/tmp/ops/control")
        run_id = ops.log_start("ventas_diarias")
        ...
        ops.log_success(run_id, "ventas_diarias", rows_read=1000, rows_written=1000)
        # o bien:
        ops.log_failure(run_id, "ventas_diarias", error=exc)
    """

    def __init__(
        self,
        spark:    SparkSession,
        ops_path: str,
        pipeline: str = "ingestion",
    ) -> None:
        self._spark    = spark
        self._ops_path = ops_path
        self._pipeline = pipeline
        # started_at de cada ejecucion abierta, para repetirlo en el cierre.
        # Asi la tabla queda autocontenida: la duracion es una resta sobre la
        # fila de cierre, sin self-join por run_id.
        self._started: dict[str, datetime] = {}
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Crea la tabla de control si no existe escribiendo un DataFrame vacío."""
        try:
            empty = self._spark.createDataFrame([], _OPS_SCHEMA)
            (
                empty.write
                    .format("delta")
                    .mode("ignore")          # no-op si ya existe
                    .save(self._ops_path)
            )
            self.log.debug(f"OpsLogger tabla lista: {self._ops_path}")
        except Exception as exc:
            self.log.warning(f"OpsLogger: no se pudo crear tabla de control: {exc}")

    def log_start(self, dataset: str) -> str:
        """Registra inicio de ingesta. Devuelve run_id para las llamadas siguientes."""
        run_id     = str(uuid.uuid4())[:8]
        started_at = datetime.now(timezone.utc)
        self._started[run_id] = started_at
        self._write_row(
            run_id      = run_id,
            dataset     = dataset,
            status      = "STARTED",
            started_at  = started_at,
            notes       = f"pipeline={self._pipeline}",
        )
        self.log.info(f"[{dataset}] run_id={run_id} | STARTED")
        return run_id

    def log_success(
        self,
        run_id:       str,
        dataset:      str,
        rows_read:    int = 0,
        rows_written: int = 0,
        notes:        str = "",
    ) -> None:
        self._write_row(
            run_id       = run_id,
            dataset      = dataset,
            status       = "SUCCESS",
            rows_read    = rows_read,
            rows_written = rows_written,
            started_at   = self._started.pop(run_id, None),
            finished_at  = datetime.now(timezone.utc),
            notes        = notes,
        )
        self.log.info(
            f"[{dataset}] run_id={run_id} | SUCCESS | "
            f"rows_written={rows_written:,}"
        )

    def log_failure(
        self,
        run_id:    str,
        dataset:   str,
        error:     Exception,
        rows_read: int = 0,
    ) -> None:
        tb    = traceback.format_exc()
        notes = f"{type(error).__name__}: {str(error)[:300]} | {tb[:300]}"
        self._write_row(
            run_id      = run_id,
            dataset     = dataset,
            status      = "FAILED",
            rows_read   = rows_read,
            started_at  = self._started.pop(run_id, None),
            finished_at = datetime.now(timezone.utc),
            notes       = notes,
        )
        self.log.error(f"[{dataset}] run_id={run_id} | FAILED | {error}")

    def _write_row(
        self,
        run_id:       str,
        dataset:      str,
        status:       str,
        started_at:   datetime | None  = None,
        finished_at:  datetime | None  = None,
        rows_read:    int              = 0,
        rows_written: int              = 0,
        notes:        str              = "",
    ) -> None:
        row = [(
            run_id,
            self._pipeline,
            dataset,
            status,
            rows_read,
            rows_written,
            started_at,
            finished_at,
            notes[:500] if notes else "",
        )]
        try:
            df = self._spark.createDataFrame(row, _OPS_SCHEMA)
            df.write.format("delta").mode("append").save(self._ops_path)
        except Exception as exc:
            # No se relanza: tumbar una ingesta que fue bien porque no se pudo
            # anotar el cierre seria peor que el problema. Pero se registra como
            # ERROR y con el tipo de excepcion — un registro operativo que se
            # cae en silencio da falsa confianza (issue #28).
            self.log.error(
                f"OpsLogger: no se pudo escribir el registro "
                f"[{status}] de run_id={run_id} en {self._ops_path} | "
                f"{type(exc).__name__}: {exc}"
            )

    def read(self):
        """Devuelve el DataFrame completo de la tabla de control."""
        return self._spark.read.format("delta").load(self._ops_path)
