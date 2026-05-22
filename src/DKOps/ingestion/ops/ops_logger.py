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
  started_at    TIMESTAMP
  finished_at   TIMESTAMP
  notes         STRING       — detalles o traceback en caso de error
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
    StructField("started_at",   TimestampType(), nullable=False),
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
        run_id = str(uuid.uuid4())[:8]
        self._write_row(
            run_id      = run_id,
            dataset     = dataset,
            status      = "STARTED",
            started_at  = datetime.now(timezone.utc),
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
            self.log.warning(f"OpsLogger: no se pudo escribir registro: {exc}")

    def read(self):
        """Devuelve el DataFrame completo de la tabla de control."""
        return self._spark.read.format("delta").load(self._ops_path)
