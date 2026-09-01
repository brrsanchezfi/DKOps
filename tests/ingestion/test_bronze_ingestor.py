"""
test_bronze_ingestor.py — Tests del BronzeIngestor con mocks de PySpark.

Verifica la lógica de orquestación: reader → enricher → validator → writer.
No requiere PySpark instalado — todos los componentes de Spark son mocks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.dataframe", "pyspark.sql.window", "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.ingestion.bronze_ingestor import BronzeIngestor
from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract, IngestionType, LoadType, MetadataConfig, SourceSpec,
)
from DKOps.table_governance.contracts.loader import ColumnContract, TableContract


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_ingestion_contract(
    name:         str = "test_dataset",
    ingest_type:  str = "batch",
    source_path:  str = "/tmp/landing/data",
    source_fmt:   str = "json",
) -> IngestionContract:
    return IngestionContract(
        name                      = name,
        ingest_type               = IngestionType(ingest_type),
        load_type                 = LoadType.INCREMENTAL,
        source                    = SourceSpec(format=source_fmt, path=source_path),
        destination_contract_path = "tables/bronze/test.json",
        metadata                  = MetadataConfig(
            add_ingested_at=True, add_ingested_date=True, add_source_file=True,
        ),
        checkpoint_suffix         = f"bronze/{name}",
    )


def _make_table_contract(name: str = "test") -> TableContract:
    """Sin particiones — activa el path de fallback a append."""
    return TableContract(
        catalog  = "bronze",
        schema   = "raw",
        name     = name,
        type     = "MANAGED",
        format   = "DELTA",
        columns  = (
            ColumnContract(name="id",   type="STRING"),
            ColumnContract(name="data", type="STRING"),
            ColumnContract(name="_ingested_at",   type="TIMESTAMP"),
            ColumnContract(name="_ingested_date", type="DATE"),
            ColumnContract(name="_source_file",   type="STRING"),
        ),
        partitions  = (),
        permissions = (),
    )


def _make_partitioned_table_contract(name: str = "test") -> TableContract:
    """Particionado por _ingested_date — activa el path de partition overwrite."""
    return TableContract(
        catalog  = "bronze",
        schema   = "raw",
        name     = name,
        type     = "MANAGED",
        format   = "DELTA",
        columns  = (
            ColumnContract(name="id",   type="STRING"),
            ColumnContract(name="data", type="STRING"),
            ColumnContract(name="_ingested_at",   type="TIMESTAMP"),
            ColumnContract(name="_ingested_date", type="DATE"),
            ColumnContract(name="_source_file",   type="STRING"),
        ),
        partitions  = ("_ingested_date",),
        permissions = (),
    )


def _mock_batch_df(n_rows: int = 5):
    df = MagicMock()
    df.isStreaming = False
    df.withColumn.return_value = df
    df.count.return_value      = n_rows
    df.columns = ["id", "data"]
    return df


# ── Tests de BronzeIngestor ───────────────────────────────────────────────────

class TestBronzeIngestor:

    @pytest.fixture
    def spark(self):
        return MagicMock()

    @pytest.fixture
    def env(self):
        env = MagicMock()
        env._is_databricks = False
        env.has_path.return_value = False
        return env

    @pytest.fixture
    def ingestor(self, spark, env) -> BronzeIngestor:
        return BronzeIngestor(spark=spark, env=env)

    def test_ingest_calls_reader_read(self, ingestor):
        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()
        mock_df      = _mock_batch_df()

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()
            MockWriter.return_value.append = MagicMock()

            ingestor.ingest(contract, dst_contract)

        mock_reader.read.assert_called_once()

    def test_ingest_calls_enricher(self, ingestor):
        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()
        mock_df      = _mock_batch_df()

        mock_enricher = MagicMock()
        mock_enricher.enrich.return_value = mock_df
        ingestor._enricher = mock_enricher  # patch instance directly (already created in __init__)

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()
            MockWriter.return_value.append = MagicMock()

            ingestor.ingest(contract, dst_contract)

        mock_enricher.enrich.assert_called_once()

    def test_ingest_calls_writer_append(self, ingestor):
        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()
        mock_df      = _mock_batch_df()

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()

            ingestor.ingest(contract, dst_contract)

        MockWriter.return_value.append.assert_called_once()

    def test_ingest_returns_row_count(self, ingestor):
        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()
        mock_df      = _mock_batch_df(n_rows=42)

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()

            rows = ingestor.ingest(contract, dst_contract)

        assert rows == 42

    def test_ingest_logs_ops_on_success(self, spark, env):
        mock_ops = MagicMock()
        mock_ops.log_start.return_value = "abc123"
        ingestor = BronzeIngestor(spark=spark, env=env, ops=mock_ops)

        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()
        mock_df      = _mock_batch_df()

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()

            ingestor.ingest(contract, dst_contract)

        mock_ops.log_start.assert_called_once_with("test_dataset")
        mock_ops.log_success.assert_called_once()

    def test_ingest_logs_ops_on_failure(self, spark, env):
        mock_ops = MagicMock()
        mock_ops.log_start.return_value = "abc123"
        ingestor = BronzeIngestor(spark=spark, env=env, ops=mock_ops)

        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory:
            MockFactory.create.side_effect = RuntimeError("reader failed")
            with pytest.raises(RuntimeError):
                ingestor.ingest(contract, dst_contract)

        mock_ops.log_failure.assert_called_once()

    def test_ingest_all_continues_on_error(self, ingestor):
        """ingest_all devuelve nombres de fallidos pero no lanza excepción."""
        c1 = _make_ingestion_contract("dataset_a")
        c2 = _make_ingestion_contract("dataset_b")
        dst = {
            "dataset_a": _make_table_contract("a"),
            "dataset_b": _make_table_contract("b"),
        }

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory:
            # dataset_a falla, dataset_b también (sin df válido)
            MockFactory.create.side_effect = RuntimeError("all fail")
            failed = ingestor.ingest_all([c1, c2], dst)

        assert "dataset_a" in failed
        assert "dataset_b" in failed

    def test_ingest_all_no_dst_contract_omitted(self, ingestor):
        """Datasets sin TableContract destino se omiten, no aparecen en fallidos."""
        c1 = _make_ingestion_contract("huerfano")
        failed = ingestor.ingest_all([c1], dst_contracts={})
        assert "huerfano" not in failed

    # ── Partition overwrite ────────────────────────────────────────────────

    def test_ingest_uses_partition_overwrite_when_partitioned(self, ingestor):
        """Si el TableContract tiene _ingested_date como partición → overwrite_partition."""
        contract     = _make_ingestion_contract()
        dst_contract = _make_partitioned_table_contract()
        mock_df      = _mock_batch_df()
        mock_df.columns = ["id", "data", "_ingested_date"]  # columna presente en df

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()

            ingestor.ingest(contract, dst_contract)

        MockWriter.return_value.overwrite_partition.assert_called_once()
        MockWriter.return_value.append.assert_not_called()

    def test_ingest_falls_back_to_append_when_no_ingestion_partition(self, ingestor):
        """Si el TableContract no tiene _ingested_date como partición → append."""
        contract     = _make_ingestion_contract()
        dst_contract = _make_table_contract()  # partitions=()
        mock_df      = _mock_batch_df()

        with patch("DKOps.ingestion.bronze_ingestor.SourceReaderFactory") as MockFactory, \
             patch("DKOps.ingestion.bronze_ingestor.SchemaValidator")     as MockVal, \
             patch("DKOps.ingestion.bronze_ingestor.TableWriter")         as MockWriter:

            mock_reader = MagicMock()
            mock_reader.read.return_value = mock_df
            MockFactory.create.return_value = mock_reader
            MockVal.return_value.validate.return_value.raise_if_critical = MagicMock()

            ingestor.ingest(contract, dst_contract)

        MockWriter.return_value.append.assert_called_once()
        MockWriter.return_value.overwrite_partition.assert_not_called()

    def test_find_ingestion_partition_returns_ingested_date(self):
        """_find_ingestion_partition detecta _ingested_date correctamente."""
        dst = _make_partitioned_table_contract()
        result = BronzeIngestor._find_ingestion_partition(dst)
        assert result == "_ingested_date"

    def test_find_ingestion_partition_returns_none_when_no_partition(self):
        """_find_ingestion_partition devuelve None si no hay partición de ingesta."""
        dst = _make_table_contract()  # partitions=()
        result = BronzeIngestor._find_ingestion_partition(dst)
        assert result is None


class TestOpsLogger:
    """
    Tests del IngestionOpsLogger con mocks de Spark.

    Aqui solo se comprueba lo que un mock puede demostrar: que cada metodo arma
    la fila que le toca. El esquema en si no se puede aseverar bajo mocks
    —_OPS_SCHEMA se construye con StructType mockeados—, y que la fila SEA
    ESCRIBIBLE por
    Spark es justo lo que los mocks no pueden ver —createDataFrame sobre un
    MagicMock nunca falla— y por eso el issue #28 paso inadvertido. Esa parte
    vive en tests/integration/test_ops_logger_spark.py.
    """

    @staticmethod
    def _logger(spark=None):
        """IngestionOpsLogger sin tocar _ensure_table ni el Launcher."""
        from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger

        ops = IngestionOpsLogger.__new__(IngestionOpsLogger)
        ops._spark    = spark or MagicMock()
        ops._ops_path = "/tmp/test_ops"
        ops._pipeline = "test"
        ops._started  = {}
        return ops

    def test_log_start_devuelve_un_run_id_de_8_caracteres(self):
        ops = self._logger()

        with patch.object(ops, "_write_row") as write:
            run_id = ops.log_start("ventas")

        assert isinstance(run_id, str) and len(run_id) == 8
        fila = write.call_args.kwargs
        assert fila["run_id"]  == run_id
        assert fila["dataset"] == "ventas"
        assert fila["status"]  == "STARTED"
        assert fila["started_at"] is not None

    def test_cada_log_start_devuelve_un_run_id_distinto(self):
        ops = self._logger()
        with patch.object(ops, "_write_row"):
            assert ops.log_start("a") != ops.log_start("b")

    def test_el_cierre_repite_el_started_at_de_su_run_id(self):
        """Issue #28 — la tabla debe quedar autocontenida, sin self-join."""
        ops = self._logger()

        with patch.object(ops, "_write_row") as write:
            run_id  = ops.log_start("ventas")
            inicio  = write.call_args.kwargs["started_at"]
            ops.log_success(run_id, "ventas", rows_written=10)
            cierre  = write.call_args.kwargs

        assert cierre["status"]      == "SUCCESS"
        assert cierre["started_at"]  == inicio
        assert cierre["finished_at"] is not None

    def test_el_cierre_por_fallo_tambien_repite_el_started_at(self):
        ops = self._logger()

        with patch.object(ops, "_write_row") as write:
            run_id = ops.log_start("ventas")
            inicio = write.call_args.kwargs["started_at"]
            ops.log_failure(run_id, "ventas", ValueError("boom"))
            cierre = write.call_args.kwargs

        assert cierre["status"]     == "FAILED"
        assert cierre["started_at"] == inicio
        assert "ValueError" in cierre["notes"]

    def test_started_at_es_none_si_el_run_id_es_desconocido(self):
        """Proceso reiniciado entre apertura y cierre: la fila no se pierde."""
        ops = self._logger()

        with patch.object(ops, "_write_row") as write:
            ops.log_success("run-de-otro-proceso", "ventas", rows_written=3)

        assert write.call_args.kwargs["started_at"] is None

    def test_el_run_id_se_olvida_tras_cerrarlo(self):
        """Sin esto el diccionario creceria sin limite en un proceso largo."""
        ops = self._logger()

        with patch.object(ops, "_write_row"):
            run_id = ops.log_start("ventas")
            assert run_id in ops._started
            ops.log_success(run_id, "ventas")

        assert ops._started == {}

    def test_un_fallo_de_escritura_se_registra_como_error(self):
        """Issue #28 — degradarlo a warning fue lo que oculto el bug."""
        spark = MagicMock()
        spark.createDataFrame.side_effect = RuntimeError("schema rechazado")

        from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
        from unittest.mock import PropertyMock

        ops       = self._logger(spark)
        log_mock  = MagicMock()

        with patch.object(
            IngestionOpsLogger, "log", new_callable=PropertyMock,
            return_value=log_mock,
        ):
            ops._write_row(run_id="a1", dataset="ventas", status="SUCCESS")

        assert log_mock.error.called, "el fallo debe salir como ERROR, no warning"
        mensaje = str(log_mock.error.call_args)
        assert "RuntimeError" in mensaje
        assert "SUCCESS" in mensaje


# ── Metadata del contrato en el camino streaming (issue #18) ─────────────────

class TestStreamingContractMetadata:
    """
    La escritura streaming crea la tabla con `writeStream.toTable()`, sin pasar
    por los writers de table_governance. Debe aplicar igualmente la metadata
    declarada en el TableContract.
    """

    @pytest.fixture
    def env(self):
        env = MagicMock()
        env._is_databricks = True
        env.has_path.return_value = False
        return env

    @pytest.fixture
    def ingestor(self, env) -> BronzeIngestor:
        return BronzeIngestor(spark=MagicMock(), env=env)

    def _mock_stream_df(self):
        df = MagicMock()
        df.isStreaming = True
        return df

    def test_write_stream_aplica_metadata_del_contrato(self, ingestor):
        contract     = _make_ingestion_contract(ingest_type="streaming")
        dst_contract = _make_partitioned_table_contract()

        with patch(
            "DKOps.ingestion.bronze_ingestor.TableWriter"
        ) as mock_tw:
            ingestor._write_stream(self._mock_stream_df(), contract, dst_contract)

        mock_tw.return_value.apply_contract_metadata.assert_called_once()

    def test_write_stream_construye_el_writer_con_el_contrato_destino(self, ingestor):
        contract     = _make_ingestion_contract(ingest_type="streaming")
        dst_contract = _make_partitioned_table_contract()

        with patch(
            "DKOps.ingestion.bronze_ingestor.TableWriter"
        ) as mock_tw:
            ingestor._write_stream(self._mock_stream_df(), contract, dst_contract)

        assert mock_tw.call_args.args[0] is dst_contract


# ── EXTERNAL + location en el camino streaming (issue #26) ───────────────────

class TestStreamingExternalLocation:
    """
    `writeStream.toTable()` crea la tabla desde el esquema del DataFrame e
    ignora `type: EXTERNAL` y `location`, dejandola MANAGED en el
    almacenamiento interno de Unity Catalog.
    """

    @pytest.fixture
    def env(self):
        env = MagicMock()
        env._is_databricks = True
        env.has_path.return_value = False
        return env

    @pytest.fixture
    def ingestor(self, env) -> BronzeIngestor:
        return BronzeIngestor(spark=MagicMock(), env=env)

    def _external_contract(self, location="abfss://ct@sa/bronze/batch/ventas_raw"):
        return TableContract(
            catalog  = "bronze",
            schema   = "batch",
            name     = "ventas_raw",
            type     = "EXTERNAL",
            format   = "DELTA",
            location = location,
            columns  = (
                ColumnContract(name="id",   type="STRING"),
                ColumnContract(name="data", type="STRING"),
            ),
            partitions  = (),
            permissions = (),
        )

    def _stream_df(self):
        df = MagicMock()
        df.isStreaming = True
        # Cadena fluida del DataStreamWriter
        w = MagicMock()
        for attr in ("format", "option", "partitionBy", "trigger"):
            getattr(w, attr).return_value = w
        df.writeStream = w
        return df, w

    def test_pasa_path_cuando_la_tabla_no_existe(self, ingestor):
        contract     = _make_ingestion_contract(ingest_type="streaming")
        dst_contract = self._external_contract()
        df, w        = self._stream_df()

        with patch("DKOps.ingestion.bronze_ingestor.TableWriter"), \
             patch.object(ingestor, "_table_exists", return_value=False):
            ingestor._write_stream(df, contract, dst_contract)

        opts = [str(c) for c in w.option.call_args_list]
        assert any("path" in c and dst_contract.location in c for c in opts), (
            f"Se esperaba .option('path', '{dst_contract.location}'). Calls: {opts}"
        )

    def test_no_pasa_path_si_la_tabla_ya_existe(self, ingestor):
        contract     = _make_ingestion_contract(ingest_type="streaming")
        dst_contract = self._external_contract()
        df, w        = self._stream_df()

        with patch("DKOps.ingestion.bronze_ingestor.TableWriter"), \
             patch.object(ingestor, "_table_exists", return_value=True):
            ingestor._write_stream(df, contract, dst_contract)

        opts = [str(c) for c in w.option.call_args_list]
        assert not any(dst_contract.location in c for c in opts)

    def test_managed_no_pasa_path(self, ingestor):
        contract     = _make_ingestion_contract(ingest_type="streaming")
        dst_contract = _make_partitioned_table_contract()   # MANAGED
        df, w        = self._stream_df()

        with patch("DKOps.ingestion.bronze_ingestor.TableWriter"), \
             patch.object(ingestor, "_table_exists", return_value=False):
            ingestor._write_stream(df, contract, dst_contract)

        opts = [str(c) for c in w.option.call_args_list]
        assert not any("'path'" in c for c in opts)
