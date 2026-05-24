"""
pipeline.py — Demo 4: Retail/Inventario — Landing -> Bronze -> Silver -> Gold

Flujo Lakehouse completo para dominio de gestion de inventario retail.

Fases:
  0 — Genera datos en Landing (productos CSV, movimientos JSON, alertas streaming)
  1 — Inicializa DKOps
  2 — Landing -> Bronze (batch: productos + movimientos; streaming: alertas)
  3 — Bronze -> Silver (full_merge, append_dedup, append_dedup)
  4 — Silver -> Gold (stock_actual via CDF, alertas_criticas)
  5 — Validacion: TableReader.read_cdf(), read_stream(), SafeMigrator plan

Caracteristicas especiales:
  - TableReader.read_cdf() en Silver productos_current para detectar cambios
  - TableReader.read_stream() demo en Silver movimientos_current
  - SafeMigrator dry_run como plan de migracion
  - Streaming con availableNow para alertas

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_4/pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[2]
SRC      = ROOT / "src"
DEMO_DIR = Path(__file__).resolve().parent

if str(SRC)      not in sys.path: sys.path.insert(0, str(SRC))
if str(DEMO_DIR) not in sys.path: sys.path.insert(0, str(DEMO_DIR))

CONFIG_PATH = DEMO_DIR / "config" / "config.json"
LANDING     = "/tmp/dkops_demo4/landing"
OPS_PATH    = "/tmp/dkops_demo4/ops/control"


def print_header(title: str) -> None:
    print(f"\n{'='*72}")
    print(f"  {title}")
    print('='*72)


def print_phase(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")
    print("-"*60)


def main() -> None:
    print_header("DKOps Demo 4 — Retail/Inventario: Landing -> Bronze -> Silver -> Gold")

    # ---- Fase 0: Generar datos en Landing ----------------------------------
    print_phase(0, "Simulando llegada de datos al Landing...")
    from datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2, n_movimientos=100, n_alertas=30)

    # ---- Fase 1: Inicializar Spark -----------------------------------------
    print_phase(1, "Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ---- Fase 2: Landing -> Bronze -----------------------------------------
    print_phase(2, "Ingesta Landing -> Bronze...")
    from DKOps.ingestion.engine import IngestionEngine

    engine = IngestionEngine.from_spark(
        spark                   = spark,
        env                     = env,
        bronze_contracts_dir    = str(DEMO_DIR / "ingestion" / "batch"),
        streaming_contracts_dir = str(DEMO_DIR / "ingestion" / "streaming"),
        silver_contracts_dir    = str(DEMO_DIR / "ingestion" / "silver"),
        tables_base_dir         = str(DEMO_DIR),
        ops_path                = OPS_PATH,
    )

    failed_batch = engine.ingest_bronze()
    if failed_batch:
        print(f"  WARN Fallidos batch: {failed_batch}")
    else:
        print("  OK Ingesta batch completada (productos, movimientos)")

    print("\n  Ejecutando streaming con availableNow (alertas)...")
    engine.run_streaming(timeout=120)
    print("  OK Streaming completado")

    # ---- Fase 3: Bronze -> Silver ------------------------------------------
    print_phase(3, "Promocion Bronze -> Silver...")
    print("  Estrategias: full_merge (productos), append_dedup (movimientos, alertas)")

    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  WARN Fallidos Silver: {failed_silver}")
    else:
        print("  OK Promocion Silver completada")

    # ---- Fase 4: Silver -> Gold via TableReader.read_cdf() ----------------
    print_phase(4, "Agregaciones Silver -> Gold (via TableReader + read_cdf)...")
    from DKOps.table_governance import load_contract, TableWriter, TableReader, SafeMigrator

    ct_s_productos   = load_contract(str(DEMO_DIR / "tables" / "silver" / "productos_current.json"))
    ct_s_movimientos = load_contract(str(DEMO_DIR / "tables" / "silver" / "movimientos_current.json"))
    ct_s_alertas     = load_contract(str(DEMO_DIR / "tables" / "silver" / "alertas_current.json"))

    ct_gold_stock    = load_contract(str(DEMO_DIR / "tables" / "gold" / "stock_actual.json"))
    ct_gold_alertas  = load_contract(str(DEMO_DIR / "tables" / "gold" / "alertas_criticas.json"))

    prods_table  = ct_s_productos.effective_name
    movs_table   = ct_s_movimientos.effective_name
    alerts_table = ct_s_alertas.effective_name

    # Demostrar TableReader.read_cdf() en productos_current
    print("\n  Demostrar TableReader.read_cdf() en productos_current...")
    reader_prods = TableReader(ct_s_productos)
    try:
        # Forzar un upsert para crear una version nueva
        df_prods = reader_prods.read()
        print(f"    Productos en Silver: {df_prods.count()}")

        # Obtener version actual
        version_actual = spark.sql(
            f"DESCRIBE HISTORY {prods_table}"
        ).select("version").first()[0]
        print(f"    Version actual de productos_current: {version_actual}")

        if version_actual >= 1:
            df_cdf = reader_prods.read_cdf(starting_version=0)
            print(f"    Entradas CDF (v0 -> {version_actual}): {df_cdf.count()}")
            print("    Tipos de cambio:")
            df_cdf.groupBy("_change_type").count().show()
        else:
            print("    (Solo v0 disponible — ejecuta el pipeline 2 veces para ver CDF)")
    except Exception as e:
        print(f"    CDF: {e}")

    # Demostrar TableReader.read_stream()
    print("\n  Demostrar TableReader.read_stream() en movimientos_current...")
    reader_movs = TableReader(ct_s_movimientos)
    stream_df = reader_movs.read_stream()
    print(f"    stream_df.isStreaming = {stream_df.isStreaming}")
    batch_stats = {"n": 0, "rows": 0}

    def procesar_batch(batch_df, batch_id: int) -> None:
        n = batch_df.count()
        batch_stats["n"]    += 1
        batch_stats["rows"] += n
        print(f"      Micro-batch {batch_id}: {n} movimientos")

    query = (
        stream_df.writeStream
        .foreachBatch(procesar_batch)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(timeout=60)
    print(f"    Stream procesado: {batch_stats['n']} batches | {batch_stats['rows']} filas")

    # KPI Stock Actual (JOIN movimientos + productos Silver)
    print("\n  Calculando gold_stock_actual...")
    df_stock = spark.sql(f"""
        SELECT
            m.producto_id,
            p.nombre,
            p.categoria,
            p.precio_usd,
            SUM(CASE WHEN m.cantidad > 0 THEN m.cantidad ELSE 0 END)    AS total_entradas,
            SUM(CASE WHEN m.cantidad < 0 THEN ABS(m.cantidad) ELSE 0 END) AS total_salidas,
            SUM(m.cantidad)                                              AS stock_neto,
            COUNT(*)                                                     AS n_movimientos
        FROM {movs_table} m
        LEFT JOIN {prods_table} p ON m.producto_id = p.producto_id
        WHERE m.producto_id IS NOT NULL
        GROUP BY m.producto_id, p.nombre, p.categoria, p.precio_usd
        ORDER BY stock_neto ASC
    """)
    TableWriter(ct_gold_stock).overwrite(df_stock)
    print(f"    Filas gold_stock_actual: {df_stock.count()}")

    # KPI Alertas Criticas
    print("  Calculando gold_alertas_criticas...")
    df_alertas_gold = spark.sql(f"""
        SELECT
            producto_id,
            almacen_id,
            COUNT(*)                                                           AS total_alertas,
            SUM(CASE WHEN severidad = 'CRITICA' THEN 1 ELSE 0 END)             AS alertas_criticas,
            SUM(CASE WHEN severidad = 'ALTA'    THEN 1 ELSE 0 END)             AS alertas_alta,
            MAX(deficit)                                                       AS deficit_max,
            MIN(stock_actual)                                                  AS stock_min_obs
        FROM {alerts_table}
        WHERE producto_id IS NOT NULL AND almacen_id IS NOT NULL
        GROUP BY producto_id, almacen_id
        ORDER BY alertas_criticas DESC, deficit_max DESC
    """)
    TableWriter(ct_gold_alertas).overwrite(df_alertas_gold)
    print(f"    Filas gold_alertas_criticas: {df_alertas_gold.count()}")

    # SafeMigrator dry_run
    print("\n  SafeMigrator dry_run en productos_current:")
    SafeMigrator(ct_s_productos, dry_run=True).apply()

    # ---- Fase 5: Validacion -----------------------------------------------
    print_phase(5, "Validacion final...")
    engine.status()

    gold_stock_name   = ct_gold_stock.effective_name
    gold_alerts_name  = ct_gold_alertas.effective_name

    print("\n  -- Top 5 productos con menos stock --")
    spark.sql(f"""
        SELECT producto_id, nombre, categoria, stock_neto, n_movimientos
        FROM {gold_stock_name}
        ORDER BY stock_neto ASC
        LIMIT 5
    """).show(truncate=False)

    print("\n  -- Productos con mas alertas criticas --")
    spark.sql(f"""
        SELECT producto_id, almacen_id, alertas_criticas, deficit_max, stock_min_obs
        FROM {gold_alerts_name}
        WHERE alertas_criticas > 0
        ORDER BY alertas_criticas DESC
        LIMIT 5
    """).show(truncate=False)

    n_prods  = spark.read.table(prods_table).count()
    n_movs   = spark.read.table(movs_table).count()
    n_alerts = spark.read.table(alerts_table).count()

    print(f"""
  +--------------------------------------------------+
  |   RESUMEN LAKEHOUSE — Demo 4: Retail/Inventario  |
  +--------------------------------------------------+
  |  Silver productos_current  : {n_prods:>6}                 |
  |  Silver movimientos_current: {n_movs:>6}                 |
  |  Silver alertas_current    : {n_alerts:>6}                 |
  |  Gold stock_actual         : {df_stock.count():>6}                 |
  |  Gold alertas_criticas     : {df_alertas_gold.count():>6}                 |
  |  TableReader.read_cdf()    : demostrado                     |
  |  TableReader.read_stream() : demostrado                     |
  +--------------------------------------------------+
    """)

    print("\n" + "="*72)
    print("  Demo 4 completado.")
    print("="*72 + "\n")


if __name__ == "__main__":
    main()
