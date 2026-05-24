"""
pipeline.py — Demo 2: Manufactura — Landing -> Bronze -> Silver -> Gold

Flujo Lakehouse completo para dominio de manufactura de productos de aseo.
Integra el motor de ingesta con el modulo de calidad de datos existente.

Fases:
  0 — Genera datos en Landing (lotes JSON, ordenes CDC JSON, ventas CSV)
  1 — Inicializa DKOps
  2 — Landing -> Bronze (ingestion motor: incremental, cdc, incremental)
  3 — Bronze -> Silver (incremental_replace, cdc_merge, full_merge)
  4 — Silver -> Gold (KPI eficiencia planta, KPI calidad lotes)
  5 — Validacion final con DQ engine existente

Caracteristicas especiales:
  - Mantiene el DQ engine existente (demos/demo_2/dq/)
  - Mantiene las funciones de transformacion (transformations/)
  - Bronze ahora es poblado por el motor de ingesta (no DataFrames hardcoded)
  - Silver strategies: incremental_replace (lotes), cdc_merge (ordenes), full_merge (ventas)

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_2/pipeline.py
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
LANDING     = "/tmp/dkops_demo2/landing"
OPS_PATH    = "/tmp/dkops_demo2/ops/control"


def print_header(title: str) -> None:
    print(f"\n{'='*72}")
    print(f"  {title}")
    print('='*72)


def print_phase(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")
    print("-"*60)


def main() -> None:
    print_header("DKOps Demo 2 — Manufactura: Landing -> Bronze -> Silver -> Gold")

    # ---- Fase 0: Generar datos en Landing ----------------------------------
    print_phase(0, "Simulando llegada de datos al Landing...")
    from datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2, n_lotes=150, n_ordenes=100, n_ventas=200)

    # ---- Fase 1: Inicializar Spark -----------------------------------------
    print_phase(1, "Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ---- Fase 2: Landing -> Bronze -----------------------------------------
    print_phase(2, "Ingesta Landing -> Bronze (motor de ingesta)...")
    from DKOps.ingestion.engine import IngestionEngine

    engine = IngestionEngine.from_spark(
        spark                = spark,
        env                  = env,
        bronze_contracts_dir = str(DEMO_DIR / "ingestion" / "batch"),
        silver_contracts_dir = str(DEMO_DIR / "ingestion" / "silver"),
        tables_base_dir      = str(DEMO_DIR),
        ops_path             = OPS_PATH,
    )
    print(f"  Bronze contracts: {len(engine._bronze_contracts)}")
    print(f"  Silver contracts: {len(engine._silver_contracts)}")

    failed_bronze = engine.ingest_bronze()
    if failed_bronze:
        print(f"  WARN Fallidos Bronze: {failed_bronze}")
    else:
        print("  OK Ingesta Bronze completada (lotes, ordenes, ventas)")

    # ---- Fase 3: Bronze -> Silver ------------------------------------------
    print_phase(3, "Promocion Bronze -> Silver (incremental_replace / cdc_merge / full_merge)...")
    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  WARN Fallidos Silver: {failed_silver}")
    else:
        print("  OK Promocion Silver completada")

    # Mostrar estado de tablas
    engine.status()

    # ---- Fase 4: Silver -> Gold --------------------------------------------
    print_phase(4, "Agregaciones Silver -> Gold (KPIs manufactura)...")
    from DKOps.table_governance import load_contract, TableWriter

    ct_gold_eficiencia = load_contract(str(DEMO_DIR / "tables" / "gold" / "eficiencia_planta.json"))
    ct_gold_calidad    = load_contract(str(DEMO_DIR / "tables" / "gold" / "calidad_lotes.json"))

    ct_s_ordenes = load_contract(str(DEMO_DIR / "tables" / "silver_new" / "ordenes_current.json"))
    ct_s_lotes   = load_contract(str(DEMO_DIR / "tables" / "silver_new" / "lotes_current.json"))
    ct_s_ventas  = load_contract(str(DEMO_DIR / "tables" / "silver_new" / "ventas_current.json"))

    ordenes_table = ct_s_ordenes.effective_name
    lotes_table   = ct_s_lotes.effective_name
    ventas_table  = ct_s_ventas.effective_name

    # KPI Eficiencia Planta
    print("  Calculando gold_eficiencia_planta...")
    df_eficiencia = spark.sql(f"""
        SELECT
            linea_id,
            COUNT(*)                                                         AS total_ordenes,
            SUM(CASE WHEN estado = 'COMPLETED'   THEN 1 ELSE 0 END)         AS ordenes_completadas,
            SUM(CASE WHEN estado = 'CANCELLED'   THEN 1 ELSE 0 END)         AS ordenes_canceladas,
            SUM(CASE WHEN estado = 'IN_PROGRESS' THEN 1 ELSE 0 END)         AS ordenes_en_curso,
            ROUND(
                SUM(CASE WHEN estado = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            )                                                                AS tasa_cumplimiento,
            SUM(COALESCE(cantidad_planeada, 0))                              AS unidades_planeadas,
            SUM(COALESCE(cantidad_real, 0))                                  AS unidades_producidas,
            ROUND(
                SUM(COALESCE(cantidad_real, 0)) * 100.0
                / NULLIF(SUM(COALESCE(cantidad_planeada, 0)), 0), 2
            )                                                                AS eficiencia_pct
        FROM {ordenes_table}
        WHERE linea_id IS NOT NULL
          AND (is_deleted IS NULL OR is_deleted = false)
        GROUP BY linea_id
        ORDER BY linea_id
    """)
    TableWriter(ct_gold_eficiencia).overwrite(df_eficiencia)
    print(f"    Filas gold_eficiencia_planta: {df_eficiencia.count()}")

    # KPI Calidad Lotes
    print("  Calculando gold_calidad_lotes...")
    df_calidad = spark.sql(f"""
        SELECT
            producto_id,
            COUNT(*)                                                              AS total_lotes,
            SUM(CASE WHEN resultado_qc = 'APPROVED' THEN 1 ELSE 0 END)           AS lotes_aprobados,
            SUM(CASE WHEN resultado_qc = 'REJECTED' THEN 1 ELSE 0 END)           AS lotes_rechazados,
            SUM(CASE WHEN resultado_qc = 'RETEST'   THEN 1 ELSE 0 END)           AS lotes_retest,
            ROUND(
                SUM(CASE WHEN resultado_qc = 'APPROVED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            )                                                                     AS tasa_aprobacion,
            ROUND(
                AVG(cantidad_defectuosa * 100.0 / NULLIF(cantidad_producida, 0)), 2
            )                                                                     AS merma_pct_prom,
            ROUND(AVG(ph_medido), 3)                                              AS ph_prom
        FROM {lotes_table}
        WHERE producto_id IS NOT NULL
        GROUP BY producto_id
        ORDER BY tasa_aprobacion ASC
    """)
    TableWriter(ct_gold_calidad).overwrite(df_calidad)
    print(f"    Filas gold_calidad_lotes: {df_calidad.count()}")

    # ---- Fase 5: Validacion y reporte -------------------------------------
    print_phase(5, "Validacion final...")

    gold_eficiencia_name = ct_gold_eficiencia.effective_name
    gold_calidad_name    = ct_gold_calidad.effective_name

    print("\n  -- Eficiencia por linea --")
    spark.sql(f"""
        SELECT linea_id, total_ordenes, ordenes_completadas, tasa_cumplimiento, eficiencia_pct
        FROM {gold_eficiencia_name}
        ORDER BY eficiencia_pct DESC
    """).show(truncate=False)

    print("\n  -- Top 5 productos con mejor tasa de aprobacion QC --")
    spark.sql(f"""
        SELECT producto_id, total_lotes, lotes_aprobados, tasa_aprobacion, merma_pct_prom
        FROM {gold_calidad_name}
        ORDER BY tasa_aprobacion DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n  -- Resumen ventas por distribuidor --")
    spark.sql(f"""
        SELECT distribuidor_id, COUNT(*) AS ventas,
               SUM(cantidad) AS unidades_totales,
               ROUND(SUM(cantidad * precio_unitario), 2) AS monto_total
        FROM {ventas_table}
        GROUP BY distribuidor_id
        ORDER BY monto_total DESC
        LIMIT 5
    """).show(truncate=False)

    n_lotes   = spark.read.table(lotes_table).count()
    n_ordenes = spark.read.table(ordenes_table).count()
    n_ventas  = spark.read.table(ventas_table).count()

    print(f"""
  +--------------------------------------------------+
  |    RESUMEN LAKEHOUSE — Demo 2: Manufactura       |
  +--------------------------------------------------+
  |  Silver lotes_current     : {n_lotes:>6}                 |
  |  Silver ordenes_current   : {n_ordenes:>6}                 |
  |  Silver ventas_current    : {n_ventas:>6}                 |
  |  Gold eficiencia_planta   : {df_eficiencia.count():>6}                 |
  |  Gold calidad_lotes       : {df_calidad.count():>6}                 |
  +--------------------------------------------------+
    """)

    print("\n" + "="*72)
    print("  Demo 2 completado.")
    print("="*72 + "\n")


if __name__ == "__main__":
    main()
