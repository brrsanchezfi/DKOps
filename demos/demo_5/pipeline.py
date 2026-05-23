"""
pipeline.py — Pipeline completo para Demo 5: Landing -> Bronze -> Silver -> Gold

Ejecuta de forma secuencial:
  0. Generacion de datos sinteticos en Landing
  1. Inicializacion DKOps
  2. Ingesta batch (Landing -> Bronze): ventas_diarias, clientes
  3. Ingesta streaming (Landing -> Bronze): eventos_app con availableNow
  4. Promocion Silver: ventas_current (CDC), clientes_current (full merge)
  5. Agregaciones Silver -> Gold: revenue_diario, engagement_clientes
  6. Validacion final

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_5/pipeline.py

El pipeline puede ejecutarse multiples veces — es idempotente por diseno:
  - Batch usa partition overwrite (_ingested_date)
  - Silver usa upsert (no inserta duplicados)
  - Streaming usa checkpoints (no reprocesa archivos ya ingestados)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Asegurar que src/ y el directorio del demo esten en el path
ROOT     = Path(__file__).resolve().parents[2]
SRC      = ROOT / "src"
DEMO_DIR = Path(__file__).resolve().parent  # demos/demo_5/
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(DEMO_DIR) not in sys.path:           # permite "from datagen.main import ..."
    sys.path.insert(0, str(DEMO_DIR))
CONFIG_PATH = DEMO_DIR / "config" / "config.json"
LANDING     = "/tmp/dkops_demo5/landing"
OPS_PATH    = "/tmp/dkops_demo5/ops/control"


def main() -> None:
    print("\n" + "=" * 65)
    print("  DKOps Demo 5 — Landing -> Bronze -> Silver -> Gold")
    print("=" * 65)

    # ── 0. Generar datos sinteticos ───────────────────────────────────
    print("\n[0/6] Generando datos en Landing...")
    from datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2)

    # ── 1. Inicializar Spark ──────────────────────────────────────────
    print("\n[1/6] Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ── 2. Crear IngestionEngine ──────────────────────────────────────
    print("\n[2/6] Construyendo IngestionEngine...")
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
    print(f"  Bronze contracts: {len(engine._bronze_contracts)}")
    print(f"  Silver contracts: {len(engine._silver_contracts)}")

    # ── 3. Ingesta batch Landing -> Bronze ────────────────────────────
    print("\n[3/6] Ingesta batch (Landing -> Bronze)...")
    failed_batch = engine.ingest_bronze()
    if failed_batch:
        print(f"  WARN Fallidos: {failed_batch}")
    else:
        print("  OK Ingesta batch completada")

    # ── 4. Ingesta streaming Landing -> Bronze ────────────────────────
    print("\n[4/6] Ingesta streaming con availableNow (Landing -> Bronze)...")
    engine.run_streaming()
    print("  OK Streaming completado")

    # ── 5. Promocion Bronze -> Silver ─────────────────────────────────
    print("\n[5/6] Promocion Silver (Bronze -> Silver)...")
    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  WARN Fallidos Silver: {failed_silver}")
    else:
        print("  OK Promocion Silver completada")

    # ── 6. Agregaciones Silver -> Gold ────────────────────────────────
    print("\n[6/6] Agregaciones Silver -> Gold...")
    from DKOps.table_governance import load_contract, TableWriter

    ct_gold_revenue    = load_contract(str(DEMO_DIR / "tables" / "gold" / "revenue_diario.json"))
    ct_gold_engagement = load_contract(str(DEMO_DIR / "tables" / "gold" / "engagement_clientes.json"))

    ct_s_ventas   = load_contract(str(DEMO_DIR / "tables" / "silver" / "ventas_current.json"))
    ct_s_clientes = load_contract(str(DEMO_DIR / "tables" / "silver" / "clientes_current.json"))

    ventas_table   = ct_s_ventas.effective_name
    clientes_table = ct_s_clientes.effective_name

    # Verificar que Silver existe antes de calcular Gold
    try:
        n_ventas   = spark.read.table(ventas_table).count()
        n_clientes = spark.read.table(clientes_table).count()
        print(f"  Silver ventas_current: {n_ventas} filas")
        print(f"  Silver clientes_current: {n_clientes} filas")
    except Exception as e:
        print(f"  WARN Silver no disponible: {e}")
        n_ventas = 0

    if n_ventas > 0:
        # Gold: revenue diario por canal
        print("  Calculando gold_revenue_diario...")
        df_revenue = spark.sql(f"""
            SELECT
                canal,
                COUNT(*)                                                        AS total_ventas,
                SUM(CASE WHEN is_deleted IS NULL OR NOT is_deleted THEN 1 ELSE 0 END)
                                                                               AS ventas_activas,
                ROUND(SUM(COALESCE(precio_total, 0)), 2)                        AS revenue_total,
                ROUND(AVG(precio_total), 2)                                     AS revenue_promedio,
                SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END)           AS ventas_canceladas,
                ROUND(
                    SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                )                                                               AS pct_canceladas
            FROM {ventas_table}
            WHERE canal IS NOT NULL
            GROUP BY canal
            ORDER BY revenue_total DESC
        """)
        TableWriter(ct_gold_revenue).overwrite(df_revenue)
        print(f"    Filas gold_revenue_diario: {df_revenue.count()}")

        # Gold: engagement de clientes
        print("  Calculando gold_engagement_clientes...")
        df_engagement = spark.sql(f"""
            SELECT
                v.cliente_id,
                COUNT(v.venta_id)              AS total_ventas,
                ROUND(SUM(v.precio_total), 2)  AS revenue_cliente,
                0                              AS total_eventos,
                FIRST(v.canal)                 AS canal_preferido
            FROM {ventas_table} v
            WHERE v.cliente_id IS NOT NULL
              AND (v.is_deleted IS NULL OR NOT v.is_deleted)
            GROUP BY v.cliente_id
            ORDER BY revenue_cliente DESC
        """)
        TableWriter(ct_gold_engagement).overwrite(df_engagement)
        print(f"    Filas gold_engagement_clientes: {df_engagement.count()}")

        # Mostrar Gold
        print("\n  -- Revenue por canal --")
        spark.sql(f"""
            SELECT canal, total_ventas, ventas_activas, revenue_total, revenue_promedio
            FROM {ct_gold_revenue.effective_name}
            ORDER BY revenue_total DESC
        """).show(truncate=False)

        print("  -- Top 5 clientes por revenue --")
        spark.sql(f"""
            SELECT cliente_id, total_ventas, revenue_cliente, canal_preferido
            FROM {ct_gold_engagement.effective_name}
            ORDER BY revenue_cliente DESC
            LIMIT 5
        """).show(truncate=False)
    else:
        print("  (Silver vacio — ejecuta el pipeline nuevamente para ver Gold)")

    # ── Validacion final ──────────────────────────────────────────────
    print("\n-- Validacion final -----------------------------------------")
    engine.status()

    # Tabla de control operativo
    print("\n-- Tabla de control operativo -------------------------------")
    try:
        ops_df = engine.ops.read()
        ops_df.select(
            "run_id", "dataset", "status", "rows_written", "started_at"
        ).show(truncate=False)
    except Exception as exc:
        print(f"  (ops table: {exc})")

    print("\n" + "=" * 65)
    print("  Demo 5 completado.")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
