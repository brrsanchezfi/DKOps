"""
pipeline.py — Pipeline completo de ingesta para Demo 5.

Ejecuta de forma secuencial:
  1. Generación de datos sintéticos en Landing
  2. Ingesta batch (Landing → Bronze): ventas_diarias, clientes
  3. Ingesta streaming (Landing → Bronze): eventos_app con availableNow
  4. Promoción Silver: ventas_current (CDC), clientes_current (full merge)
  5. Validación final (conteo de filas por tabla)

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_5/pipeline.py

El pipeline puede ejecutarse múltiples veces — es idempotente por diseño:
  - Batch usa append con merge_schema
  - Silver usa upsert (no inserta duplicados)
  - Streaming usa checkpoints (no reprocesa archivos ya ingestados)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Asegurar que src/ esté en el path cuando se ejecuta desde la raíz
ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

DEMO_DIR    = Path(__file__).resolve().parent
CONFIG_PATH = DEMO_DIR / "config" / "config.json"
LANDING     = "/tmp/dkops_demo5/landing"
OPS_PATH    = "/tmp/dkops_demo5/ops/control"


def main() -> None:
    print("\n" + "=" * 65)
    print("  DKOps Demo 5 — Motor de Ingesta: Landing → Bronze → Silver")
    print("=" * 65)

    # ── 0. Generar datos sintéticos ───────────────────────────────────
    print("\n[0/5] Generando datos en Landing...")
    from demos.demo_5.datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2)

    # ── 1. Inicializar Spark ──────────────────────────────────────────
    print("\n[1/5] Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ── 2. Crear IngestionEngine ──────────────────────────────────────
    print("\n[2/5] Construyendo IngestionEngine...")
    from DKOps.ingestion import IngestionEngine

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

    # ── 3. Ingesta batch Landing → Bronze ─────────────────────────────
    print("\n[3/5] Ingesta batch (Landing → Bronze)...")
    failed_batch = engine.ingest_bronze()
    if failed_batch:
        print(f"  ⚠ Fallidos: {failed_batch}")
    else:
        print("  ✔ Ingesta batch completada")

    # ── 4. Ingesta streaming Landing → Bronze ─────────────────────────
    print("\n[4/5] Ingesta streaming con availableNow (Landing → Bronze)...")
    engine.run_streaming()
    print("  ✔ Streaming completado")

    # ── 5. Promoción Bronze → Silver ──────────────────────────────────
    print("\n[5/5] Promoción Silver (Bronze → Silver)...")

    # Bronze → Silver requiere que las tablas Bronze existan
    # Las tablas Silver se crean vía CreateWriter en el primer upsert
    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  ⚠ Fallidos Silver: {failed_silver}")
    else:
        print("  ✔ Promoción Silver completada")

    # ── Validación final ──────────────────────────────────────────────
    print("\n── Validación final ─────────────────────────────────────────")
    engine.status()

    # Mostrar tabla de control operativo
    print("\n── Tabla de control operativo ───────────────────────────────")
    try:
        ops_df = engine.ops.read()
        ops_df.select(
            "run_id", "dataset", "status", "rows_written", "started_at"
        ).show(truncate=False)
    except Exception as exc:
        print(f"  (ops table: {exc})")

    print("\n" + "=" * 65)
    print("  Demo 5 completado ✔")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
