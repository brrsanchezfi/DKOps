"""
datagen/main.py — Generador de datos sintéticos para Demo 5.

Genera todos los archivos necesarios en la zona Landing local
antes de ejecutar el pipeline de ingesta.

Uso
---
    python demos/demo_5/datagen/main.py
    python demos/demo_5/datagen/main.py --landing /ruta/custom --batches 3
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from demos.demo_5.datagen import generate_ventas, generate_clientes, generate_eventos

DEFAULT_LANDING = "/tmp/dkops_demo5/landing"


def run(
    landing_path: str  = DEFAULT_LANDING,
    n_batches:    int  = 2,
    n_ventas:     int  = 200,
    n_clientes:   int  = 150,
    n_eventos:    int  = 100,
) -> None:
    """
    Genera datos sintéticos en la zona Landing.

    Parámetros
    ----------
    landing_path : ruta base del Landing (local)
    n_batches    : número de archivos por fuente (simula días consecutivos)
    n_ventas     : eventos CDC de ventas por batch
    n_clientes   : clientes por snapshot
    n_eventos    : eventos de app por batch (streaming)
    """
    fecha_base = datetime.now(timezone.utc)
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DKOps Demo 5 — Data Generator")
    print(f"  Landing: {landing_path}")
    print(f"  Batches: {n_batches}")
    print(f"{'='*60}\n")

    print("── Generando datos BATCH ──────────────────────────────────")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_ventas.generate(landing_path, n_events=n_ventas,   batch_id=i, fecha=fecha_base)
        generate_clientes.generate(landing_path, n_clientes=n_clientes, batch_id=i, fecha=fecha_base)

    print("\n── Generando datos STREAMING ──────────────────────────────")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_eventos.generate(landing_path, n_eventos=n_eventos, batch_id=i, fecha=fecha_base)

    print(f"\n{'='*60}")
    print("  Datos generados correctamente ✔")
    print(f"  Ejecuta ahora: python demos/demo_5/pipeline.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de datos Demo 5 — DKOps Ingestion")
    parser.add_argument("--landing", default=DEFAULT_LANDING, help="Ruta base Landing")
    parser.add_argument("--batches", type=int, default=2,   help="Número de batches por fuente")
    parser.add_argument("--ventas",  type=int, default=200, help="Eventos CDC de ventas por batch")
    parser.add_argument("--clientes",type=int, default=150, help="Clientes por snapshot")
    parser.add_argument("--eventos", type=int, default=100, help="Eventos streaming por batch")
    args = parser.parse_args()

    run(
        landing_path = args.landing,
        n_batches    = args.batches,
        n_ventas     = args.ventas,
        n_clientes   = args.clientes,
        n_eventos    = args.eventos,
    )
