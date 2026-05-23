"""
datagen/main.py — Generador de datos sintéticos para Demo 2 — Manufactura.

Genera lotes, ordenes y ventas en la zona Landing.

Uso
---
    python demos/demo_2/datagen/main.py
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from datagen import generate_lotes, generate_ordenes, generate_ventas

DEFAULT_LANDING = "/tmp/dkops_demo2/landing"


def run(
    landing_path: str = DEFAULT_LANDING,
    n_batches: int = 2,
    n_lotes: int = 150,
    n_ordenes: int = 100,
    n_ventas: int = 200,
) -> None:
    """Genera datos sinteticos en la zona Landing."""
    fecha_base = datetime.now(timezone.utc)
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DKOps Demo 2 — Data Generator (Manufactura)")
    print(f"  Landing: {landing_path}")
    print(f"  Batches: {n_batches}")
    print(f"{'='*60}\n")

    print("-- Generando datos BATCH --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_lotes.generate(landing_path, n_records=n_lotes,   batch_id=i, fecha=fecha_base)
        generate_ordenes.generate(landing_path, n_records=n_ordenes, batch_id=i, fecha=fecha_base)
        generate_ventas.generate(landing_path, n_records=n_ventas,  batch_id=i, fecha=fecha_base)

    print(f"\n{'='*60}")
    print("  Datos generados correctamente.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador Demo 2 — Manufactura")
    parser.add_argument("--landing",  default=DEFAULT_LANDING)
    parser.add_argument("--batches",  type=int, default=2)
    parser.add_argument("--lotes",    type=int, default=150)
    parser.add_argument("--ordenes",  type=int, default=100)
    parser.add_argument("--ventas",   type=int, default=200)
    args = parser.parse_args()
    run(
        landing_path=args.landing,
        n_batches=args.batches,
        n_lotes=args.lotes,
        n_ordenes=args.ordenes,
        n_ventas=args.ventas,
    )
