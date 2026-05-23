"""
datagen/main.py — Generador de datos sinteticos para Demo 4 — Retail/Inventario.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from datagen import generate_productos, generate_movimientos, generate_alertas

DEFAULT_LANDING = "/tmp/dkops_demo4/landing"


def run(
    landing_path: str = DEFAULT_LANDING,
    n_batches: int = 2,
    n_movimientos: int = 100,
    n_alertas: int = 30,
) -> None:
    """Genera datos sinteticos en la zona Landing."""
    fecha_base = datetime.now(timezone.utc)
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DKOps Demo 4 — Data Generator (Retail/Inventario)")
    print(f"  Landing: {landing_path}")
    print(f"  Batches: {n_batches}")
    print(f"{'='*60}\n")

    print("-- Generando datos BATCH --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_productos.generate(landing_path, batch_id=i, fecha=fecha_base)
        generate_movimientos.generate(landing_path, n_records=n_movimientos, batch_id=i, fecha=fecha_base)

    print("\n-- Generando datos STREAMING (alertas stock) --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_alertas.generate(landing_path, n_alertas=n_alertas, batch_id=i, fecha=fecha_base)

    print(f"\n{'='*60}")
    print("  Datos generados correctamente.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador Demo 4 — Retail/Inventario")
    parser.add_argument("--landing",       default=DEFAULT_LANDING)
    parser.add_argument("--batches",       type=int, default=2)
    parser.add_argument("--movimientos",   type=int, default=100)
    parser.add_argument("--alertas",       type=int, default=30)
    args = parser.parse_args()
    run(
        landing_path=args.landing,
        n_batches=args.batches,
        n_movimientos=args.movimientos,
        n_alertas=args.alertas,
    )
