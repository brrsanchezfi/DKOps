"""
datagen/main.py — Generador de datos sintéticos para Demo 1 — Aeronautica.

Genera todos los archivos necesarios en la zona Landing local antes de
ejecutar el pipeline de ingesta.

Uso
---
    python demos/demo_1/datagen/main.py
    python demos/demo_1/datagen/main.py --landing /ruta/custom --batches 2
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from datagen import generate_vuelos, generate_aeropuertos

DEFAULT_LANDING = "/tmp/dkops_demo1/landing"


def run(
    landing_path: str = DEFAULT_LANDING,
    n_batches: int = 2,
    n_vuelos: int = 200,
) -> None:
    """
    Genera datos sintéticos en la zona Landing.

    Parametros
    ----------
    landing_path : ruta base del Landing (local)
    n_batches    : numero de archivos por fuente de vuelos (simula dias consecutivos)
    n_vuelos     : vuelos por batch
    """
    fecha_base = datetime.now(timezone.utc)
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DKOps Demo 1 — Data Generator (Aeronautica)")
    print(f"  Landing: {landing_path}")
    print(f"  Batches: {n_batches}")
    print(f"{'='*60}\n")

    print("-- Generando datos BATCH --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_vuelos.generate(landing_path, n_events=n_vuelos, batch_id=i, fecha=fecha_base)
        generate_aeropuertos.generate(landing_path, batch_id=i, fecha=fecha_base)

    print(f"\n{'='*60}")
    print("  Datos generados correctamente.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de datos Demo 1 — DKOps Aeronautica")
    parser.add_argument("--landing", default=DEFAULT_LANDING, help="Ruta base Landing")
    parser.add_argument("--batches", type=int, default=2, help="Numero de batches")
    parser.add_argument("--vuelos",  type=int, default=200, help="Vuelos por batch")
    args = parser.parse_args()

    run(
        landing_path=args.landing,
        n_batches=args.batches,
        n_vuelos=args.vuelos,
    )
