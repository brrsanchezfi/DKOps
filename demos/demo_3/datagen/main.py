"""
datagen/main.py — Generador de datos sinteticos para Demo 3 — E-commerce.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from datagen import generate_clientes, generate_pedidos, generate_eventos

DEFAULT_LANDING = "/tmp/dkops_demo3/landing"


def run(
    landing_path: str = DEFAULT_LANDING,
    n_batches: int = 2,
    n_clientes: int = 150,
    n_pedidos: int = 200,
    n_eventos: int = 100,
) -> None:
    """Genera datos sinteticos en la zona Landing."""
    fecha_base = datetime.now(timezone.utc)
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DKOps Demo 3 — Data Generator (E-commerce)")
    print(f"  Landing: {landing_path}")
    print(f"  Batches: {n_batches}")
    print(f"{'='*60}\n")

    print("-- Generando datos BATCH --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        # Clientes: FULL load (snapshot)
        generate_clientes.generate(
            landing_path, n_clientes=n_clientes, batch_id=i, fecha=fecha_base
        )
        # Pedidos v1 (primer batch) y v2 con schema evolution (batches siguientes)
        schema_ver = 1 if i == 1 else 2
        generate_pedidos.generate(
            landing_path, n_records=n_pedidos, batch_id=i,
            fecha=fecha_base, schema_version=schema_ver, n_clientes=n_clientes
        )

    print("\n-- Generando datos STREAMING (eventos web) --")
    for i in range(1, n_batches + 1):
        print(f"\n  Batch {i}/{n_batches}:")
        generate_eventos.generate(
            landing_path, n_eventos=n_eventos, batch_id=i,
            fecha=fecha_base, n_clientes=n_clientes
        )

    print(f"\n{'='*60}")
    print("  Datos generados correctamente.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador Demo 3 — E-commerce")
    parser.add_argument("--landing",   default=DEFAULT_LANDING)
    parser.add_argument("--batches",   type=int, default=2)
    parser.add_argument("--clientes",  type=int, default=150)
    parser.add_argument("--pedidos",   type=int, default=200)
    parser.add_argument("--eventos",   type=int, default=100)
    args = parser.parse_args()
    run(
        landing_path=args.landing,
        n_batches=args.batches,
        n_clientes=args.clientes,
        n_pedidos=args.pedidos,
        n_eventos=args.eventos,
    )
