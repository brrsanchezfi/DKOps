"""
generate_ventas.py — Genera datos de ventas en formato CSV.

Simula carga incremental diaria de ventas de distribuidores.

Salida: un archivo CSV en {landing_path}/ventas_manufactura/
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


PRODUCTOS = [
    "JABON-LIQ-1L",   "JABON-BAR-100G", "JABON-BAR-200G",
    "DET-POLVO-2KG",  "DET-LIQ-3L",     "DET-LIQ-5L",
    "SHAMP-500ML",    "SHAMP-1L",       "ACOND-500ML",
    "SUAV-1L",        "SUAV-3L",        "LIMP-MULTI-1L",
]

DISTRIBUIDORES = [
    "DIST-001", "DIST-002", "DIST-003", "DIST-004",
    "DIST-005", "DIST-006", "DIST-007", "DIST-008",
]

ESTADOS = ["CONFIRMED", "CANCELLED", "RETURNED", "PENDING"]


def generate(
    landing_path: str,
    n_records: int = 200,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo CSV con ventas de manufactura."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "ventas_manufactura"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / f"ventas_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.csv"

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "venta_id", "fecha", "distribuidor_id",
            "producto_id", "cantidad", "precio_unitario", "estado_venta"
        ])

        for i in range(n_records):
            estado = random.choices(
                ESTADOS, weights=[0.80, 0.05, 0.08, 0.07]
            )[0]
            cantidad = random.randint(10, 500)
            if estado == "RETURNED":
                cantidad = -cantidad
            writer.writerow([
                f"VTA-{fecha.strftime('%Y%m%d')}-{i:05d}",
                fecha.strftime("%Y-%m-%d"),
                random.choice(DISTRIBUIDORES),
                random.choice(PRODUCTOS),
                cantidad,
                round(random.uniform(5_000, 45_000), 2),
                estado,
            ])

    print(f"  ventas: {n_records} registros -> {out_file}")
    return out_file
