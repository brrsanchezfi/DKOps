"""
generate_movimientos.py — Genera movimientos de inventario (JSON, INCREMENTAL).

Simula entradas, salidas y ajustes de stock en un almacen.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


TIPOS_MOV = ["ENTRADA", "SALIDA", "AJUSTE", "DEVOLUCION"]
ALMACENES = ["ALM-BOG-001", "ALM-MDE-001", "ALM-CLO-001", "ALM-CTG-001"]
N_PRODS   = 18  # mismo numero que en productos


def generate(
    landing_path: str,
    n_records: int = 100,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo JSON con movimientos de inventario."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "movimientos_inventario"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _ in range(n_records):
        tipo_mov = random.choice(TIPOS_MOV)
        cantidad = random.randint(1, 50)
        if tipo_mov == "SALIDA":
            cantidad = -cantidad
        elif tipo_mov == "AJUSTE":
            cantidad = random.randint(-20, 20)

        ts = fecha - timedelta(minutes=random.randint(0, 1440))
        records.append({
            "movimiento_id": str(uuid.uuid4()),
            "producto_id":   f"PROD-{random.randint(1, N_PRODS):04d}",
            "almacen_id":    random.choice(ALMACENES),
            "tipo_movimiento": tipo_mov,
            "cantidad":      cantidad,
            "costo_unitario": round(random.uniform(5.0, 800.0), 2),
            "referencia":    f"REF-{random.randint(10000, 99999)}",
            "timestamp":     ts.isoformat(),
        })

    out_file = output_dir / f"movimientos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  movimientos: {len(records)} registros -> {out_file}")
    return out_file
