"""
generate_ordenes.py — Genera ordenes de produccion en formato JSON (CDC).

Simula eventos CDC desde el sistema ERP con op_type: I/U/D.

Salida: un archivo JSON por linea en {landing_path}/ordenes_produccion/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


LINEAS    = ["L1", "L2", "L3", "L4"]
PRODUCTOS = [
    "JABON-LIQ-1L",   "JABON-BAR-100G", "JABON-BAR-200G",
    "DET-POLVO-2KG",  "DET-LIQ-3L",     "DET-LIQ-5L",
    "SHAMP-500ML",    "SHAMP-1L",       "ACOND-500ML",
    "SUAV-1L",        "SUAV-3L",        "LIMP-MULTI-1L",
]
ESTADOS = ["COMPLETED", "IN_PROGRESS", "CANCELLED", "PENDING"]
OPERADORES = [
    "Carlos Mejia", "Ana Rodriguez", "Pedro Gomez",
    "Sofia Velez",  "Juan Perez",    "Maria Lopez",
]


def _orden(fecha: datetime, op_type: str) -> dict:
    inicio = fecha - timedelta(hours=random.randint(0, 8))
    estado = random.choices(ESTADOS, weights=[0.5, 0.3, 0.15, 0.05])[0]
    fin = (inicio + timedelta(minutes=random.randint(60, 480))).isoformat() \
          if estado == "COMPLETED" else None
    cantidad_planeada = random.randint(500, 5000)
    ratio = random.gauss(0.95, 0.15)
    return {
        "orden_id":          f"ORD-{uuid.uuid4().hex[:6].upper()}",
        "linea_id":          random.choice(LINEAS),
        "producto_id":       random.choice(PRODUCTOS),
        "fecha_inicio":      inicio.isoformat(),
        "fecha_fin":         fin,
        "cantidad_planeada": cantidad_planeada,
        "cantidad_real":     max(0, int(cantidad_planeada * ratio)),
        "estado":            estado,
        "operador":          random.choice(OPERADORES + [None, None]),
        "op_type":           op_type,
        "updated_at":        fecha.isoformat(),
    }


def generate(
    landing_path: str,
    n_records: int = 100,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo JSON con eventos CDC de ordenes de produccion."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "ordenes_produccion"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _ in range(n_records):
        op_type = random.choices(["I", "U", "D"], weights=[0.70, 0.22, 0.08])[0]
        records.append(_orden(fecha, op_type))

    out_file = output_dir / f"ordenes_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  ordenes: {len(records)} eventos -> {out_file}")
    return out_file
