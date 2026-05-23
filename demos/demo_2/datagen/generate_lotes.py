"""
generate_lotes.py — Genera datos de lotes de produccion en formato JSON.

Simula la carga diaria incremental de lotes desde el sistema MES
(Manufacturing Execution System) hacia la zona Landing.

Salida: un archivo JSON por linea en {landing_path}/lotes_produccion/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


PRODUCTOS = [
    "JABON-LIQ-1L",   "JABON-BAR-100G", "JABON-BAR-200G",
    "DET-POLVO-2KG",  "DET-LIQ-3L",     "DET-LIQ-5L",
    "SHAMP-500ML",    "SHAMP-1L",       "ACOND-500ML",
    "SUAV-1L",        "SUAV-3L",        "LIMP-MULTI-1L",
]

RESULTADOS_QC = ["APPROVED", "REJECTED", "RETEST"]


def _lote(fecha: datetime, idx: int) -> dict:
    producida = random.randint(100, 2000)
    tasa_merma = random.choice([random.uniform(0, 0.08)] * 9 + [random.uniform(0.1, 0.3)])
    defectuosa = int(producida * tasa_merma)

    if defectuosa / max(producida, 1) > 0.10:
        qc = random.choice(["REJECTED", "REJECTED", "RETEST"])
    else:
        qc = random.choice(["APPROVED", "APPROVED", "APPROVED", "RETEST"])

    return {
        "lote_id":             f"LOTE-{uuid.uuid4().hex[:8].upper()}",
        "orden_id":            f"ORD-{random.randint(0, 999):05d}",
        "producto_id":         random.choice(PRODUCTOS),
        "fecha_produccion":    fecha.strftime("%Y-%m-%d"),
        "cantidad_producida":  producida,
        "cantidad_defectuosa": defectuosa,
        "resultado_qc":        qc,
        "ph_medido":           round(random.gauss(6.5, 0.8), 2),
        "viscosidad_cp":       round(random.uniform(800, 2500), 1),
    }


def generate(
    landing_path: str,
    n_records: int = 150,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo JSON con lotes de produccion."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "lotes_produccion"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = [_lote(fecha, i) for i in range(n_records)]

    out_file = output_dir / f"lotes_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  lotes: {len(records)} registros -> {out_file}")
    return out_file
