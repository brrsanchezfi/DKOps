"""
generate_alertas.py — Genera alertas de stock bajo (JSON, streaming).

Simula alertas generadas automaticamente cuando el stock cae por debajo
del umbral minimo. Se consumen via streaming.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


SEVERIDADES = ["CRITICA", "ALTA", "MEDIA"]
ALMACENES   = ["ALM-BOG-001", "ALM-MDE-001", "ALM-CLO-001", "ALM-CTG-001"]
N_PRODS     = 18


def generate(
    landing_path: str,
    n_alertas: int = 30,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo JSON con alertas de stock bajo."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "alertas_stock"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _ in range(n_alertas):
        stock_actual  = random.randint(0, 15)
        stock_minimo  = random.randint(10, 30)
        severidad = (
            "CRITICA" if stock_actual == 0
            else "ALTA" if stock_actual <= 5
            else "MEDIA"
        )
        ts = fecha - timedelta(minutes=random.randint(0, 60))
        records.append({
            "alerta_id":    str(uuid.uuid4()),
            "producto_id":  f"PROD-{random.randint(1, N_PRODS):04d}",
            "almacen_id":   random.choice(ALMACENES),
            "stock_actual": stock_actual,
            "stock_minimo": stock_minimo,
            "deficit":      max(0, stock_minimo - stock_actual),
            "severidad":    severidad,
            "timestamp":    ts.isoformat(),
        })

    out_file = output_dir / f"alertas_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  alertas_stock: {len(records)} alertas -> {out_file}")
    return out_file
