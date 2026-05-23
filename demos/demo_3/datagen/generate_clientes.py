"""
generate_clientes.py — Genera snapshot del catalogo de clientes (JSON, FULL load).
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path


PAISES   = ["CO", "MX", "PE", "AR", "CL", "EC", "VE", "BR"]
SEGMENTOS = ["VIP", "PREMIUM", "REGULAR", "NEW"]
DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com"]


def generate(
    landing_path: str,
    n_clientes: int = 150,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un snapshot JSON del catalogo de clientes."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "clientes"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for i in range(1, n_clientes + 1):
        pais = random.choice(PAISES)
        records.append({
            "cliente_id":       f"C{i:06d}",
            "nombre":           f"Cliente Ecommerce {i}",
            "email":            f"cliente{i}@{random.choice(DOMINIOS)}",
            "telefono":         f"+57 300 {i:07d}",
            "pais":             pais,
            "segmento":         random.choice(SEGMENTOS),
            "fecha_registro":   fecha.strftime("%Y-%m-%d"),
            "activo":           random.choices([True, False], weights=[0.90, 0.10])[0],
        })

    out_file = output_dir / f"clientes_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  clientes: {len(records)} registros -> {out_file}")
    return out_file
