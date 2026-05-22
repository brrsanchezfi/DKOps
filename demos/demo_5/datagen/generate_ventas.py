"""
generate_ventas.py — Genera eventos CDC de ventas en formato JSON.

Simula la actividad diaria de un sistema de pedidos:
  - 70% INSERT  (ventas nuevas)
  - 20% UPDATE  (cambios de estado)
  - 10% DELETE  (cancelaciones)

Salida: un archivo JSON por línea en {landing_path}/ventas_diarias/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


CANALES   = ["web", "app", "tienda"]
ESTADOS   = ["pendiente", "pagado", "enviado", "cancelado"]
PRODUCTOS = [f"PROD-{i:04d}" for i in range(1, 51)]
CLIENTES  = [f"CLI-{i:06d}" for i in range(1, 201)]


def _venta(op_type: str, fecha: datetime) -> dict:
    return {
        "venta_id":     str(uuid.uuid4()),
        "cliente_id":   random.choice(CLIENTES),
        "producto_id":  random.choice(PRODUCTOS),
        "cantidad":     random.randint(1, 10),
        "precio_total": round(random.uniform(10.0, 2000.0), 2),
        "canal":        random.choice(CANALES),
        "estado":       random.choice(ESTADOS) if op_type != "D" else "cancelado",
        "op_type":      op_type,
        "fecha_venta":  fecha.isoformat(),
    }


def generate(
    landing_path:  str,
    n_events:      int  = 200,
    fecha:         datetime | None = None,
    batch_id:      int = 1,
) -> Path:
    """
    Genera un archivo JSON con eventos CDC de ventas.

    Parámetros
    ----------
    landing_path : ruta base de la zona Landing (se crea si no existe)
    n_events     : número de eventos a generar
    fecha        : fecha de los eventos (default: hoy)
    batch_id     : sufijo numérico del archivo (para múltiples batches)

    Devuelve
    --------
    Path del archivo generado.
    """
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "ventas_diarias"
    output_dir.mkdir(parents=True, exist_ok=True)

    weights = [0.70, 0.20, 0.10]
    records = []

    for _ in range(n_events):
        op_type = random.choices(["I", "U", "D"], weights=weights)[0]
        offset  = timedelta(seconds=random.randint(0, 86400))
        records.append(_venta(op_type, fecha - offset))

    out_file = output_dir / f"ventas_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  ✔ ventas: {len(records)} eventos → {out_file}")
    return out_file
