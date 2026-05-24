"""
generate_pedidos.py — Genera eventos CDC de pedidos (JSON).

Los pedidos incluyen op_type: I/U/D y una segunda version con columnas
adicionales (metodo_envio, dias_entrega, calificacion) para demostrar
merge_schema.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


ESTADOS  = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
METODOS  = ["STANDARD", "EXPRESS", "PICKUP", "SAME_DAY"]
CANALES  = ["web", "app", "tienda"]


def _pedido_v1(fecha: datetime, op_type: str, cliente_id: str) -> dict:
    return {
        "pedido_id":     str(uuid.uuid4()),
        "cliente_id":    cliente_id,
        "email_cliente": f"{cliente_id.lower()}@ejemplo.com",
        "fecha_pedido":  fecha.strftime("%Y-%m-%d"),
        "total_usd":     round(random.uniform(10.0, 500.0), 2),
        "estado":        random.choice(ESTADOS) if op_type != "D" else "CANCELLED",
        "canal":         random.choice(CANALES),
        "op_type":       op_type,
        "updated_at":    fecha.isoformat(),
    }


def _pedido_v2(fecha: datetime, op_type: str, cliente_id: str) -> dict:
    """Pedido con schema v2: agrega metodo_envio, dias_entrega, calificacion."""
    base = _pedido_v1(fecha, op_type, cliente_id)
    base["metodo_envio"]  = random.choice(METODOS)
    base["dias_entrega"]  = random.randint(1, 15)
    base["calificacion"]  = round(random.uniform(1.0, 5.0), 1)
    return base


def generate(
    landing_path: str,
    n_records: int = 200,
    fecha: datetime | None = None,
    batch_id: int = 1,
    schema_version: int = 1,
    n_clientes: int = 150,
) -> Path:
    """Genera un archivo JSON con eventos CDC de pedidos."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "pedidos"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _ in range(n_records):
        op_type    = random.choices(["I", "U", "D"], weights=[0.65, 0.25, 0.10])[0]
        cliente_id = f"C{random.randint(1, n_clientes):06d}"
        if schema_version >= 2:
            records.append(_pedido_v2(fecha, op_type, cliente_id))
        else:
            records.append(_pedido_v1(fecha, op_type, cliente_id))

    out_file = output_dir / f"pedidos_v{schema_version}_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  pedidos v{schema_version}: {len(records)} eventos -> {out_file}")
    return out_file
