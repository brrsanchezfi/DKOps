"""
generate_clientes.py — Genera snapshot del catálogo de clientes en JSON.

Simula el estado del sistema CRM:
  - Clientes nuevos y existentes con diferentes segmentos
  - Distribución geográfica realista
  - Corridas múltiples simulan cambios de segmento (para full_merge)

Salida: un archivo JSON por línea en {landing_path}/clientes/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


NOMBRES = [
    "Carlos García", "Ana Martínez", "Luis Rodríguez", "María López",
    "Juan Hernández", "Laura González", "Pedro Sánchez", "Isabel Pérez",
    "Diego Ramírez", "Sofía Torres", "Ricardo Flores", "Valentina Cruz",
    "Andrés Morales", "Camila Vargas", "Felipe Castro", "Daniela Romero",
]

CIUDADES_PAIS = [
    ("Bogotá", "CO"), ("Medellín", "CO"), ("Cali", "CO"), ("Barranquilla", "CO"),
    ("Buenos Aires", "AR"), ("Córdoba", "AR"), ("Rosario", "AR"),
    ("Ciudad de México", "MX"), ("Guadalajara", "MX"), ("Monterrey", "MX"),
    ("Lima", "PE"), ("Arequipa", "PE"),
    ("Santiago", "CL"), ("Valparaíso", "CL"),
]

SEGMENTOS = ["PREMIUM", "STANDARD", "BASICO"]


def _cliente(client_num: int, fecha_base: datetime) -> dict:
    nombre   = random.choice(NOMBRES)
    ciudad, pais = random.choice(CIUDADES_PAIS)
    reg_offset = timedelta(days=random.randint(30, 730))
    return {
        "cliente_id":      f"CLI-{client_num:06d}",
        "nombre":          f"{nombre} {client_num}",
        "email":           f"cliente_{client_num}@mail.com",
        "ciudad":          ciudad,
        "pais":            pais,
        "segmento":        random.choice(SEGMENTOS),
        "activo":          random.random() > 0.05,   # 95% activos
        "fecha_registro":  (fecha_base - reg_offset).isoformat(),
    }


def generate(
    landing_path: str,
    n_clientes:   int  = 200,
    fecha:        datetime | None = None,
    batch_id:     int = 1,
) -> Path:
    """
    Genera un snapshot del catálogo de clientes.

    Parámetros
    ----------
    landing_path : ruta base de la zona Landing
    n_clientes   : número de clientes en el snapshot
    fecha        : fecha de referencia
    batch_id     : sufijo numérico del archivo

    Devuelve
    --------
    Path del archivo generado.
    """
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "clientes"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = [_cliente(i, fecha) for i in range(1, n_clientes + 1)]

    out_file = output_dir / f"clientes_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  ✔ clientes: {len(records)} registros → {out_file}")
    return out_file
