"""
generate_eventos.py — Genera eventos de aplicación para pruebas de streaming.

Simula el clickstream de una aplicación e-commerce:
  - page_view, add_to_cart, purchase, search
  - Distribución temporal realista dentro del día

Para streaming local: genera archivos JSON uno a uno en el directorio
monitoreado por FileStreamReader (Landing/eventos_app/).
El pipeline.py llama a generate() antes de run_streaming() para que
haya archivos que procesar.

En producción (Databricks + Kafka): este generador se reemplaza por
el producer Kafka real. La lógica del BronzeIngestor no cambia.

Salida: archivos JSON por línea en {landing_path}/eventos_app/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


TIPOS_EVENTO = ["page_view", "add_to_cart", "purchase", "search", "wishlist"]
CANALES      = ["web", "app", "mobile"]
PESOS_TIPO   = [0.45, 0.25, 0.10, 0.15, 0.05]
PRODUCTOS    = [f"PROD-{i:04d}" for i in range(1, 51)]
CLIENTES     = [f"CLI-{i:06d}" for i in range(1, 201)] + [None] * 30  # 30 anónimos


def _evento(fecha: datetime) -> dict:
    offset = timedelta(seconds=random.randint(0, 3600))
    return {
        "evento_id":   str(uuid.uuid4()),
        "cliente_id":  random.choice(CLIENTES),
        "tipo_evento": random.choices(TIPOS_EVENTO, weights=PESOS_TIPO)[0],
        "producto_id": random.choice(PRODUCTOS) if random.random() > 0.2 else None,
        "canal":       random.choice(CANALES),
        "evento_ts":   (fecha - offset).isoformat(),
    }


def generate(
    landing_path: str,
    n_eventos:    int  = 150,
    fecha:        datetime | None = None,
    batch_id:     int = 1,
) -> Path:
    """
    Genera un archivo de eventos de app para ingestión streaming.

    Parámetros
    ----------
    landing_path : ruta base de la zona Landing
    n_eventos    : número de eventos a generar
    fecha        : fecha de referencia del batch
    batch_id     : sufijo numérico del archivo

    Devuelve
    --------
    Path del archivo generado.
    """
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "eventos_app"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = [_evento(fecha) for _ in range(n_eventos)]

    out_file = output_dir / f"eventos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  ✔ eventos streaming: {len(records)} eventos → {out_file}")
    return out_file
