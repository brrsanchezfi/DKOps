"""
generate_eventos.py — Genera clickstream web (JSON, streaming).

Simula eventos de navegacion web generados continuamente por el sitio
de e-commerce. Se usan para enriquecer metricas de engagement.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


TIPOS_EVENTO = ["page_view", "click", "add_to_cart", "purchase", "search", "logout"]
PAGINAS      = ["/home", "/productos", "/carrito", "/checkout", "/cuenta", "/buscar"]
DISPOSITIVOS = ["mobile", "desktop", "tablet"]


def generate(
    landing_path: str,
    n_eventos: int = 100,
    fecha: datetime | None = None,
    batch_id: int = 1,
    n_clientes: int = 150,
) -> Path:
    """Genera un archivo JSON con eventos de clickstream web."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "eventos_web"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _ in range(n_eventos):
        ts = fecha - timedelta(seconds=random.randint(0, 3600))
        records.append({
            "evento_id":    f"EVT-{random.randint(10**9, 10**10-1)}",
            "cliente_id":   f"C{random.randint(1, n_clientes):06d}" if random.random() > 0.2 else None,
            "session_id":   f"SES-{random.randint(10**6, 10**7-1)}",
            "tipo_evento":  random.choice(TIPOS_EVENTO),
            "pagina":       random.choice(PAGINAS),
            "dispositivo":  random.choice(DISPOSITIVOS),
            "duracion_seg": random.randint(1, 300),
            "timestamp":    ts.isoformat(),
        })

    out_file = output_dir / f"eventos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  eventos_web: {len(records)} eventos -> {out_file}")
    return out_file
