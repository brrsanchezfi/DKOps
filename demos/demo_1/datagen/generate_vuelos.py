"""
generate_vuelos.py — Genera eventos de vuelos diarios en formato JSON.

Simula la descarga diaria de datos operativos de vuelos desde el sistema
fuente (como si fuera enviado por Data Factory a la zona Landing).

Salida: un archivo JSON por línea en {landing_path}/vuelos_diarios/
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


AEROPUERTOS = [
    "BOG", "MDE", "CLO", "CTG", "BAQ",
    "SMR", "PEI", "BGA", "LIM", "GRU",
    "SCL", "EZE", "MIA", "MAD", "PTY",
]

AEROLINEAS = ["AV", "LA", "AA", "IB", "CM", "VX", "P9", "JA"]

ESTADOS = ["ON_TIME", "DELAYED", "CANCELLED", "DIVERTED"]

CAUSAS = ["WEATHER", "AIRLINE", "AIRPORT", "SECURITY", None, None, None]


def _vuelo(fecha: datetime) -> dict:
    origen = random.choice(AEROPUERTOS)
    destino = random.choice([a for a in AEROPUERTOS if a != origen])
    retraso_salida = max(0, int(random.gauss(10, 25)))
    retraso_llegada = max(0, int(random.gauss(12, 28)))
    estado = random.choices(
        ESTADOS, weights=[0.70, 0.20, 0.07, 0.03]
    )[0]
    capacidad = random.choice([120, 150, 180, 220, 280])
    pasajeros = int(capacidad * random.uniform(0.55, 0.98))
    aerolinea = random.choice(AEROLINEAS)

    hora_salida_prog = f"{random.randint(5,22):02d}:{random.choice(['00','15','30','45'])}"
    return {
        "vuelo_id":           str(uuid.uuid4()),
        "iata_aerolinea":     aerolinea,
        "iata_origen":        origen,
        "iata_destino":       destino,
        "fecha":              fecha.strftime("%Y-%m-%d"),
        "hora_salida_prog":   hora_salida_prog,
        "retraso_salida_min": retraso_salida,
        "retraso_llegada_min": retraso_llegada,
        "estado":             estado,
        "causa_retraso":      random.choice(CAUSAS) if estado == "DELAYED" else None,
        "pasajeros":          pasajeros,
        "capacidad":          capacidad,
        "distancia_km":       random.randint(150, 9800),
        "updated_at":         fecha.isoformat(),
    }


def generate(
    landing_path: str,
    n_events: int = 200,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un archivo JSON con eventos de vuelos diarios."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "vuelos_diarios"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = [_vuelo(fecha) for _ in range(n_events)]

    out_file = output_dir / f"vuelos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  vuelos: {len(records)} eventos -> {out_file}")
    return out_file
