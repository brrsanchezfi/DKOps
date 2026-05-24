"""
generate_aeropuertos.py — Genera snapshot del catálogo de aeropuertos en CSV.

Simula un FULL load del catálogo de aeropuertos, como si fuera enviado
por el sistema MRO (Maintenance, Repair & Operations) a la zona Landing.

Salida: un archivo CSV en {landing_path}/aeropuertos/
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timezone
from pathlib import Path


AEROPUERTOS_DATA = [
    # (iata, nombre, ciudad, pais, lat, lon, tipo)
    ("BOG", "El Dorado Internacional",        "Bogota",          "Colombia",  4.7016,  -74.1469, "Internacional"),
    ("MDE", "Jose Maria Cordova",              "Medellin",        "Colombia",  6.1645,  -75.4231, "Internacional"),
    ("CLO", "Alfonso Bonilla Aragon",          "Cali",            "Colombia",  3.5432,  -76.3816, "Internacional"),
    ("CTG", "Rafael Nunez",                    "Cartagena",       "Colombia", 10.4424,  -75.5130, "Internacional"),
    ("BAQ", "Ernesto Cortissoz",               "Barranquilla",    "Colombia", 10.8896,  -74.7808, "Internacional"),
    ("SMR", "Simon Bolivar",                   "Santa Marta",     "Colombia", 11.1196,  -74.2306, "Domestico"),
    ("PEI", "Matecana Internacional",          "Pereira",         "Colombia",  4.8127,  -75.7395, "Domestico"),
    ("BGA", "Palonegro Internacional",         "Bucaramanga",     "Colombia",  7.1265,  -73.1848, "Domestico"),
    ("LIM", "Jorge Chavez Internacional",      "Lima",            "Peru",     -12.0219, -77.1143, "Internacional"),
    ("GRU", "Guarulhos Internacional",         "Sao Paulo",       "Brasil",   -23.4356, -46.4731, "Internacional"),
    ("SCL", "Arturo Merino Benitez",           "Santiago",        "Chile",    -33.3930, -70.7858, "Internacional"),
    ("EZE", "Ministro Pistarini",              "Buenos Aires",    "Argentina",-34.8222, -58.5358, "Internacional"),
    ("MIA", "Miami Internacional",             "Miami",           "EEUU",      25.7959, -80.2870, "Internacional"),
    ("MAD", "Adolfo Suarez Barajas",           "Madrid",          "Espana",   40.4936,  -3.5668, "Internacional"),
    ("PTY", "Tocumen Internacional",           "Ciudad de Panama","Panama",    9.0714, -79.3835, "Internacional"),
]


def generate(
    landing_path: str,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un snapshot CSV del catálogo de aeropuertos."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "aeropuertos"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / f"aeropuertos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "aeropuerto_id", "nombre", "ciudad", "pais",
            "latitud", "longitud", "tipo",
            "capacidad_anual_pasajeros", "activo"
        ])
        for iata, nombre, ciudad, pais, lat, lon, tipo in AEROPUERTOS_DATA:
            capacidad = random.randint(2_000_000, 45_000_000)
            writer.writerow([
                iata, nombre, ciudad, pais,
                lat, lon, tipo,
                capacidad, "true"
            ])

    print(f"  aeropuertos: {len(AEROPUERTOS_DATA)} registros -> {out_file}")
    return out_file
