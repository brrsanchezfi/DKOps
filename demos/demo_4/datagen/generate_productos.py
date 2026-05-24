"""
generate_productos.py — Genera catalogo de productos retail (CSV, FULL load).
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timezone
from pathlib import Path


CATEGORIAS = ["ELECTRONICA", "ROPA", "HOGAR", "ALIMENTOS", "DEPORTES", "JUGUETES"]

PRODUCTOS_BASE = [
    ("Smartphone Pro 12",       "ELECTRONICA",  599.99, 50),
    ("Laptop UltraBook",        "ELECTRONICA", 1299.99, 20),
    ("Tablet Mini 8",           "ELECTRONICA",  349.99, 35),
    ("Auriculares BT",          "ELECTRONICA",   79.99, 100),
    ("Camiseta Sport M",        "ROPA",           29.99, 200),
    ("Pantalon Cargo L",        "ROPA",           59.99, 150),
    ("Zapatos Running 42",      "ROPA",          109.99, 80),
    ("Licuadora 2L",            "HOGAR",          89.99, 60),
    ("Aspiradora Robot",        "HOGAR",         299.99, 25),
    ("Set Ollas 5pz",           "HOGAR",          149.99, 40),
    ("Arroz Premium 5kg",       "ALIMENTOS",      12.99, 500),
    ("Aceite Oliva 1L",         "ALIMENTOS",      18.99, 300),
    ("Multivitaminico 60cap",   "ALIMENTOS",      22.99, 200),
    ("Bicicleta MTB 26",        "DEPORTES",      499.99, 15),
    ("Mancuernas 10kg",         "DEPORTES",       45.99, 75),
    ("Pelota Futbol N5",        "DEPORTES",       39.99, 120),
    ("LEGO City 500pz",         "JUGUETES",       89.99, 55),
    ("Muneca Clasica",          "JUGUETES",       29.99, 90),
]


def generate(
    landing_path: str,
    fecha: datetime | None = None,
    batch_id: int = 1,
) -> Path:
    """Genera un snapshot CSV del catalogo de productos."""
    if fecha is None:
        fecha = datetime.now(timezone.utc)

    output_dir = Path(landing_path) / "productos"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / f"productos_{fecha.strftime('%Y%m%d')}_{batch_id:03d}.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "producto_id", "nombre", "categoria",
            "precio_usd", "stock_inicial", "activo", "proveedor_id"
        ])
        for i, (nombre, cat, precio, stock) in enumerate(PRODUCTOS_BASE, start=1):
            writer.writerow([
                f"PROD-{i:04d}", nombre, cat,
                precio,
                stock + random.randint(-10, 50),
                "true",
                f"PROV-{random.randint(1, 10):03d}"
            ])

    print(f"  productos: {len(PRODUCTOS_BASE)} registros -> {out_file}")
    return out_file
