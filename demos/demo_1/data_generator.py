"""
data_generator.py
=================
Genera datos sintéticos realistas para el dominio aeronáutico.
Cada llamada produce datos ligeramente distintos (timestamps, retrasos,
pasajeros) — simula una nueva ejecución del pipeline.

Uso
---
    gen = DataGenerator(spark)

    df_aeropuertos = gen.aeropuertos()          # full — 15 aeropuertos fijos
    df_aerolineas  = gen.aerolineas()           # full — 8 aerolíneas fijas
    df_tiempo      = gen.tiempo("2024-01-01",   # full — rango de fechas
                                "2024-03-31")
    df_vuelos      = gen.vuelos(fecha="2024-01-15", n=50)   # incremental
    df_vuelos_mod  = gen.vuelos_modificados(fecha="2024-01-15", n=10)  # correcciones
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T


# ── Datos maestros ────────────────────────────────────────────────────────────

AEROPUERTOS = [
    # (iata, nombre, ciudad, pais, lat, lon)
    ("BOG", "El Dorado Internacional",       "Bogotá",        "Colombia",  4.7016,  -74.1469),
    ("MDE", "José María Córdova",             "Medellín",      "Colombia",  6.1645,  -75.4231),
    ("CLO", "Alfonso Bonilla Aragón",         "Cali",          "Colombia",  3.5432,  -76.3816),
    ("CTG", "Rafael Núñez",                   "Cartagena",     "Colombia", 10.4424,  -75.5130),
    ("BAQ", "Ernesto Cortissoz",              "Barranquilla",  "Colombia", 10.8896,  -74.7808),
    ("SMR", "Simón Bolívar",                  "Santa Marta",   "Colombia", 11.1196,  -74.2306),
    ("PEI", "Matecaña Internacional",         "Pereira",       "Colombia",  4.8127,  -75.7395),
    ("BGA", "Palonegro Internacional",        "Bucaramanga",   "Colombia",  7.1265,  -73.1848),
    ("LIM", "Jorge Chávez Internacional",     "Lima",          "Perú",     -12.0219, -77.1143),
    ("GRU", "Guarulhos Internacional",        "São Paulo",     "Brasil",   -23.4356, -46.4731),
    ("SCL", "Arturo Merino Benítez",          "Santiago",      "Chile",    -33.3930, -70.7858),
    ("EZE", "Ministro Pistarini",             "Buenos Aires",  "Argentina",-34.8222, -58.5358),
    ("MIA", "Miami Internacional",            "Miami",         "EEUU",      25.7959, -80.2870),
    ("MAD", "Adolfo Suárez Barajas",          "Madrid",        "España",   40.4936,  -3.5668),
    ("PTY", "Tocumen Internacional",          "Ciudad de Panamá","Panamá",  9.0714, -79.3835),
]

AEROLINEAS = [
    # (iata, nombre, pais, alianza, tipo)
    ("AV", "Avianca",              "Colombia",  "Star Alliance", "traditional"),
    ("LA", "LATAM Airlines",       "Chile",     "Oneworld",      "traditional"),
    ("AA", "American Airlines",    "EEUU",      "Oneworld",      "traditional"),
    ("IB", "Iberia",               "España",    "Oneworld",      "traditional"),
    ("CM", "Copa Airlines",        "Panamá",    "Star Alliance", "traditional"),
    ("VX", "Viva Air",             "Colombia",  "Ninguna",       "low_cost"),
    ("P9", "Wingo",                "Colombia",  "Ninguna",       "low_cost"),
    ("JA", "JetSmart",             "Chile",     "Ninguna",       "low_cost"),
]

CAUSAS_RETRASO = ["WEATHER", "AIRLINE", "AIRPORT", "SECURITY", None, None, None]

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

DIAS_ES = {
    1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves",
    5: "Viernes", 6: "Sábado", 7: "Domingo",
}

# Festivos Colombia 2024 (muestra)
FESTIVOS_CO_2024 = {
    date(2024, 1, 1), date(2024, 1, 8), date(2024, 3, 25),
    date(2024, 3, 28), date(2024, 3, 29), date(2024, 5, 1),
    date(2024, 5, 13), date(2024, 6, 3), date(2024, 6, 10),
    date(2024, 7, 4), date(2024, 7, 20), date(2024, 8, 7),
    date(2024, 8, 19), date(2024, 10, 14), date(2024, 11, 4),
    date(2024, 11, 11), date(2024, 12, 8), date(2024, 12, 25),
}


class DataGenerator:
    """
    Genera DataFrames sintéticos realistas para el dominio aeronáutico.

    Cada llamada a vuelos() y vuelos_modificados() produce datos ligeramente
    distintos — simula una nueva extracción del sistema fuente.
    """

    def __init__(self, spark: SparkSession, seed: int | None = None) -> None:
        self._spark = spark
        self._rng   = random.Random(seed)   # seed=None → aleatorio cada vez

    # ── Dimensión Aeropuertos ─────────────────────────────────────────────

    def aeropuertos(self) -> DataFrame:
        """15 aeropuertos fijos. Full load — siempre el mismo conjunto."""
        rows = [
            (iata, nombre, ciudad, pais, lat, lon, True)
            for iata, nombre, ciudad, pais, lat, lon in AEROPUERTOS
        ]
        schema = T.StructType([
            T.StructField("iata_code", T.StringType(),  False),
            T.StructField("nombre",    T.StringType(),  True),
            T.StructField("ciudad",    T.StringType(),  True),
            T.StructField("pais",      T.StringType(),  True),
            T.StructField("latitud",   T.DoubleType(),  True),
            T.StructField("longitud",  T.DoubleType(),  True),
            T.StructField("activo",    T.BooleanType(), True),
        ])
        return self._spark.createDataFrame(rows, schema)

    # ── Dimensión Aerolíneas ──────────────────────────────────────────────

    def aerolineas(self) -> DataFrame:
        """8 aerolíneas fijas. Full load."""
        rows = [
            (iata, nombre, pais, alianza, tipo, True)
            for iata, nombre, pais, alianza, tipo in AEROLINEAS
        ]
        schema = T.StructType([
            T.StructField("iata_code",   T.StringType(),  False),
            T.StructField("nombre",      T.StringType(),  True),
            T.StructField("pais_origen", T.StringType(),  True),
            T.StructField("alianza",     T.StringType(),  True),
            T.StructField("tipo",        T.StringType(),  True),
            T.StructField("activa",      T.BooleanType(), True),
        ])
        return self._spark.createDataFrame(rows, schema)

    # ── Dimensión Tiempo ──────────────────────────────────────────────────

    def tiempo(self, fecha_inicio: str, fecha_fin: str) -> DataFrame:
        """Genera una fila por cada fecha en el rango. Full load."""
        start = date.fromisoformat(fecha_inicio)
        end   = date.fromisoformat(fecha_fin)
        rows  = []

        current = start
        while current <= end:
            dow = current.isoweekday()  # 1=Lunes … 7=Domingo
            rows.append((
                current,
                current.year,
                current.month,
                MESES_ES[current.month],
                (current.month - 1) // 3 + 1,
                int(current.strftime("%W")),
                dow,
                DIAS_ES[dow],
                dow >= 6,
                current in FESTIVOS_CO_2024,
            ))
            current += timedelta(days=1)

        schema = T.StructType([
            T.StructField("fecha",            T.DateType(),    False),
            T.StructField("anio",             T.IntegerType(), True),
            T.StructField("mes",              T.IntegerType(), True),
            T.StructField("mes_nombre",       T.StringType(),  True),
            T.StructField("trimestre",        T.IntegerType(), True),
            T.StructField("semana_anio",      T.IntegerType(), True),
            T.StructField("dia_semana",       T.IntegerType(), True),
            T.StructField("dia_semana_nombre",T.StringType(),  True),
            T.StructField("es_fin_semana",    T.BooleanType(), True),
            T.StructField("es_festivo",       T.BooleanType(), True),
        ])
        return self._spark.createDataFrame(rows, schema)

    # ── Fact Vuelos ───────────────────────────────────────────────────────

    def vuelos(self, fecha: str, n: int = 80) -> DataFrame:
        """
        Genera n vuelos para una fecha dada.
        Cada llamada produce retrasos y pasajeros distintos — simula
        una nueva extracción del sistema fuente.
        """
        fecha_dt = date.fromisoformat(fecha)
        rows     = []
        pares    = self._pares_ruta(n)

        for i, (orig, dest) in enumerate(pares):
            aerolinea  = self._rng.choice(AEROLINEAS)[0]
            vuelo_id   = f"{aerolinea}-{fecha.replace('-', '')}-{i+1:04d}"
            hora_sal_p = self._hora_aleatoria()
            retraso_s  = self._retraso()
            hora_sal_r = self._sumar_minutos(hora_sal_p, retraso_s)
            duracion   = self._duracion(orig, dest)
            hora_lle_p = self._sumar_minutos(hora_sal_p, duracion)
            retraso_l  = retraso_s + self._rng.randint(-5, 15)
            hora_lle_r = self._sumar_minutos(hora_sal_r, duracion)
            capacidad  = self._rng.choice([150, 180, 200, 220, 280])
            pasajeros  = int(capacidad * self._rng.uniform(0.6, 0.99))
            estado     = self._estado(retraso_l)
            causa      = self._rng.choice(CAUSAS_RETRASO) if retraso_l > 15 else None

            rows.append((
                vuelo_id, fecha_dt,
                orig, dest, aerolinea,
                hora_sal_p, hora_sal_r,
                hora_lle_p, hora_lle_r,
                retraso_s, retraso_l,
                duracion,
                round(self._distancia_km(orig, dest), 1),
                pasajeros, capacidad,
                estado, causa,
            ))

        return self._spark.createDataFrame(rows, self._fact_schema())

    def vuelos_modificados(self, fecha: str, n: int = 15) -> DataFrame:
        """
        Genera n vuelos que simulan correcciones de registros ya existentes
        para una fecha — retrasos ajustados, estados actualizados.
        Usado para probar UPSERT.
        """
        fecha_dt = date.fromisoformat(fecha)
        rows     = []
        pares    = self._pares_ruta(n)

        for i, (orig, dest) in enumerate(pares):
            aerolinea = self._rng.choice(AEROLINEAS)[0]
            # IDs que coinciden con los generados por vuelos() para esa fecha
            vuelo_id  = f"{aerolinea}-{fecha.replace('-', '')}-{i+1:04d}"
            hora_sal_p = self._hora_aleatoria()
            # Retraso corregido — generalmente menor que el original
            retraso_s  = max(0, self._retraso() - self._rng.randint(5, 20))
            hora_sal_r = self._sumar_minutos(hora_sal_p, retraso_s)
            duracion   = self._duracion(orig, dest)
            hora_lle_p = self._sumar_minutos(hora_sal_p, duracion)
            retraso_l  = retraso_s + self._rng.randint(-5, 10)
            hora_lle_r = self._sumar_minutos(hora_sal_r, duracion)
            capacidad  = self._rng.choice([150, 180, 200, 220, 280])
            pasajeros  = int(capacidad * self._rng.uniform(0.6, 0.99))
            estado     = self._estado(retraso_l)
            causa      = self._rng.choice(CAUSAS_RETRASO) if retraso_l > 15 else None

            rows.append((
                vuelo_id, fecha_dt,
                orig, dest, aerolinea,
                hora_sal_p, hora_sal_r,
                hora_lle_p, hora_lle_r,
                retraso_s, retraso_l,
                duracion,
                round(self._distancia_km(orig, dest), 1),
                pasajeros, capacidad,
                estado, causa,
            ))

        return self._spark.createDataFrame(rows, self._fact_schema())

    def vuelos_nueva_particion(self, fecha: str, n: int = 60) -> DataFrame:
        """Alias semántico de vuelos() — para claridad en el pipeline."""
        return self.vuelos(fecha, n)

    # ── Helpers privados ──────────────────────────────────────────────────

    def _pares_ruta(self, n: int) -> list[tuple[str, str]]:
        iatas = [a[0] for a in AEROPUERTOS]
        pares = []
        for _ in range(n):
            orig, dest = self._rng.sample(iatas, 2)
            pares.append((orig, dest))
        return pares

    def _hora_aleatoria(self) -> str:
        h = self._rng.randint(5, 22)
        m = self._rng.choice([0, 15, 30, 45])
        return f"{h:02d}:{m:02d}"

    def _retraso(self) -> int:
        """80% puntual (0-10 min), 15% moderado, 5% severo."""
        r = self._rng.random()
        if r < 0.80:
            return self._rng.randint(0, 10)
        if r < 0.95:
            return self._rng.randint(11, 60)
        return self._rng.randint(61, 240)

    @staticmethod
    def _sumar_minutos(hora: str, minutos: int) -> str:
        h, m = map(int, hora.split(":"))
        total = h * 60 + m + minutos
        total = total % (24 * 60)
        return f"{total // 60:02d}:{total % 60:02d}"

    def _duracion(self, orig: str, dest: str) -> int:
        dist = self._distancia_km(orig, dest)
        base = int(dist / 800 * 60)  # ~800 km/h crucero
        return max(30, base + self._rng.randint(-10, 20))

    @staticmethod
    def _distancia_km(orig: str, dest: str) -> float:
        coords = {a[0]: (a[4], a[5]) for a in AEROPUERTOS}
        if orig not in coords or dest not in coords:
            return 500.0
        lat1, lon1 = map(radians, coords[orig])
        lat2, lon2 = map(radians, coords[dest])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a    = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        return 6371 * 2 * atan2(sqrt(a), sqrt(1-a))

    @staticmethod
    def _estado(retraso_llegada: int) -> str:
        if retraso_llegada <= 15:
            return "ON_TIME"
        if retraso_llegada <= 60:
            return "DELAYED"
        return "DELAYED"

    @staticmethod
    def _fact_schema() -> T.StructType:
        return T.StructType([
            T.StructField("vuelo_id",               T.StringType(),  False),
            T.StructField("fecha",                  T.DateType(),    False),
            T.StructField("iata_origen",            T.StringType(),  False),
            T.StructField("iata_destino",           T.StringType(),  False),
            T.StructField("iata_aerolinea",         T.StringType(),  False),
            T.StructField("hora_salida_programada", T.StringType(),  True),
            T.StructField("hora_salida_real",       T.StringType(),  True),
            T.StructField("hora_llegada_programada",T.StringType(),  True),
            T.StructField("hora_llegada_real",      T.StringType(),  True),
            T.StructField("retraso_salida_min",     T.IntegerType(), True),
            T.StructField("retraso_llegada_min",    T.IntegerType(), True),
            T.StructField("duracion_min",           T.IntegerType(), True),
            T.StructField("distancia_km",           T.DoubleType(),  True),
            T.StructField("pasajeros",              T.IntegerType(), True),
            T.StructField("capacidad",              T.IntegerType(), True),
            T.StructField("estado",                 T.StringType(),  True),
            T.StructField("causa_retraso",          T.StringType(),  True),
        ])
