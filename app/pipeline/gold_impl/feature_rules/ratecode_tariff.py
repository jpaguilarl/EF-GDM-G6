"""Tarifas teoricas por RatecodeID + año fiscal y heuristicas de anomalia (D3.3).

NOTA DE DOMINIO: los importes son tarifas reguladas por la TLC y deben verificarse
contra la normativa vigente de cada año. JFK (RatecodeID=2) usa una tarifa plana
que aparece en ``fare_amount``; los recargos (peajes, MTA, mejora, congestion) van
en columnas aparte y NO forman parte de la tarifa plana. Estas reglas solo generan
CANDIDATOS a anomalia (``is_anomaly_candidate``), no el score final del modelo.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

# ratecode_id -> {fiscal_year: flat_fare_usd}. Si no aparece, no es tarifa plana.
FLAT_FARES: dict[int, dict[int, float]] = {
    2: {2023: 70.0, 2024: 70.0, 2025: 70.0},  # JFK <-> Manhattan (tarifa plana)
}

RATECODE_NAMES: dict[int, str] = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Desconocido",
}

# Umbrales heuristicos (documentados; ajustables). No son el score final.
JFK_FARE_TOLERANCE = 15.0  # USD de desviacion tolerada vs la tarifa plana
MAX_PLAUSIBLE_SPEED_MPH = 80.0  # por encima: fisicamente improbable en ciudad
MAX_COST_PER_MILE = 30.0  # costo_por_distancia muy por encima de lo tipico


def flat_fare_rows() -> list[tuple]:
    """Filas (ratecode_id, fiscal_year, flat_fare, ratecode_name) para la dim."""
    years = sorted({y for fares in FLAT_FARES.values() for y in fares})
    rows: list[tuple] = []
    for rc, name in RATECODE_NAMES.items():
        for y in years:
            flat = FLAT_FARES.get(rc, {}).get(y)
            rows.append((rc, y, flat, name))
    return rows


def desviacion_tarifa_teorica(fare: Column, flat_fare: Column) -> Column:
    """|fare_amount - tarifa_plana| cuando aplica una tarifa plana; null si no."""
    return F.when(
        flat_fare.isNotNull(), F.abs(fare - flat_fare)
    ).otherwise(F.lit(None).cast("double"))


def is_anomaly_candidate(
    ratecode: Column,
    fare: Column,
    flat_fare: Column,
    speed_mph: Column,
    cost_per_mile: Column,
) -> Column:
    """Bandera heuristica de candidato a fraude/adulteracion de taximetro."""
    jfk_anomaly = (
        (ratecode == 2)
        & flat_fare.isNotNull()
        & (F.abs(fare - flat_fare) > JFK_FARE_TOLERANCE)
    )
    impossible_speed = speed_mph.isNotNull() & (speed_mph > MAX_PLAUSIBLE_SPEED_MPH)
    meter_tampering = (
        (ratecode == 1)
        & speed_mph.isNotNull()
        & (speed_mph > 0)
        & (speed_mph <= MAX_PLAUSIBLE_SPEED_MPH)
        & (cost_per_mile > MAX_COST_PER_MILE)
    )
    nonpositive_fare = fare <= 0
    return jfk_anomaly | impossible_speed | meter_tampering | nonpositive_fare
