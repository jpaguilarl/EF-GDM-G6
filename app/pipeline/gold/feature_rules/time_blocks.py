"""Reglas reusables de bloques de tiempo (compartidas por marts y feature store).

Cada funcion recibe una Column (hora 0-23, o dia de semana ISO 1=Lunes..7=Domingo)
y devuelve una Column categorica. Esta es la unica fuente de verdad de los umbrales
horarios: cambiarlos aqui mantiene dashboards y modelos consistentes.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def bloque_horario(hour: Column) -> Column:
    """Bloque operativo de la hora de pickup (D1.1 Volumen y Demanda)."""
    return (
        F.when((hour >= 0) & (hour <= 5), F.lit("Madrugada"))
        .when((hour >= 6) & (hour <= 9), F.lit("Punta Mañana"))
        .when((hour >= 10) & (hour <= 15), F.lit("Mediodía"))
        .when((hour >= 16) & (hour <= 19), F.lit("Punta Tarde"))
        .when((hour >= 20) & (hour <= 23), F.lit("Noche"))
        .otherwise(F.lit(None).cast("string"))
    )


def franja_horaria(hour: Column) -> Column:
    """Franja categorica nominal para K-Modes (D3.2): 4 buckets amplios."""
    return (
        F.when((hour >= 0) & (hour <= 5), F.lit("Madrugada"))
        .when((hour >= 6) & (hour <= 11), F.lit("Mañana"))
        .when((hour >= 12) & (hour <= 18), F.lit("Tarde"))
        .when((hour >= 19) & (hour <= 23), F.lit("Noche"))
        .otherwise(F.lit(None).cast("string"))
    )


def dia_categoria(iso_weekday: Column) -> Column:
    """Dicotomica laborable / fin de semana (ISO 1=Lunes .. 7=Domingo)."""
    return F.when(iso_weekday >= 6, F.lit("Fin de Semana")).otherwise(
        F.lit("Día Laborable")
    )


def is_weekend(iso_weekday: Column) -> Column:
    """True para sabado (6) y domingo (7) en convencion ISO."""
    return iso_weekday >= 6


def iso_weekday(ts: Column) -> Column:
    """Dia de semana ISO (1=Lunes .. 7=Domingo) desde un timestamp/fecha.

    Se deriva de ``dayofweek`` (1=Domingo .. 7=Sabado) reindexado a ISO. NO se usa
    ``date_format(ts, 'u')`` porque en el calendario proleptico de Spark 3+/4 el
    patron 'u' no representa el dia de la semana (difiere de SimpleDateFormat).
    Coincide con ``isoweekday()`` usado en silver ``dim_date.weekday``.
    """
    return ((F.dayofweek(ts) + 5) % 7) + 1
