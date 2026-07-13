"""Binning de passenger_count en categorias nominales para K-Modes.

Regla reutilizable por el feature store y por marts que necesiten agrupar
pasajeros. Acorde al plan: 1 -> Solo, 2 -> Pareja, 3-4 -> Grupo pequeno,
5+ -> Grupo grande, NULL -> Desconocido.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def passenger_group_py(count: int | None) -> str:
    if count is None:
        return "Desconocido"
    if count == 1:
        return "Solo"
    if count == 2:
        return "Pareja"
    if count <= 4:
        return "Grupo pequeño"
    return "Grupo grande"


def passenger_group(count: Column) -> Column:
    """Clasifica el numero de pasajeros en 5 categorias nominales."""
    return (
        F.when(count.isNull(), F.lit("Desconocido"))
        .when(count == 1, F.lit("Solo"))
        .when(count == 2, F.lit("Pareja"))
        .when((count >= 3) & (count <= 4), F.lit("Grupo pequeño"))
        .when(count >= 5, F.lit("Grupo grande"))
        .otherwise(F.lit("Desconocido"))
    )
