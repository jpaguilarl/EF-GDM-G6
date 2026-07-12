"""Categoria de generosidad de propina (D2.3). Umbrales parametrizables via config."""

from pyspark.sql import Column
from pyspark.sql import functions as F


def categoria_generosidad(
    pct: Column,
    standard_low: float = 10.0,
    standard_high: float = 18.0,
) -> Column:
    """Clasifica el porcentaje de propina en categorias.

    Un ``pct`` nulo permanece nulo (p.ej. efectivo en taxis: la propina no queda
    registrada y no debe contaminar el analisis). Umbrales por defecto:
    Sin Propina [0%], Baja [<low], Estandar [low-high], Alta [>high].
    """
    return (
        F.when(pct.isNull(), F.lit(None).cast("string"))
        .when(pct <= 0, F.lit("Sin Propina"))
        .when(pct < standard_low, F.lit("Baja"))
        .when(pct <= standard_high, F.lit("Estándar"))
        .otherwise(F.lit("Alta"))
    )
