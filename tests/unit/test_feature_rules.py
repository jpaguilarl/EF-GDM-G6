from datetime import datetime

from pyspark.sql import Row, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from app.pipeline.gold.feature_rules.generosity import categoria_generosidad
from app.pipeline.gold.feature_rules.passenger_groups import passenger_group
from app.pipeline.gold.feature_rules.ratecode_tariff import (
    FLAT_FARES,
    MAX_COST_PER_MILE,
    MAX_PLAUSIBLE_SPEED_MPH,
    desviacion_tarifa_teorica,
    flat_fare_rows,
    is_anomaly_candidate,
)
from app.pipeline.gold.feature_rules.time_blocks import (
    bloque_horario,
    dia_categoria,
    franja_horaria,
    is_weekend,
    iso_weekday,
)


class TestBloqueHorario:
    def test_madrugada(self, spark):
        df = spark.createDataFrame([Row(hour=0), Row(hour=3), Row(hour=5)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b == "Madrugada" for r in result)

    def test_punta_manana(self, spark):
        df = spark.createDataFrame([Row(hour=6), Row(hour=9)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b == "Punta Mañana" for r in result)

    def test_mediodia(self, spark):
        df = spark.createDataFrame([Row(hour=10), Row(hour=15)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b == "Mediodía" for r in result)

    def test_punta_tarde(self, spark):
        df = spark.createDataFrame([Row(hour=16), Row(hour=19)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b == "Punta Tarde" for r in result)

    def test_noche(self, spark):
        df = spark.createDataFrame([Row(hour=20), Row(hour=23)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b == "Noche" for r in result)

    def test_invalid_hour(self, spark):
        df = spark.createDataFrame([Row(hour=-1), Row(hour=24)])
        result = df.select(bloque_horario(F.col("hour")).alias("b")).collect()
        assert all(r.b is None for r in result)


class TestFranjaHoraria:
    def test_madrugada(self, spark):
        df = spark.createDataFrame([Row(hour=0), Row(hour=5)])
        result = df.select(franja_horaria(F.col("hour")).alias("f")).collect()
        assert all(r.f == "Madrugada" for r in result)

    def test_manana(self, spark):
        df = spark.createDataFrame([Row(hour=6), Row(hour=11)])
        result = df.select(franja_horaria(F.col("hour")).alias("f")).collect()
        assert all(r.f == "Mañana" for r in result)

    def test_tarde(self, spark):
        df = spark.createDataFrame([Row(hour=12), Row(hour=18)])
        result = df.select(franja_horaria(F.col("hour")).alias("f")).collect()
        assert all(r.f == "Tarde" for r in result)

    def test_noche(self, spark):
        df = spark.createDataFrame([Row(hour=19), Row(hour=23)])
        result = df.select(franja_horaria(F.col("hour")).alias("f")).collect()
        assert all(r.f == "Noche" for r in result)


class TestDiaCategoria:
    def test_laborable(self, spark):
        df = spark.createDataFrame([Row(wd=1), Row(wd=5)])
        result = df.select(dia_categoria(F.col("wd")).alias("d")).collect()
        assert all(r.d == "Día Laborable" for r in result)

    def test_fin_de_semana(self, spark):
        df = spark.createDataFrame([Row(wd=6), Row(wd=7)])
        result = df.select(dia_categoria(F.col("wd")).alias("d")).collect()
        assert all(r.d == "Fin de Semana" for r in result)


class TestIsWeekend:
    def test_weekend_true(self, spark):
        df = spark.createDataFrame([Row(wd=6), Row(wd=7)])
        result = df.select(is_weekend(F.col("wd")).alias("w")).collect()
        assert all(r.w for r in result)

    def test_weekday_false(self, spark):
        df = spark.createDataFrame([Row(wd=1), Row(wd=5)])
        result = df.select(is_weekend(F.col("wd")).alias("w")).collect()
        assert all(not r.w for r in result)


class TestIsoWeekday:
    def test_monday(self, spark):
        df = spark.createDataFrame([Row(ts=datetime(2023, 1, 2))])
        result = df.select(iso_weekday(F.col("ts")).alias("d")).collect()
        assert result[0].d == 1

    def test_saturday(self, spark):
        df = spark.createDataFrame([Row(ts=datetime(2023, 1, 7))])
        result = df.select(iso_weekday(F.col("ts")).alias("d")).collect()
        assert result[0].d == 6


class TestCategoriaGenerosidad:
    def test_null(self, spark):
        schema = StructType([StructField("pct", DoubleType(), True)])
        df = spark.createDataFrame([Row(pct=None)], schema=schema)
        result = df.select(categoria_generosidad(F.col("pct")).alias("c")).collect()
        assert result[0].c is None

    def test_sin_propina(self, spark):
        df = spark.createDataFrame([Row(pct=0.0), Row(pct=-1.0)])
        result = df.select(categoria_generosidad(F.col("pct")).alias("c")).collect()
        assert all(r.c == "Sin Propina" for r in result)

    def test_baja(self, spark):
        df = spark.createDataFrame([Row(pct=5.0)])
        result = df.select(categoria_generosidad(F.col("pct")).alias("c")).collect()
        assert result[0].c == "Baja"

    def test_estandar(self, spark):
        df = spark.createDataFrame([Row(pct=10.0), Row(pct=15.0), Row(pct=18.0)])
        result = df.select(categoria_generosidad(F.col("pct")).alias("c")).collect()
        assert all(r.c == "Estándar" for r in result)

    def test_alta(self, spark):
        df = spark.createDataFrame([Row(pct=25.0)])
        result = df.select(categoria_generosidad(F.col("pct")).alias("c")).collect()
        assert result[0].c == "Alta"

    def test_custom_thresholds(self, spark):
        df = spark.createDataFrame([Row(pct=5.0)])
        result = df.select(
            categoria_generosidad(F.col("pct"), standard_low=3.0, standard_high=10.0).alias("c")
        ).collect()
        assert result[0].c == "Estándar"


class TestRatecodeTariff:
    def test_flat_fares_jfk_2023(self):
        assert FLAT_FARES[2][2023] == 70.0

    def test_jfk_fare_across_years(self):
        for y in (2023, 2024, 2025):
            assert FLAT_FARES[2][y] == 70.0

    def test_max_plausible_speed(self):
        assert MAX_PLAUSIBLE_SPEED_MPH == 80.0

    def test_max_cost_per_mile(self):
        assert MAX_COST_PER_MILE == 30.0

    def test_flat_fare_rows_includes_jfk(self):
        rows = flat_fare_rows()
        assert (2, 2023, 70.0, "JFK") in rows

    def test_flat_fare_rows_ratecode_1_has_none_flat(self):
        rows = flat_fare_rows()
        for rc, y, flat, name in rows:
            if rc == 1:
                assert flat is None

    def test_flat_fare_rows_ratecode_names(self, spark):
        rows = flat_fare_rows()
        df = spark.createDataFrame(
            [Row(ratecode=rc, year=y, flat=fl, name=n) for rc, y, fl, n in rows]
        )
        names = {r.name for r in df.select("name").distinct().collect()}
        assert "JFK" in names
        assert "Standard rate" in names

    def test_desviacion_null_when_no_flat(self, spark):
        schema = StructType([
            StructField("fare", DoubleType(), True),
            StructField("flat", DoubleType(), True),
        ])
        df = spark.createDataFrame([Row(fare=50.0, flat=None)], schema=schema)
        result = df.select(desviacion_tarifa_teorica(F.col("fare"), F.col("flat")).alias("d")).collect()
        assert result[0].d is None

    def test_desviacion_value(self, spark):
        df = spark.createDataFrame([Row(fare=75.0, flat=70.0)])
        result = df.select(desviacion_tarifa_teorica(F.col("fare"), F.col("flat")).alias("d")).collect()
        assert result[0].d == 5.0

    def test_is_anomaly_candidate_jfk_far_from_flat(self, spark):
        df = spark.createDataFrame([
            Row(ratecode=2, fare=50.0, flat=70.0, speed=30.0, cpm=5.0)
        ])
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is True

    def test_is_anomaly_candidate_jfk_exact_flat_not_anomaly(self, spark):
        df = spark.createDataFrame([
            Row(ratecode=2, fare=70.0, flat=70.0, speed=30.0, cpm=5.0)
        ])
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is False

    def test_is_anomaly_candidate_impossible_speed(self, spark):
        df = spark.createDataFrame([
            Row(ratecode=2, fare=70.0, flat=70.0, speed=100.0, cpm=5.0)
        ])
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is True

    def test_is_anomaly_candidate_meter_tampering(self, spark):
        schema = StructType([
            StructField("ratecode", IntegerType(), True),
            StructField("fare", DoubleType(), True),
            StructField("flat", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("cpm", DoubleType(), True),
        ])
        df = spark.createDataFrame([
            Row(ratecode=1, fare=100.0, flat=None, speed=30.0, cpm=50.0)
        ], schema=schema)
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is True

    def test_is_anomaly_candidate_nonpositive_fare(self, spark):
        schema = StructType([
            StructField("ratecode", IntegerType(), True),
            StructField("fare", DoubleType(), True),
            StructField("flat", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("cpm", DoubleType(), True),
        ])
        df = spark.createDataFrame([
            Row(ratecode=1, fare=0.0, flat=None, speed=0.0, cpm=0.0)
        ], schema=schema)
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is True

    def test_is_anomaly_candidate_negative_fare(self, spark):
        schema = StructType([
            StructField("ratecode", IntegerType(), True),
            StructField("fare", DoubleType(), True),
            StructField("flat", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("cpm", DoubleType(), True),
        ])
        df = spark.createDataFrame([
            Row(ratecode=1, fare=-5.0, flat=None, speed=0.0, cpm=0.0)
        ], schema=schema)
        result = df.select(
            is_anomaly_candidate(F.col("ratecode"), F.col("fare"), F.col("flat"), F.col("speed"), F.col("cpm")).alias("a")
        ).collect()
        assert result[0].a is True


class TestPassengerGroup:
    def test_null(self, spark):
        schema = StructType([StructField("count", IntegerType(), True)])
        df = spark.createDataFrame([Row(count=None)], schema=schema)
        result = df.select(passenger_group(F.col("count")).alias("g")).collect()
        assert result[0].g == "Desconocido"

    def test_solo(self, spark):
        df = spark.createDataFrame([Row(count=1)])
        result = df.select(passenger_group(F.col("count")).alias("g")).collect()
        assert result[0].g == "Solo"

    def test_pareja(self, spark):
        df = spark.createDataFrame([Row(count=2)])
        result = df.select(passenger_group(F.col("count")).alias("g")).collect()
        assert result[0].g == "Pareja"

    def test_grupo_pequeno(self, spark):
        df = spark.createDataFrame([Row(count=3), Row(count=4)])
        result = df.select(passenger_group(F.col("count")).alias("g")).collect()
        assert all(r.g == "Grupo pequeño" for r in result)

    def test_grupo_grande(self, spark):
        df = spark.createDataFrame([Row(count=5), Row(count=9)])
        result = df.select(passenger_group(F.col("count")).alias("g")).collect()
        assert all(r.g == "Grupo grande" for r in result)
