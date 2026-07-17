from pathlib import Path

import polars as pl
from polars import ScanCastOptions

from app.utils.globals import globals

MARTS_DIR = globals.project_root / "data/gold/marts"

_SCAN_CAST_OPTS = ScanCastOptions(
    integer_cast="allow-float",
)


def _scan(
    mart: str,
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> pl.LazyFrame | None:
    d = (marts_dir or MARTS_DIR) / mart
    if not d.exists():
        return None
    files = sorted(d.rglob("*.parquet"))
    if not files:
        return None
    lf = pl.scan_parquet(
        [str(f) for f in files],
        cast_options=_SCAN_CAST_OPTS,
        hive_partitioning=True,
    )
    schema = lf.collect_schema()
    if years is not None and "year" in schema:
        lf = lf.filter(pl.col("year").is_in(years))
    if months is not None and "month" in schema:
        lf = lf.filter(pl.col("month").is_in(months))
    return lf





def read_mart_summary(
    mart: str,
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    if mart == "mart_demand_volume":
        return _demand_volume_summary(years, months, marts_dir)
    elif mart == "mart_financial_performance":
        return _financial_performance_summary(years, months, marts_dir)
    elif mart == "mart_operational_profile":
        return _operational_profile_summary(years, months, marts_dir)
    elif mart == "mart_supply_demand_balance":
        return _supply_demand_balance_summary(years, months, marts_dir)
    elif mart == "mart_abc_xyz_zones":
        return _abc_xyz_zones_summary(years, months, marts_dir)
    elif mart == "mart_tipping_behavior":
        return _tipping_behavior_summary(years, months, marts_dir)
    return {}


def _demand_volume_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_demand_volume", years, months, marts_dir)
    if lf is None:
        return {}
    timeline = (
        lf.group_by("fecha_viaje")
        .agg(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("espera_total_min").sum().alias("espera_total_min"),
        )
        .sort("fecha_viaje")
        .collect()
        .to_dicts()
    )
    top_zones = (
        lf.group_by("pu_zone")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("viajes", descending=True)
        .limit(10)
        .collect()
        .to_dicts()
    )
    by_service = (
        lf.group_by("service_id")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("service_id")
        .collect()
        .to_dicts()
    )
    by_hour = (
        lf.group_by(["pickup_hour", "service_id"])
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("pickup_hour")
        .collect()
        .to_dicts()
    )
    by_borough = (
        lf.group_by("pu_borough")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("viajes", descending=True)
        .collect()
        .to_dicts()
    )
    total_row = (
        lf.select(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("espera_total_min").sum().alias("espera_total_min"),
            pl.col("viajes_con_espera").sum().alias("viajes_con_espera"),
        )
        .collect()
        .to_dicts()
    )
    return {
        "timeline": timeline,
        "top_zones": top_zones,
        "by_service": by_service,
        "by_hour": by_hour,
        "by_borough": by_borough,
        "total": total_row[0] if total_row else {},
    }


def _financial_performance_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_financial_performance", years, months, marts_dir)
    if lf is None:
        return {}

    # total — single row for KPI cards
    tot_rows = (
        lf.select(
            (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias(
                "ingreso_bruto"
            ),
            pl.col("margen_plataforma").sum().alias("margen_plataforma"),
            pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
            pl.col("driver_pay").sum().alias("driver_pay"),
        )
        .collect()
        .to_dicts()
    )
    t: dict = tot_rows[0] if tot_rows else {}
    bpf_tot = t.get("base_passenger_fare")
    if bpf_tot and bpf_tot > 0:
        t["ratio_pago_conductor"] = round(t["driver_pay"] / bpf_tot, 4)
        t["margen_promedio"] = round(t["margen_plataforma"] / bpf_tot, 4)
    else:
        t["ratio_pago_conductor"] = None
        t["margen_promedio"] = None

    # by_service — grouped bar chart (unified coalesce per codebase convention)
    by_svc = (
        lf.group_by("service_id")
        .agg(
            pl.coalesce("fare_amount", "base_passenger_fare")
            .fill_null(0)
            .sum()
            .alias("fare"),
            pl.coalesce("tip_amount", "tips").fill_null(0).sum().alias("tips"),
            pl.coalesce("tolls_amount", "tolls").fill_null(0).sum().alias("tolls"),
            pl.coalesce("congestion_surcharge", "bcf")
            .fill_null(0)
            .sum()
            .alias("congestion"),
            (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias(
                "ingreso_bruto"
            ),
            pl.col("margen_plataforma").sum().alias("margen"),
            pl.col("driver_pay").sum().alias("driver_pay"),
            pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
        )
        .sort("service_id")
        .collect()
        .to_dicts()
    )
    for row in by_svc:
        bpf = row["base_passenger_fare"]
        row["ratio"] = round(row["driver_pay"] / bpf, 4) if bpf and bpf > 0 else None
        del row["driver_pay"]
        del row["base_passenger_fare"]

    # matrix — table rows (año × mes × servicio)
    mat = (
        lf.group_by(["year", "month", "service_id"])
        .agg(
            (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias(
                "ingreso_total"
            ),
            pl.col("margen_plataforma").sum().alias("margen"),
            pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
        )
        .sort(["year", "month", "service_id"])
        .collect()
        .to_dicts()
    )
    for row in mat:
        bpf = row["base_passenger_fare"]
        m = row["margen"]
        row["margen_promedio"] = (
            round(m / bpf, 4) if bpf and bpf > 0 and m is not None else None
        )
        del row["margen"]
        del row["base_passenger_fare"]

    return {
        "total": t,
        "by_service": by_svc,
        "matrix": mat,
    }


def _operational_profile_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_operational_profile", years, months, marts_dir)
    if lf is None:
        return {}
    by_block = (
        lf.group_by("bloque_horario")
        .agg(
            pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
            pl.col("distancia_promedio_millas").mean().alias("distancia_promedio"),
            pl.col("distancia_total_millas").sum().alias("distancia_total"),
            pl.col("duracion_promedio_min").mean().alias("duracion_promedio"),
            pl.col("tasa_ocupacion_compartida").mean().alias("tasa_ocupacion"),
        )
        .sort("bloque_horario")
        .collect()
        .to_dicts()
    )
    by_borough = (
        lf.group_by("pu_borough")
        .agg(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
            pl.col("distancia_promedio_millas").mean().alias("distancia_promedio"),
        )
        .sort("viajes", descending=True)
        .collect()
        .to_dicts()
    )
    scatter = (
        lf.group_by(["bloque_horario", "service_id"])
        .agg(
            pl.col("duracion_promedio_min").mean().alias("duracion"),
            pl.col("distancia_promedio_millas").mean().alias("distancia"),
        )
        .collect()
        .to_dicts()
    )
    shared = (
        lf.select(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("viajes_match_compartido").sum().alias("viajes_match"),
        )
        .collect()
        .to_dicts()
    )
    total_row = (
        lf.select(
            pl.col("duracion_promedio_min").mean().alias("duracion_promedio"),
            pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
            pl.col("tasa_ocupacion_compartida").mean().alias("tasa_ocupacion"),
        )
        .collect()
        .to_dicts()
    )
    return {
        "by_block": by_block,
        "by_borough": by_borough,
        "scatter": scatter,
        "shared_efficiency": shared[0] if shared else {},
        "total": total_row[0] if total_row else {},
    }


def _supply_demand_balance_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_supply_demand_balance", years, months, marts_dir)
    if lf is None:
        return {}
    by_borough = (
        lf.group_by("borough")
        .agg(
            pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"),
            pl.col("deficit_severo_flag")
            .cast(pl.Int32)
            .sum()
            .alias("deficit_count"),
            pl.len().alias("periods"),
        )
        .sort("borough")
        .collect()
        .to_dicts()
    )
    total_periods = sum(c["periods"] for c in by_borough)
    total_deficit = sum(c["deficit_count"] for c in by_borough)

    # Zone-hour matrix for heatmap — top 20 zones by block count
    zone_ranking = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.len().alias("_cnt"))
        .sort("_cnt", descending=True)
        .limit(20)
        .collect()
    )
    top_ids = zone_ranking["location_id"].to_list()
    by_zone_hour = (
        lf.filter(pl.col("location_id").is_in(top_ids))
        .with_columns(pl.col("bloque_temporal_t").dt.hour().alias("hour"))
        .group_by("location_id", "zone", "borough", "hour")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("location_id", "hour")
        .collect()
        .to_dicts()
    )

    # Top 10 deficit zones (Desiertos de Servicio)
    top_deficit_zones = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("flujo_neto_oferta")
        .limit(10)
        .collect()
        .to_dicts()
    )

    # Top 10 surplus zones (Acumulación de Vehículos)
    top_surplus_zones = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("flujo_neto_oferta", descending=True)
        .limit(10)
        .collect()
        .to_dicts()
    )

    # Critical zones (any deficit_severo_flag period)
    critical_zones = (
        lf.filter(pl.col("deficit_severo_flag"))
        .select(pl.col("location_id").n_unique())
        .collect()
        .item()
    )

    # Global net flow
    global_net_flow = (
        lf.select(pl.col("flujo_neto_oferta").sum())
        .collect()
        .item()
    )

    # Average wait time from demand-volume mart
    dv_lf = _scan("mart_demand_volume", years, months, marts_dir)
    avg_wait_min = None
    if dv_lf is not None:
        r = dv_lf.select(pl.col("espera_promedio_min").mean()).collect()
        avg_wait_min = r.item()

    return {
        "by_borough": by_borough,
        "deficit_ratio": round(total_deficit / total_periods * 100, 2)
        if total_periods > 0
        else 0,
        "total_periods": total_periods,
        "by_zone_hour": by_zone_hour,
        "top_deficit_zones": top_deficit_zones,
        "top_surplus_zones": top_surplus_zones,
        "critical_zones_count": critical_zones,
        "global_net_flow": global_net_flow,
        "avg_wait_min": avg_wait_min,
    }


def _abc_xyz_zones_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_abc_xyz_zones", years, months, marts_dir)
    if lf is None:
        return {}
    scatter = lf.collect().to_dicts()
    abc_dist = (
        lf.group_by("clase_abc")
        .agg(pl.len().alias("count"))
        .sort("clase_abc")
        .collect()
        .to_dicts()
    )
    xyz_dist = (
        lf.group_by("clase_xyz")
        .agg(pl.len().alias("count"))
        .sort("clase_xyz")
        .collect()
        .to_dicts()
    )
    return {
        "scatter": scatter,
        "abc_distribution": abc_dist,
        "xyz_distribution": xyz_dist,
    }


def _tipping_behavior_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_tipping_behavior", years, months, marts_dir)
    if lf is None:
        return {}

    # Filter to card-paying trips (non-null categoria_generosidad)
    card_lf = lf.filter(pl.col("categoria_generosidad").is_not_null())

    # Total KPIs
    total_row = (
        card_lf.select(
            pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina_promedio"),
            pl.col("propina_por_milla").mean().alias("propina_prom_por_milla"),
            pl.col("viajes").sum().alias("viajes"),
            pl.col("viajes_con_propina").sum().alias("viajes_con_propina"),
        )
        .collect()
        .to_dicts()
    )
    t = total_row[0] if total_row else {}
    viajes_total = t.get("viajes", 0) or 0
    viajes_con_propina = t.get("viajes_con_propina", 0) or 0
    t["pct_viajes_sin_propina"] = (
        round((1 - viajes_con_propina / viajes_total) * 100, 2)
        if viajes_total > 0 else 0.0
    )
    del t["viajes_con_propina"]

    # by_borough_origin: group by pickup borough
    by_borough_origin = (
        card_lf.group_by("pu_borough")
        .agg(
            pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina"),
            pl.col("viajes").sum().alias("viajes"),
        )
        .sort("viajes", descending=True)
        .collect()
        .to_dicts()
    )

    # by_borough_destination: group by dropoff borough
    by_borough_destination = (
        card_lf.group_by("do_borough")
        .agg(
            pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina"),
            pl.col("viajes").sum().alias("viajes"),
        )
        .sort("viajes", descending=True)
        .collect()
        .to_dicts()
    )

    # generosity_by_service: cross-tab service_id × categoria_generosidad
    gen_by_svc = (
        lf.group_by(["service_id", "categoria_generosidad"])
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("service_id", "categoria_generosidad")
        .collect()
        .to_dicts()
    )

    return {
        "total": t,
        "by_borough_origin": by_borough_origin,
        "by_borough_destination": by_borough_destination,
        "generosity_by_service": gen_by_svc,
    }
