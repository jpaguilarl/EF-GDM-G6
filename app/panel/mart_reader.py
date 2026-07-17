from pathlib import Path

import polars as pl
from polars import ScanCastOptions

from app.panel._cache import ttl_cache
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


@ttl_cache(ttl_seconds=300)
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
    timeline_lf = (
        lf.group_by("fecha_viaje")
        .agg(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("espera_total_min").sum().alias("espera_total_min"),
        )
        .sort("fecha_viaje")
    )
    top_zones_lf = (
        lf.group_by("pu_zone")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("viajes", descending=True)
        .limit(10)
    )
    by_service_lf = (
        lf.group_by("service_id")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("service_id")
    )
    by_hour_lf = (
        lf.group_by(["pickup_hour", "service_id"])
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("pickup_hour")
    )
    by_borough_lf = (
        lf.group_by("pu_borough")
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("viajes", descending=True)
    )
    total_lf = lf.select(
        pl.col("viajes").sum().alias("viajes"),
        pl.col("espera_total_min").sum().alias("espera_total_min"),
        pl.col("viajes_con_espera").sum().alias("viajes_con_espera"),
    )
    results = pl.collect_all([
        timeline_lf, top_zones_lf, by_service_lf, by_hour_lf,
        by_borough_lf, total_lf,
    ])
    return {
        "timeline": results[0].to_dicts(),
        "top_zones": results[1].to_dicts(),
        "by_service": results[2].to_dicts(),
        "by_hour": results[3].to_dicts(),
        "by_borough": results[4].to_dicts(),
        "total": results[5].to_dicts()[0] if results[5].height > 0 else {},
    }

def _financial_performance_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_financial_performance", years, months, marts_dir)
    if lf is None:
        return {}

    total_lf = lf.select(
        (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias("ingreso_bruto"),
        pl.col("margen_plataforma").sum().alias("margen_plataforma"),
        pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
        pl.col("driver_pay").sum().alias("driver_pay"),
    )

    by_svc_lf = (
        lf.group_by("service_id")
        .agg(
            pl.coalesce("fare_amount", "base_passenger_fare").fill_null(0).sum().alias("fare"),
            pl.coalesce("tip_amount", "tips").fill_null(0).sum().alias("tips"),
            pl.coalesce("tolls_amount", "tolls").fill_null(0).sum().alias("tolls"),
            pl.coalesce("congestion_surcharge", "bcf").fill_null(0).sum().alias("congestion"),
            (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias("ingreso_bruto"),
            pl.col("margen_plataforma").sum().alias("margen"),
            pl.col("driver_pay").sum().alias("driver_pay"),
            pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
        )
        .sort("service_id")
    )

    mat_lf = (
        lf.group_by(["year", "month", "service_id"])
        .agg(
            (pl.col("total_amount").sum() + pl.col("base_passenger_fare").sum()).alias("ingreso_total"),
            pl.col("margen_plataforma").sum().alias("margen"),
            pl.col("base_passenger_fare").sum().alias("base_passenger_fare"),
        )
        .sort(["year", "month", "service_id"])
    )

    tot_df, by_svc_df, mat_df = pl.collect_all([total_lf, by_svc_lf, mat_lf])

    t: dict = tot_df.to_dicts()[0] if tot_df.height > 0 else {}
    bpf_tot = t.get("base_passenger_fare")
    if bpf_tot and bpf_tot > 0:
        t["ratio_pago_conductor"] = round(t["driver_pay"] / bpf_tot, 4)
        t["margen_promedio"] = round(t["margen_plataforma"] / bpf_tot, 4)
    else:
        t["ratio_pago_conductor"] = None
        t["margen_promedio"] = None

    by_svc = by_svc_df.to_dicts()
    for row in by_svc:
        bpf = row["base_passenger_fare"]
        row["ratio"] = round(row["driver_pay"] / bpf, 4) if bpf and bpf > 0 else None
        del row["driver_pay"]
        del row["base_passenger_fare"]

    mat = mat_df.to_dicts()
    for row in mat:
        bpf = row["base_passenger_fare"]
        m = row["margen"]
        row["margen_promedio"] = round(m / bpf, 4) if bpf and bpf > 0 and m is not None else None
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
    by_block_lf = (
        lf.group_by("bloque_horario")
        .agg(
            pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
            pl.col("distancia_promedio_millas").mean().alias("distancia_promedio"),
            pl.col("distancia_total_millas").sum().alias("distancia_total"),
            pl.col("duracion_promedio_min").mean().alias("duracion_promedio"),
            pl.col("tasa_ocupacion_compartida").mean().alias("tasa_ocupacion"),
        )
        .sort("bloque_horario")
    )
    by_borough_lf = (
        lf.group_by("pu_borough")
        .agg(
            pl.col("viajes").sum().alias("viajes"),
            pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
            pl.col("distancia_promedio_millas").mean().alias("distancia_promedio"),
        )
        .sort("viajes", descending=True)
    )
    scatter_lf = (
        lf.group_by(["bloque_horario", "service_id"])
        .agg(
            pl.col("duracion_promedio_min").mean().alias("duracion"),
            pl.col("distancia_promedio_millas").mean().alias("distancia"),
        )
    )
    shared_lf = lf.select(
        pl.col("viajes").sum().alias("viajes"),
        pl.col("viajes_match_compartido").sum().alias("viajes_match"),
    )
    total_lf = lf.select(
        pl.col("duracion_promedio_min").mean().alias("duracion_promedio"),
        pl.col("velocidad_promedio_mph").mean().alias("velocidad_promedio"),
        pl.col("tasa_ocupacion_compartida").mean().alias("tasa_ocupacion"),
    )
    results = pl.collect_all([by_block_lf, by_borough_lf, scatter_lf, shared_lf, total_lf])
    return {
        "by_block": results[0].to_dicts(),
        "by_borough": results[1].to_dicts(),
        "scatter": results[2].to_dicts(),
        "shared_efficiency": results[3].to_dicts()[0] if results[3].height > 0 else {},
        "total": results[4].to_dicts()[0] if results[4].height > 0 else {},
    }


def _supply_demand_balance_summary(
    years: list[int] | None = None,
    months: list[int] | None = None,
    marts_dir: Path | None = None,
) -> dict:
    lf = _scan("mart_supply_demand_balance", years, months, marts_dir)
    if lf is None:
        return {}

    by_borough_lf = (
        lf.group_by("borough")
        .agg(
            pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"),
            pl.col("deficit_severo_flag").cast(pl.Int32).sum().alias("deficit_count"),
            pl.len().alias("periods"),
        )
        .sort("borough")
    )
    zone_ranking_lf = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.len().alias("_cnt"))
        .sort("_cnt", descending=True)
        .limit(20)
    )
    top_deficit_lf = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("flujo_neto_oferta")
        .limit(10)
    )
    top_surplus_lf = (
        lf.group_by("location_id", "zone", "borough")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("flujo_neto_oferta", descending=True)
        .limit(10)
    )
    critical_lf = (
        lf.filter(pl.col("deficit_severo_flag"))
        .select(pl.col("location_id").n_unique().alias("cnt"))
    )
    global_flow_lf = lf.select(pl.col("flujo_neto_oferta").sum().alias("flow"))

    by_borough_df, zone_ranking_df, top_deficit_df, top_surplus_df, critical_df, global_flow_df = (
        pl.collect_all([by_borough_lf, zone_ranking_lf, top_deficit_lf, top_surplus_lf, critical_lf, global_flow_lf])
    )

    total_periods = by_borough_df["periods"].sum()
    total_deficit = by_borough_df["deficit_count"].sum()

    top_ids = zone_ranking_df["location_id"].to_list()
    by_zone_hour = () if not top_ids else (
        lf.filter(pl.col("location_id").is_in(top_ids))
        .with_columns(pl.col("bloque_temporal_t").dt.hour().alias("hour"))
        .group_by("location_id", "zone", "borough", "hour")
        .agg(pl.col("flujo_neto_oferta").sum().alias("flujo_neto_oferta"))
        .sort("location_id", "hour")
        .collect()
        .to_dicts()
    )

    # Average wait time from demand-volume mart
    dv_lf = _scan("mart_demand_volume", years, months, marts_dir)
    avg_wait_min = None
    if dv_lf is not None:
        r = dv_lf.select(pl.col("espera_promedio_min").mean()).collect()
        avg_wait_min = r.item()

    return {
        "by_borough": by_borough_df.to_dicts(),
        "deficit_ratio": round(total_deficit / total_periods * 100, 2) if total_periods > 0 else 0,
        "total_periods": int(total_periods),
        "by_zone_hour": by_zone_hour,
        "top_deficit_zones": top_deficit_df.to_dicts(),
        "top_surplus_zones": top_surplus_df.to_dicts(),
        "critical_zones_count": int(critical_df.item() if critical_df.height > 0 else 0),
        "global_net_flow": float(global_flow_df.item() if global_flow_df.height > 0 else 0),
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
    abc_dist_lf = (
        lf.group_by("clase_abc")
        .agg(pl.len().alias("count"))
        .sort("clase_abc")
    )
    xyz_dist_lf = (
        lf.group_by("clase_xyz")
        .agg(pl.len().alias("count"))
        .sort("clase_xyz")
    )
    scatter_df, abc_df, xyz_df = pl.collect_all([lf, abc_dist_lf, xyz_dist_lf])
    return {
        "scatter": scatter_df.to_dicts(),
        "abc_distribution": abc_df.to_dicts(),
        "xyz_distribution": xyz_df.to_dicts(),
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

    total_lf = card_lf.select(
        pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina_promedio"),
        pl.col("propina_por_milla").mean().alias("propina_prom_por_milla"),
        pl.col("viajes").sum().alias("viajes"),
        pl.col("viajes_con_propina").sum().alias("viajes_con_propina"),
    )
    by_borough_origin_lf = (
        card_lf.group_by("pu_borough")
        .agg(
            pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina"),
            pl.col("viajes").sum().alias("viajes"),
        )
        .sort("viajes", descending=True)
    )
    by_borough_dest_lf = (
        card_lf.group_by("do_borough")
        .agg(
            pl.col("porcentaje_propina_ponderado").mean().alias("pct_propina"),
            pl.col("viajes").sum().alias("viajes"),
        )
        .sort("viajes", descending=True)
    )
    gen_by_svc_lf = (
        lf.group_by(["service_id", "categoria_generosidad"])
        .agg(pl.col("viajes").sum().alias("viajes"))
        .sort("service_id", "categoria_generosidad")
    )

    total_df, origin_df, dest_df, gen_df = pl.collect_all([total_lf, by_borough_origin_lf, by_borough_dest_lf, gen_by_svc_lf])

    t = total_df.to_dicts()[0] if total_df.height > 0 else {}
    viajes_total = t.get("viajes", 0) or 0
    viajes_con_propina = t.get("viajes_con_propina", 0) or 0
    t["pct_viajes_sin_propina"] = (
        round((1 - viajes_con_propina / viajes_total) * 100, 2)
        if viajes_total > 0 else 0.0
    )
    del t["viajes_con_propina"]

    return {
        "total": t,
        "by_borough_origin": origin_df.to_dicts(),
        "by_borough_destination": dest_df.to_dicts(),
        "generosity_by_service": gen_df.to_dicts(),
    }
