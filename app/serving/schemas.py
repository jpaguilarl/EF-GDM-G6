from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class DemandVolumeRow(BaseModel):
    service_id: str
    fecha_viaje: date
    pickup_hour: int
    bloque_horario: str
    dia_semana: int
    is_weekend: bool
    pu_location_id: Optional[int] = None
    pu_borough: Optional[str] = None
    pu_zone: Optional[str] = None
    hvfhs_license_num: Optional[str] = None
    viajes: int
    espera_total_min: Optional[float] = None
    viajes_con_espera: Optional[int] = None
    espera_promedio_min: Optional[float] = None
    year: int
    month: int


class FinancialPerformanceRow(BaseModel):
    service_id: str
    fecha_viaje: date
    bloque_horario: str
    pu_location_id: Optional[int] = None
    pu_borough: Optional[str] = None
    pu_zone: Optional[str] = None
    viajes: int
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    cbd_congestion_fee: Optional[float] = None
    airport_fee: Optional[float] = None
    ehail_fee: Optional[float] = None
    total_amount: Optional[float] = None
    base_passenger_fare: Optional[float] = None
    tolls: Optional[float] = None
    bcf: Optional[float] = None
    sales_tax: Optional[float] = None
    tips: Optional[float] = None
    driver_pay: Optional[float] = None
    trip_distance: Optional[float] = None
    trip_miles: Optional[float] = None
    ingreso_bruto_por_milla: Optional[float] = None
    margen_plataforma: Optional[float] = None
    ratio_pago_conductor: Optional[float] = None
    year: int
    month: int


class OperationalProfileRow(BaseModel):
    service_id: str
    fecha_viaje: date
    bloque_horario: str
    pu_location_id: Optional[int] = None
    pu_borough: Optional[str] = None
    pu_zone: Optional[str] = None
    viajes: int
    duracion_total_min: float
    duracion_promedio_min: float
    distancia_total_millas: float
    distancia_promedio_millas: float
    velocidad_promedio_mph: Optional[float] = None
    viajes_solicitud_compartida: Optional[int] = None
    viajes_match_compartido: Optional[int] = None
    tasa_ocupacion_compartida: Optional[float] = None
    year: int
    month: int


class SupplyDemandBalanceRow(BaseModel):
    location_id: int
    borough: Optional[str] = None
    zone: Optional[str] = None
    bloque_temporal_t: datetime
    bloque_temporal_t_plus_1: Optional[datetime] = None
    taxis_entrantes_zona_t: int
    taxis_salientes_zona_t_plus_1: int
    flujo_neto_oferta: int
    deficit_severo_flag: bool
    year: int
    month: int


class AbcXyzZonesRow(BaseModel):
    pu_location_id: int
    borough: Optional[str] = None
    zone: Optional[str] = None
    service_id: str
    year: int
    ingresos_totales_zona: float
    viajes_diarios_promedio: float
    viajes_diarios_std: float
    coeficiente_variacion_xyz: Optional[float] = None
    clase_xyz: Optional[str] = None
    porcentaje_acumulado_ingresos: Optional[float] = None
    clase_abc: Optional[str] = None


class TippingBehaviorRow(BaseModel):
    service_id: str
    fecha_viaje: date
    pu_borough: Optional[str] = None
    do_borough: Optional[str] = None
    payment_type_id: Optional[int] = None
    is_credit_card: Optional[bool] = None
    categoria_generosidad: Optional[str] = None
    viajes: int
    viajes_con_propina: int
    propina_total: float
    porcentaje_propina_promedio: float
    porcentaje_propina_ponderado: float
    propina_por_milla: Optional[float] = None
    year: int
    month: int
