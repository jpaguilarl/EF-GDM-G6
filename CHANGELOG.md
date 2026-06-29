# Changelog

Todos los cambios notables del pipeline ETL de NY TLC se documentan en este archivo.
Formato orientado al usuario (equipos de analítica / BI / ML).

---

## [0.2.0] — 2026-06-27 — Capa de Oro (Gold)

Nueva capa **gold** del pipeline medallón (Bronze → Silver → **Gold**) que alimenta los
9 dashboards de Power BI y los modelos de IA descritos en
`especificaciones_dashboards_nyc_tlc.md`.

### ✨ Nuevas funcionalidades

- **Capa gold completa** con nuevo subcomando:
  ```bash
  uv run main.py --gold                 # full (default)
  uv run main.py --gold incremental     # solo particiones nuevas
  uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud
  ```
- **6 marts para Power BI** (tablas anchas denormalizadas) en `data/gold/marts/`:
  - `mart_demand_volume` — volumen y demanda (bloque horario, día de semana, tiempo de
    espera HVFHV).
  - `mart_financial_performance` — ingresos, margen de plataforma, ratio de pago al
    conductor, ingreso por milla.
  - `mart_operational_profile` — duración, velocidad promedio, viajes compartidos.
  - `mart_supply_demand_balance` — flujo neto oferta/demanda por zona y bloque temporal
    (15/30 min configurable) con bandera de déficit severo.
  - `mart_abc_xyz_zones` — clasificación ABC (Pareto de ingresos) y XYZ (coeficiente de
    variación) de zonas de origen.
  - `mart_tipping_behavior` — comportamiento de propinas con categoría de generosidad
    (filtrando efectivo en taxis).
- **3 feature stores para IA** en `data/gold/ml/`:
  - `ml_feat_arima_trips` — serie temporal de viajes por borough y hora (con variables
    exógenas: feriado, fin de semana, hora).
  - `ml_feat_kmodes_trips` — variables categóricas nominales por viaje para clustering.
  - `ml_feat_isolation_fraud` — features de detección de fraude en taxímetros (yellow/green)
    con desviación de tarifa teórica y candidato a anomalía.
- **3 dimensiones gold enriquecidas** en `data/gold/dims/`: `dim_date_gold` (categoría de
  día + feriados NYC 2023-2025), `dim_zone_gold` (nombres de borough en español),
  `dim_ratecode_theoretical` (tarifa teórica por RatecodeID y año fiscal).
- **Configuración parametrizable** vía sección `gold:` en `config.yaml` (tamaño de bloque
  temporal, umbral de déficit, cortes ABC/XYZ, umbrales de generosidad).
- **Auditoría gold** en `data/gold/audit.parquet` con trazabilidad `silver_audit_id` →
  `gold_audit_id`, modo, conteos y snapshot de configuración.

### 🔧 Mejoras

- **Facts del modelo estrella enriquecidos** (`silver/star`): ahora conservan
  `pickup_datetime`, `dropoff_datetime` (timestamps estandarizados entre categorías) y un
  `trip_id` (hash de la PK compuesta). Antes solo guardaban la fecha a nivel día, lo que
  impedía cualquier análisis por hora.
- **Heurísticas de dominio centralizadas y testeables** en `app/pipeline/gold/feature_rules/`
  (bloques horarios, generosidad de propina, tarifas teóricas) — única fuente de verdad
  compartida por marts y features ML.
- **Escritura idempotente** por partición (`partitionOverwriteMode=dynamic`) y persistencia
  de agregaciones intermedias para datasets grandes (HVFHV).

### 🐛 Correcciones

- **`driver_pay` de HVFHV ya no se corrompe**: la corrección de calidad de silver
  sobrescribía `driver_pay` con la suma de componentes (≈ costo al pasajero), arruinando
  `margen_plataforma` y `ratio_pago_conductor`. Ahora se preserva el valor original.
- **Día de la semana correcto**: se reemplazó `date_format(ts, "u")` —que en el calendario
  proléptico de Spark 3+/4 no representa el día de semana— por un cálculo ISO robusto
  (`dayofweek` reindexado), evitando valores silenciosamente incorrectos en `dia_semana`,
  `is_weekend`, `dia_categoria` y `franja_horaria`.

### 📝 Notas

- La capa gold lee de `data/silver/star/`; requiere ejecutar antes
  `bronze → --silver → --silver schema → --silver load`. Si silver no está cargado, el
  pipeline aborta con un mensaje claro.
- La tarifa plana JFK (70 USD) en `dim_ratecode_theoretical` debe verificarse contra la
  normativa TLC vigente (solo afecta la heurística `is_anomaly_candidate`).
