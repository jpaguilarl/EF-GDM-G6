# Plan de Implementación — Capa de Oro (Gold)

> Objetivo: construir la capa gold del pipeline medallón (Bronze → Silver → **Gold**)
> para alimentar los **dashboards (Power BI)** y los **modelos de IA** descritos en
> `especificaciones_dashboards_nyc_tlc.md`.
>
> Estado: **plan (no ejecutar)**.

---

## 0. Decisiones de diseño acordadas

| Eje | Decisión |
|---|---|
| Motor de procesamiento | **PySpark** (consistencia con silver/star) |
| Formato físico | **Parquet** particionado bajo `data/gold/` |
| Modelado BI | **Tablas anchas denormalizadas** ( wide marts) optimizadas para Power BI |
| Feature store ML | **Sí**: tablas específicas para ARIMA, K-Modes, Isolation Forest |
| Modo de ejecución | **Ambos**: `--mode full` (rebuild) y `--mode incremental` |
| Auditoría | Hereda patrón `audit_id` del silver → nuevo `data/gold/audit.parquet` |
| Origen de datos gold | `data/silver/star/facts/*` + `data/silver/star/dims/*` (no desde stage) |

---

## 1. Alcance funcional

La capa de oro debe satisfacer los **9 productos analíticos** del documento de
especificaciones, agrupados en 3 categorías:

### 1.1 Dashboards descriptivos
- **D1.1 Volumen y Demanda** → `mart_demand_volume`
- **D1.2 Rendimiento Financiero** → `mart_financial_performance`
- **D1.3 Perfil Operativo** → `mart_operational_profile`

### 1.2 Dashboards diagnósticos
- **D2.1 Desequilibrio Oferta-Demanda** → `mart_supply_demand_balance`
- **D2.2 Análisis ABC/XYZ** → `mart_abc_xyz_zones`
- **D2.3 Comportamiento de Propinas** → `mart_tipping_behavior`

### 1.3 Dashboards predictivos / avanzados
- **D3.1 Predicción ARIMA** → `ml_feat_arima_trips` (serie univariada por Borough)
- **D3.2 Clustering K-Modes** → `ml_feat_kmodes_trips` (cualitativo, viaje-a-viaje)
- **D3.3 Detección de Anomalías/Fraude** → `ml_feat_isolation_fraud` (taxímetros)

---

## 2. Arquitectura de la capa gold

```
data/gold/
├── marts/                       # Dashboards (Power BI - tablas anchas)
│   ├── mart_demand_volume/          particionado por service_id, year, month
│   ├── mart_financial_performance/  particionado por service_id, year, month
│   ├── mart_operational_profile/   particionado por service_id, year, month
│   ├── mart_supply_demand_balance/ particionado por year, month
│   ├── mart_abc_xyz_zones/          particionado por service_id, year
│   └── mart_tipping_behavior/       particionado por service_id, year, month
├── ml/                          # Feature store para modelos de IA
│   ├── ml_feat_arima_trips/         particionado por borough, year, month
│   ├── ml_feat_kmodes_trips/        particionado por service_id, year, month
│   └── ml_feat_isolation_fraud/    particionado por year, month
├── dims/                        # Dimensiones gold (denormalizadas y enriquecidas)
│   ├── dim_date_gold.parquet        (con bloque_horario, dia_categoria, franja_horaria)
│   ├── dim_zone_gold.parquet        (con borough, zone, service_zone)
│   └── dim_ratecode_theoretical.parquet  (tarifa teórica por RatecodeID y año fiscal)
└── audit.parquet                # Trazabilidad silver_audit_id → gold_audit_id
```

### Particionamiento (estándar)
- Tablas de hechos/viaje: **`year`, `month`** (cardinalidad baja, óptima para Power BI).
- Series temporales ML (ARIMA): **`borough`, `year`, `month`** para ventana de
  entrenamiento por macro-región.
- Marts agregados (ABC/XYZ, diario/horario): **`year[, month]`** según granularidad.

### Esquema de auditoría gold
Extiende el patrón silver. Cada mart/feature table escribe una fila en
`data/gold/audit.parquet` con:

```
gold_audit_id (uuid), silver_audit_id (FK), mart_name, mode (full|incremental),
start_timestamp, end_timestamp, source_files[], rowcount_output,
partition_keys{}, config_snapshot_md5
```

---

## 3. Modelo de datos por producto

### Marts de dashboards (Power BI — tablas anchas)

#### `mart_demand_volume` (D1.1)
Granularidad: 1 fila por viaje (para drill-through) + agregados derivados vía medidas DAX.

Columnas:
- `trip_id` (hash de la PK compuesta silver), `service_id`
- `pickup_datetime`, `dropoff_datetime`, `date_key`, `year`, `month`
- `fecha_viaje` (date), `bloque_horario` (categoría: Madrugada|Punta Mañana|Mediodía|Punta Tarde|Noche), `dia_semana` (1-7), `is_weekend`
- `pu_location_id`, `do_location_id`, `pu_borough`, `do_borough`, `pu_zone`, `do_zone`
- `hvfhs_license_num` (nullable)
- Métricas: `trip_count=1`
- Features exclusivas HVFHV: `tiempo_espera_minutos = (on_scene - request)/60`

#### `mart_financial_performance` (D1.2)
Granularidad: 1 fila por viaje.

Columnas:
- `trip_id`, `service_id`, `date_key`, `year`, `month`, `pickup_datetime`
- Componentes tarifa (yellow/green): `fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, cbd_congestion_fee, airport_fee, total_amount, ehail_fee` (`cbd_congestion_fee` solo 2025+)
- Componentes HVFHV: `base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge, cbd_congestion_fee, airport_fee, tips, driver_pay` (`cbd_congestion_fee` solo 2025+)
- `trip_distance` (taxis) / `trip_miles` (fhvhv)
- Computadas:
  - `margen_plataforma = base_passenger_fare - driver_pay` (fhvhv)
  - `ingreso_bruto_por_milla = total_amount / trip_distance` (taxis) ó `base_passenger_fare / trip_miles` (fhvhv)
  - `ratio_pago_conductor = driver_pay / base_passenger_fare` (fhvhv)

#### `mart_operational_profile` (D1.3)
Granularidad: 1 fila por viaje, con filtro de validez para velocidad.

Columnas:
- `trip_id`, `service_id`, `date_key`, `pu/do_location_id`
- `pickup_datetime`, `dropoff_datetime`
- `duracion_viaje_minutos` (de silver star fact)
- `trip_distance`/`trip_miles`
- `velocidad_promedio_mph = trip_distance / (duracion_viaje_minutos/60)` (null si duración<=0 o distancia==0)
- `shared_request_flag`, `shared_match_flag` (fhvhv)
- `is_shared_match` (bool, fhvhv) — alimentará `tasa_ocupacion_compartida` como medida agregada

#### `mart_supply_demand_balance` (D2.1)
Granularidad: 1 fila por `zona × bloque_temporal_t` (parametrizable 15/30 min).

Columnas:
- `location_id`, `borough`, `zone`
- `bloque_temporal_t` (timestamp truncado a 15 min por defecto, configurable)
- `bloque_temporal_t_plus_1` (t+1)
- `taxis_entrantes_zona_t = COUNT(DOLocationID==z en t)`
- `taxis_salientes_zona_t_plus_1 = COUNT(PULocationID==z en t+1)`
- `flujo_neto_oferta = entrantes_t - salientes_t_plus_1`
- `deficit_severo_flag = flujo_neto < -threshold` (umbral configurable)

#### `mart_abc_xyz_zones` (D2.2)
Granularidad: 1 fila por `PULocationID × periodo histórico (año)`.

Columnas:
- `pu_location_id`, `borough`, `zone`, `service_id`, `year`
- `ingresos_totales_zona` (suma total_amount/base_passenger_fare)
- `viajes_diarios_promedio`, `viajes_diarios_std`
- `coeficiente_variacion_xyz = std / promedio`
- `clase_xyz` (X<0.2, Y 0.2-0.5, Z>0.5)
- `porcentaje_acumulado_ingresos`, `clase_abc` (A 80%, B 15%, C 5%)

#### `mart_tipping_behavior` (D2.3)
Granularidad: 1 fila por viaje, con filtro `payment_type=1` en taxis (bandera `is_credit_card`).

Columnas:
- `trip_id`, `service_id`, `date_key`, `pu_borough`, `do_borough`
- `payment_type_id`, `is_credit_card` (bool)
- `fare_amount`, `base_passenger_fare`, `tip_amount`/`tips`
- `trip_miles`/`trip_distance`
- `porcentaje_propina` (filtrado a tarjetas en taxis)
- `propina_por_milla`
- `categoria_generosidad` (Sin Propina|Baja|Estándar|Alta)

### Feature store ML

#### `ml_feat_arima_trips` (D3.1)
Granularidad: serie univariada `borough × hour` (resample count).

Columnas:
- `borough`, `pickup_hour` (timestamp truncado), `year`, `month`
- `trip_count` (agregación)
- `service_id` (segmentación de modelo)
- Columnas exógenas opcionales: `is_holiday`, `is_weekend`, `dow`, `hour_of_day`

#### `ml_feat_kmodes_trips` (D3.2)
Granularidad: 1 fila por viaje, sólo variables categóricas nominales.

Columnas:
- `trip_id`, `service_id`, `date_key`
- `PULocationID`, `DOLocationID`, `borough_pu`, `borough_do`
- `franja_horaria` (Mañana|Tarde|Noche|Madrugada)
- `dia_categoria` (Día Laborable|Fin de Semana)
- `hvfhs_license_num` (fhvhv) ó `vendor_id` (taxis)
- Excluye variables continuas (distancia, tarifa).

#### `ml_feat_isolation_fraud` (D3.3)
Granularidad: 1 fila por viaje (yellow/green únicamente — foco dataset).

Columnas:
- `trip_id`, `service_id`, `date_key`, `RatecodeID`, `Ratecode_name`
- `tpep/lpep_pickup_datetime`, `dropoff_datetime`
- `duracion_viaje_segundos`
- `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `improvement_surcharge`
- `velocidad_promedio_calculada`
- `costo_por_distancia = fare_amount / (trip_distance + 0.001)`
- `desviacion_tarifa_teorica` (vs `dim_ratecode_theoretical` por RatecodeID + año fiscal)
- `is_anomaly_candidate` (bool derivado de reglas heurísticas por RatecodeID — no es score final)

### Dimensiones gold

- **`dim_date_gold`**: extiende `dim_date` con `bloque_horario`, `dia_categoria`, `franja_horaria`, `is_holiday`. Rango 2023-2025 (igual a silver).
- **`dim_zone_gold`**: enriquece `dim_zone` con `borough`, `zone`, `service_zone` y `borough_name_es` (Power BI en español).
- **`dim_ratecode_theoretical`**: tarifa teórica por `RatecodeID` y `año_fiscal` (ej. flat JFK=70 USD para 2023-2024; actualizar por TLC rules).

---

## 4. Estructura de código propuesta

```
app/
└── pipeline/
    └── gold/
        ├── __init__.py
        ├── gold_pipeline.py          # Orquestador principal (GoldPipeline)
        ├── mart_builder.py           # Builder base + lógica común
        ├── marts/
        │   ├── __init__.py
        │   ├── demand_volume.py
        │   ├── financial_performance.py
        │   ├── operational_profile.py
        │   ├── supply_demand_balance.py
        │   ├── abc_xyz_zones.py
        │   └── tipping_behavior.py
        ├── ml/
        │   ├── __init__.py
        │   ├── arima_features.py
        │   ├── kmodes_features.py
        │   └── isolation_fraud_features.py
        ├── dims/
        │   ├── __init__.py
        │   └── gold_dimensions.py
        └── feature_rules/           # Heurísticas reusables
            ├── __init__.py
            ├── time_blocks.py        # bloque_horario, franja_horaria, dia_categoria
            ├── ratecode_tariff.py    # tarifas teóricas por RatecodeID + año fiscal
            └── generosity.py         # categoría de propina
```

### Clases clave

- **`GoldPipeline`** (orquestador): carga las facts/dims de silver, itera los builders,
  soporta `mode='full'|'incremental'`, escribe audit. Patrón análogo a `SilverPipeline`.
- **`MartBuilder`** (base abstracta): define `build(df, dims)` y `write(df, mode)` con
  particionado uniforme. Cada mart hereda y aplica su feature engineering con PySpark
  `functions`.
- **`FeatureStoreBuilder`** (base abstracta ML): igual interfaz pero orientado a
  features no agregadas.
- Reutiliza `SparkClient`, `Logger`, `globals`, `Settings` existentes.

### Integración con `main.py`

Nuevo subcomando siguiendo el patrón silver:

```bash
uv run main.py --gold                 # default: full
uv run main.py --gold full
uv run main.py --gold incremental
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud
```

`Settings` se enriquece si se necesita persistir parámetros (cadencia bloques de
15/30 min, umbrales ABC/XYZ, horizonte ARIMA) en `config.yaml`:

```yaml
gold:
  mode: full
  supply_demand:
    block_minutes: 15
    deficit_threshold: -10
  abc_xyz:
    class_a_pct: 0.80
    class_b_pct: 0.15
  generosity:
    standard_low: 10
    standard_high: 18
```

---

## 5. Plan de ejecución (fases)

### Fase 0 — Cimientos (1-2 h)
- [ ] Crear `app/pipeline/gold/` y subpaquetes (`marts/`, `ml/`, `dims/`, `feature_rules/`).
- [ ] Definir `GoldPipeline`, `MartBuilder` (base), `FeatureStoreBuilder` (base).
- [ ] Implementar escritura de `data/gold/audit.parquet` (FK `silver_audit_id`).
- [ ] Crear `dim_date_gold`, `dim_zone_gold`, `dim_ratecode_theoretical`.
- [ ] Añadir `feature_rules/time_blocks.py` (compartido por varios marts/ML).

### Fase 1 — Marts descriptivos (D1)
- [ ] `mart_demand_volume` (incluye `tiempo_espera_minutos` HVFHV).
- [ ] `mart_financial_performance` (margen_plataforma, ingreso_bruto_por_milla,
      ratio_pago_conductor).
- [ ] `mart_operational_profile` (duracion, velocidad con filtro de validez,
      flags shared).

### Fase 2 — Marts diagnósticos (D2)
- [ ] `mart_supply_demand_balance` (bloques de 15 min parametrizables, flujo neto).
- [ ] `mart_abc_xyz_zones` (Pareto + coef. variación, clases A/B/C y X/Y/Z).
- [ ] `mart_tipping_behavior` (filtro `payment_type==1`, `categoria_generosidad`).

### Fase 3 — Feature store ML (D3)
- [ ] `ml_feat_arima_trips` (serie por borough × hora).
- [ ] `ml_feat_kmodes_trips` (variables categóricas nominales únicamente).
- [ ] `ml_feat_isolation_fraud` (yellow/green, `desviacion_tarifa_teorica`,
      `is_anomaly_candidate`).

### Fase 4 — Orquestación y flags
- [ ] Conectar `GoldPipeline` en `main.py` con `--gold [full|incremental] [--only ...]`.
- [ ] Implementar detección de meses nuevos desde `silver/audit.parquet` para modo incremental.
- [ ] Configuración en `config.yaml` (sección `gold:`).

### Fase 5 — Verificación
- [ ] Smoke test: `uv run main.py --gold` sobre datos ya existentes en silver/star.
- [ ] Validar conteos (bronze → silver stage → silver facts → gold marts).
- [ ] Validar nulidad y tipos en columnas computadas (sin nulls en
      `bloque_horario`, `dia_semana`, etc.).
- [ ] Chequear que los marts cargan en Power BI Desktop sin transformaciones extra.

---

## 6. Reglas de calidad gold

1. **No enriquecimiento de dominio en marts**: las heurísticas (umbrales, bloques)
   viven en `feature_rules/` y son unit-testeables.
2. **Idempotencia**: `write.mode("overwrite")` por partición `year/month` (igual que silver).
3. **Trazabilidad**: cada mart registrado en `gold/audit.parquet` con `silver_audit_id`.
4. **Manejo de nulos explícito**: columnas computadas que puedan generar división por
   cero (`ingreso_bruto_por_milla`, `velocidad_promedio_mph`) usan guards
   (`when(cero, null)`).
5. **Consistencia multilenguaje**: log en español (igual a resto del proyecto).
6. **Sin dependencias externas nuevas**: sólo PySpark + librerías ya instaladas.

---

## 7. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Volumen de HVFHV (millones de filas/mes) | Particionado por `year, month`; `persist(MEMORY_AND_DISK)` en agregaciones intermedias; `spark.sql.shuffle.partitions=32` ya configurado |
| Difrn-75/30 min para supply/demand genera muchas filas | Implementar como agregado pre-computado en mart dedicado (no unirse a facts) |
| Tarifas teóricas Ratecode cambian por año fiscal TLC | Tabla `dim_ratecode_theoretical` versionada por `año_fiscal` |
| ARIMA/K-Modes requieren pandas/statsmodels scikit-learn en el futuro | El feature store gold sólo **entrega** features; el entrenamiento se hace fuera del pipeline (no rompe dependencias) |
| Modo incremental puede dejar marts inconsistentes si cambia silver | Bind `silver_audit_id`: si una partición silver cambia, el incremental detecta y reprocesa su gold |

---

## 8. Entregables finales

- [ ] Paquete `app/pipeline/gold/` funcional e integrado en `main.py`.
- [ ] 6 marts Power BI en `data/gold/marts/`.
- [ ] 3 tablas de features ML en `data/gold/ml/`.
- [ ] 3 dimensiones gold en `data/gold/dims/`.
- [ ] `data/gold/audit.parquet` con trazabilidad silver→gold.
- [ ] Sección `gold:` en `config.yaml` con parámetros parametrizables.
- [ ] Este plan actualizado al estado real de la implementación.

---

## 9. Dependencias con el estado actual

El plan asume **silver en estrella ya cargado**:
- `data/silver/star/dims/dim_*.parquet` (de `SilverPipeline.run_schema()`).
- `data/silver/star/facts/fact_{yellow,green,fhvhv,fhv}_trip/*.parquet`
  (de `SilverPipeline.run_load()`).
- `data/silver/audit.parquet` (para FK y detección incremental).

Si silver no está cargado, `GoldPipeline` debe abortar con mensaje claro en lugar de fallar.

---

## 10. Estado de implementación (2026-06-27)

**Plan aprobado e implementado** (Fases 0–5). Código escrito; **pendiente de ejecutar**
sobre datos reales (data/ está vacío: requiere bronze → silver quality → schema → load
antes de `--gold`).

### Cambios de cimientos acordados con el usuario (necesarios para que el plan funcione)

1. **Enriquecimiento de los facts silver** (`app/pipeline/star.py`): los facts ahora
   conservan `pickup_datetime`, `dropoff_datetime` (timestamps estandarizados) y
   `trip_id` (sha2 de la PK compuesta de silver). Sin esto, los facts solo tenían
   `date_key` a nivel día y era **imposible** computar `bloque_horario`, `franja_horaria`,
   bloques de 15 min, hora ARIMA ni `trip_id`. → Decisión: *"Enriquecer facts"*.

2. **Corrección de `driver_pay` (fhvhv)** (`app/pipeline/silver.py`): `_fix_accuracy` ya
   **no** sobrescribe `driver_pay` en fhvhv (no existe un total real cobrado al pasajero).
   Mantiene el valor original → `margen_plataforma` y `ratio_pago_conductor` (D1.2) ya son
   correctos. → Decisión: *"Corregir silver"*.

### Hallazgo corregido durante la verificación (skill systematic-debugging)

- **`dia_semana` / `is_weekend`**: se usaba `date_format(ts, "u")`, pero en el calendario
  proléptico de Spark 3+/4 el patrón `'u'` NO es día-de-semana. Se centralizó en
  `feature_rules/time_blocks.iso_weekday()` (vía `dayofweek` reindexado a ISO 1=Lun..7=Dom),
  consistente con `dim_date.weekday` de silver.

### Decisiones menores

- `mart_financial_performance`, `mart_operational_profile`, `mart_tipping_behavior` y
  `ml_feat_kmodes_trips` excluyen `fhv` (`applies_to`), que carece de tarifa/distancia.
- `is_holiday` (dim_date_gold): set hardcodeado de feriados federales NYC 2023-2025 (sin
  dependencias nuevas).
- `dim_ratecode_theoretical`: JFK (RatecodeID=2) tarifa plana 70 USD 2023-2025 — **verificar
  contra normativa TLC vigente**.
- Modo `incremental`: marts a nivel viaje omiten particiones existentes; marts agregados
  (supply/demand, ABC/XYZ, ARIMA) siempre se recomputan (abarcan todo el histórico).

### Verificación estática realizada (sin ejecutar el pipeline)

- `compileall` OK sobre todo `app/` + `main.py`.
- Grafo de imports completo resuelto (sin import circular star↔silver; 9 builders
  registrados con sus nombres).
- `config.yaml` + `GoldConfig` parsean; todas las funciones `pyspark.sql.functions`
  usadas existen.

### Cómo ejecutar (cuando silver esté cargado)

```bash
uv run main.py --gold                                   # full (default)
uv run main.py --gold incremental
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud
```
