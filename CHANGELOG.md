# Changelog

Todos los cambios notables del pipeline ETL de NY TLC se documentan en este archivo.
Formato orientado al usuario (equipos de analítica / BI / ML).

---

## 2026-07-12 - v0.5.1

Reducido el ancho del modelo estrella (silver) y los feature stores gold eliminando columnas no utilizadas aguas abajo. Agregadas funciones Python en las reglas de características para inferencia fuera de Spark, configuración de serving/speed, y documentación CLI.

### Added
- Python-accessible helper functions (`*_py`) en `time_blocks.py`, `generosity.py`, `passenger_groups.py`, `ratecode_tariff.py` — para uso en inferencia/serving fuera de Spark (FastAPI)
- `SpeedConfig` y `ServingConfig` Pydantic schemas en `settings_schema.py` con secciones `speed:` y `serving:` en `config.yaml`
- `serving` optional dependency extra en `pyproject.toml` (`fastapi`, `uvicorn`, `redis`, `xxhash`)
- `docs/CLI.md` — documentación de referencia completa del CLI

### Changed
- **Star schema**: eliminadas columnas redundantes de facts (`store_and_fwd_flag`, `dispatching_base_num`, `originating_base_num`, `trip_time`, `trip_type`, `SR_Flag`, `shared_match_flag`, `access_a_ride_flag`, `wav_request_flag`, `wav_match_flag`)
- **Gold dimensions**: selects acotados en `dim_date_gold` y `dim_zone_gold`; agregada columna `is_weekend`
- **ARIMA features**: eliminadas `hour_of_day`, `dow`, `is_weekend`, `is_holiday` del store (ningún consumidor las lee)
- **Isolation fraud features**: reducidas columnas (`date_key`, `ratecode_name`, pickup/dropoff timestamps, `tolls_amount`, `improvement_surcharge`, `desviacion_tarifa_teorica`)
- **KModes features**: eliminadas `date_key`, `pu_location_id`, `do_location_id`, `vendor_id`

### Removed
- `dim_vendor` y `dim_service` del star schema (no utilizadas por gold ni los dashboards)

## 2026-07-12 - 09d8c6d

Silver cleaner refactored to reject-only (no imputation, no clamping, no recomputation). Gold implementation moved to `gold_impl/` package. Settings refactored to singleton. New config reference docs and optional dependency extras.

### Added
- `docs/config_reference.md` — comprehensive reference for `config.yaml`, `.env`, profiling rules, gold feature rules, and Spark/Airflow configuration
- Optional dependency extras in `pyproject.toml`: `s3` (s3fs for S3 storage backend) and `jupyter` (jupyterlab for docker-compose)
- Storage abstraction utilities (`app.utils.storage`) for unified local/S3 path handling
- New test cases verifying reject-only behavior: null required columns reject, nullable columns pass through, no imputation, no clamping, no total_amount recomputation

### Changed
- **Silver cleaner refactored to reject-only**: removed all fix/imputation phases — no clamping of `trip_distance`/`passenger_count`, no `total_amount` recomputation, no default imputation of nulls. Silver now only rejects invalid rows; source values pass through unchanged.
- **Expanded `NULLABLE_COLUMNS`** across all categories: yellow/green now include `RatecodeID`, `store_and_fwd_flag`, `payment_type`, `tip_amount`, `tolls_amount`, `extra`, `mta_tax`, `improvement_surcharge`; fhv adds `Affiliated_base_number`; fhvhv adds `request_datetime`, `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `tips`
- **Gold implementation moved** from `app/pipeline/gold/` to `app/pipeline/gold_impl/`
- **Star schema builder moved** from `app/pipeline/star.py` to `app/pipeline/silver_impl/star.py`
- **Settings refactored** to singleton pattern (`settings` instance, removed `.config` accessor)
- **`main.py` simplified** — removed unused imports, uses storage abstraction
- **Tests updated** — silver cleaner tests verify reject-only behavior (no imputation, no clamping, no recomputation); NULLABLE_COLUMNS tests expanded for all categories

### Removed
- Fix/imputation phase from SilverCleaner: no more clamping of `trip_distance`/`passenger_count`, no `total_amount` recomputation, no default imputation of nulls

### Fixed
- **`pyproject.toml`** — added missing optional dependency extras (`s3`, `jupyter`) for S3 storage backend and Jupyter service

## [0.4.1] — 2026-07-10 — Resiliencia OOM y dependencias en Docker

### 🐛 Correcciones
- **Prevención de OutOfMemory (OOM) en Spark**: El procesamiento paralelo estático (`MAX_PARALLEL_*`) provocaba caídas completas de la JVM al procesar meses masivos como `fhvhv` y `yellow` simultáneamente. Se implementó **concurrencia dinámica** en `SilverPipeline`, `ProfilingPipeline` y `StarSchemaBuilder`: los datasets pesados se procesan secuencialmente (1 worker) para proteger la RAM de 6GB, mientras que los livianos (`green`, `fhv`) se solapan en paralelo (2-3 workers).
- **Corrupción de caché Ivy eliminada**: Las dependencias Maven (`hadoop-aws`, `aws-java-sdk-bundle`) ahora se **pre-descargan** en la imagen de Docker durante el build mediante una sesión fantasma de PySpark (`Dockerfile`). Los DAGs en runtime ya no dependen de internet, eliminando fallos de red por descargas concurrentes (`[Errno 111] Connection refused`).

### 🔧 Mejoras
- **Logs de Airflow limpios**: Se deshabilitó la barra de progreso por consola nativa de PySpark (`spark.ui.showConsoleProgress = false`) para evitar ruido innecesario de los *stages* en la salida estándar, manteniendo únicamente el sistema de logging propio.

---

## [0.4.0] — 2026-07-02 — Histórico multi-año, modelos ML integrados y pipeline reanudable

El pipeline pasa de procesar un año a **todo el histórico 2023–2025** (~940M de viajes).
Para hacerlo viable en una máquina local se rediseñó el grano de los marts, se paralelizó
cada capa y toda la cadena se volvió **idempotente**: una corrida interrumpida se reanuda
donde quedó.

### ✨ Nuevas funcionalidades

- **Cobertura multi-año 2023–2025**: `config.yaml` ahora procesa los tres años completos
  (4 categorías × 12 meses × 3 años). `dim_date` y los feriados de `dim_date_gold` ya
  cubrían el rango.
- **Entrenamiento de modelos integrado al pipeline** con el nuevo subcomando `--gold-ml`
  (antes el entrenamiento quedaba fuera del pipeline):
  ```bash
  uv run main.py --gold-ml kmodes     # clustering de perfiles de viaje (codo + silhouette)
  uv run main.py --gold-ml isolation  # detección de fraude por RatecodeID (scores por viaje)
  uv run main.py --gold-ml sarimax    # pronóstico de viajes por borough × servicio
  ```
  Salidas: labels/scores/predicciones en `data/gold/ml/` y modelos serializados
  (`joblib`) en `data/gold/models/`. Nuevas dependencias: `scikit-learn`, `kmodes`,
  `statsmodels`, `joblib`.
- **Marts de Power BI rediseñados a grano agregado**: los 4 marts que emitían 1 fila por
  viaje (volumen, financiero, operativo, propinas) ahora agregan por
  fecha × hora/bloque × zona/borough (+ dimensiones propias de cada mart). Los dashboards
  consumen conteos, sumas y promedios — replicar ~940M de filas era inviable para Power BI.
  Los componentes de tarifa se guardan como **sumas re-agregables** y los ratios se derivan
  de las sumas (p. ej. velocidad ponderada, no promedio de promedios). El detalle viaje a
  viaje permanece íntegro en `silver/star/facts`.
- **Pipeline idempotente y reanudable** en todas las capas: bronze omite descargas cuyo
  parquet ya es legible (un archivo truncado se re-descarga), profiling reutiliza los JSON
  existentes, silver omite meses ya limpios en `stage/`, el modelo estrella omite facts ya
  construidos y gold ya contaba con modo `incremental`. Para forzar un reproceso basta
  borrar la salida correspondiente.
- **Notebooks de revisión por capa** (`notebooks/revision_01..05`): inventario del bronce,
  resultados del profiling, integridad silver (`bronce = stage + reject`, ninguna fila se
  pierde), granos y prueba de no-pérdida de gold (`SUM(viajes)` == filas de los facts) y
  cadena de auditoría completa.
- **Suite de tests con `pytest`** (unit / spark / integration): reglas de calidad,
  limpieza silver, modelo estrella, dimensiones y marts gold, entrenamiento de los 3
  modelos sobre feature stores sintéticos, y un e2e de la cadena completa con verificación
  de la trazabilidad de auditoría.

### ⚡ Rendimiento

- **Procesamiento paralelo por capa** (aprovecha los task slots ociosos de Spark local):
  profiling 3 archivos a la vez, silver 2, facts del modelo estrella 3. Las escrituras de
  auditoría quedan serializadas con un lock para no perder filas.
- **Spark afinado para la corrida completa**: `local[6]` (antes 4), 128 particiones de
  shuffle (antes 64) y **AQE habilitado** (coalescencia dinámica de particiones y joins
  con skew) — mismo plan lógico, menos tiempo muerto.
- **Compresión zstd calibrada a nivel 9**: medido sobre un mes real de yellow (3.4M filas),
  el nivel 19 escribía 6× más lento para ganar solo 2.5% de disco (38.6s/51.9MB vs
  6.2s/53.2MB). Con ~60 escrituras de hasta 20M de filas, el nivel 19 dominaba el tiempo
  del pipeline.
- **Menos archivos, mejor compresión**: las escrituras se coalescean a ~2M de filas por
  archivo (`target_files`), eliminando las decenas de parquet diminutos que dejaba el
  shuffle (overhead de footers y diccionarios no compartidos).
- **Profiling ~8× menos I/O por archivo**: el dataset se cachea una sola vez por perfil;
  antes cada una de las 8 dimensiones relanzaba la lectura del parquet desde disco.
- **Lectura única compartida en gold**: los 3 builders agregados (oferta/demanda, ABC/XYZ,
  ARIMA) derivan de una **unión lazy común** de los facts (`get_union_facts`) en lugar de
  leer y unir los ~144 archivos mensuales cada uno por su cuenta. La unión no se
  materializa: a escala completa el persist reventaba el heap; el escaneo lazy con
  proyección estrecha procesa en streaming con memoria acotada.
- **Feature store K-Modes dimensionado al consumo**: fhvhv se muestrea al 5% (uniforme,
  semilla fija) — el modelo entrena con ≤100k filas por servicio y emitir 240M de viajes
  era trabajo sin consumidor. Yellow/green se emiten completos; silver conserva el 100%.

### 🐛 Correcciones

- **Los fallos parciales ya no pasan desapercibidos**: silver, la carga de facts y gold
  acumulan los errores por archivo/builder y **abortan al final con la lista de fallos**.
  Antes un builder caído (p. ej. por muerte de la JVM) dejaba una capa incompleta
  reportada como exitosa — un mes ausente era pérdida de datos silenciosa río abajo.
- **Los tests de Spark ya corren en Windows**: el fixture fija `PYSPARK_PYTHON` al
  intérprete actual; sin esto los workers caían en el stub de Python de Microsoft Store y
  la suite colgaba. También se alineó el año de muestreo del bronce (2025) con los datos
  reales en disco.
- **El profiling paralelo no se auto-sabotea**: se eliminó el `clearCache()` global entre
  perfiles, que despersistía los DataFrames de los perfiles aún en curso.

### ⚠️ Migración

- **Esquema de marts incompatible**: los 4 marts re-granulados cambian de esquema (ya no
  llevan `trip_id` ni columnas por viaje; aparecen `viajes` y agregados `*_total`/
  `*_promedio`). Reconstruir con `uv run main.py --gold` y reapuntar los dashboards de
  Power BI al nuevo grano.
- **Primer arranque multi-año**: con `years: [2023, 2024, 2025]`, la primera corrida
  descarga y procesa ~3× más datos. Gracias a la idempotencia, los meses de 2025 ya
  materializados no se recalculan.

---

## [0.3.0] — 2026-07-01 — Congestion pricing 2025 + optimización de almacenamiento

### ✨ Nuevas funcionalidades

- **Soporte de `cbd_congestion_fee`** (peaje de la *Congestion Relief Zone* de la MTA
  que la TLC añadió a los datasets Yellow, Green y HVFHV a partir de 2025). Es un
  componente del cobro al pasajero y se integra en todas las capas:
  - **Silver (calidad)**: entra en la recomposición de `total_amount` de yellow/green
    (`amount_components.py`), se imputa a 0.0 cuando es nulo (viajes fuera de la zona) y
    tiene rango de razonabilidad `(0, 5)`. También reconocido por el *profiling*
    (completitud, exactitud, razonabilidad) al compartir las mismas reglas.
  - **Silver (estrella)**: los facts `fact_{yellow,green,fhvhv}_trip` ahora arrastran
    `cbd_congestion_fee`.
  - **Gold**: `mart_financial_performance` expone la columna junto al resto de recargos.

### 🐛 Correcciones

- **`total_amount` ya no se subestima en datos de 2025**: la corrección de exactitud de
  silver recomputaba `total_amount` como suma de componentes, pero la lista **no incluía**
  `cbd_congestion_fee`, restando ~0.75 USD a cada viaje de yellow/green en la zona de
  congestión (y disparando falsos *mismatch* de exactitud en el profiling). Con `config.yaml`
  en `years: [2025]`, afectaba a **todo** el dataset procesado.

### ⚡ Rendimiento / almacenamiento

- **Facts ~68% más pequeños** (medido sobre fact fhvhv sintético de 300k filas):
  - **`trip_id` de sha2-256 hex (string 64) → `xxhash64` (BIGINT 8 bytes)**. El hash hex de
    alta entropía no se codificaba por diccionario ni comprimía, y ocupaba **~67%** del fact
    (además de propagarse a casi todos los marts). El BIGINT es ~8x menor, mejor clave de
    relación en Power BI y determinista. La unicidad estricta ya la garantiza silver vía
    `COMPOSITE_KEYS`.
  - **Codec Parquet `zstd`** (antes snappy por defecto) en todas las escrituras: ~30-40%
    menos disco con CPU despreciable.
  - **Fuga de columnas helper corregida**: `_pickup_dt`/`_dropoff_dt` (de la fase de rechazo)
    se filtraban al stage silver; ahora se eliminan.
- **`OUTPUT_SCHEMA` de Isolation Forest** actualizado (`trip_id` `StringType` → `LongType`)
  para mantener la consistencia de tipo con el nuevo `trip_id` BIGINT (evita que los *scores*
  de fraude salieran con `trip_id` string, incompatible en joins).

### ⚠️ Migración

- El cambio de tipo de `trip_id` (string → BIGINT) hace **incompatibles** los datos ya
  materializados en `data/silver/star/` y `data/gold/`. Requiere **reconstruir** desde
  `--silver load` en adelante (o `--silver schema`/`load` + `--gold`). Con `data/` vacío no
  hay acción necesaria.

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
