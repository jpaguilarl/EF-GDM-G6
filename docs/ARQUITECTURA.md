# Arquitectura del Pipeline

## Visión general

Este proyecto implementa una **arquitectura Lambda** para el procesamiento de datos
de viajes del programa TLC de Nueva York (yellow, green, fhv, fhvhv). Combina un
pipeline batch que procesa el histórico completo con un pipeline en tiempo real que
absorbe viajes entrantes, unificando ambos en una capa de servicio que ofrece
consultas tanto históricas como en vivo.

Los datos siguen un modelo **medallion** (bronce → plata → oro) dentro del batch
layer, con una etapa independiente de profiling de calidad. El stack tecnológico
se divide según la naturaleza de cada fase:

| Capa | Tecnología | Propósito |
|---|---|---|
| Descargas + auditoría | Polars | Lectura/escritura ligera de Parquet, async-friendly |
| Batch (profiling, silver, star, gold) | PySpark | Procesamiento distribuido de ~940M filas |
| ML models | pandas + scikit-learn / kmodes / statsmodels | Algoritmos de ML no disponibles en Spark nativo |
| Speed layer | Python + Redis | Agregación en memoria, baja latencia |
| Serving layer | FastAPI + Polars (lazy scan) + SSE | Consultas históricas y streaming, sin Spark |

---

## Arquitectura Lambda

```
┌─────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                            │
│  FastAPI                                                       │
│  ┌─────────────────────┐  ┌──────────────────────────────────┐ │
│  │  Historic Routes     │  │  Realtime Routes                 │ │
│  │  GET /historic/{mart}│  │  GET /realtime/{view}[/stream]   │ │
│  │  Polars scan_parquet │  │  MergedViewReader               │ │
│  │  (lazy, predicados)  │  │  (batch + Redis fusionado)      │ │
│  └──────────┬──────────┘  └──────────────┬───────────────────┘ │
│             │                             │                     │
└─────────────┼─────────────────────────────┼─────────────────────┘
              │                             │
              ▼                             ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│    BATCH LAYER          │    │     SPEED LAYER          │
│  (PySpark, procesamiento│    │  (Python + Redis,        │
│   completo del histórico│    │   procesamiento en       │
│   ~940M filas)          │    │   tiempo real)           │
│                         │    │                          │
│  Medallion architecture │    │  POST /api/v1/ingest     │
│                         │    │  → EventProcessor        │
│  Bronce → Silver → Gold │    │  → RealtimeAggregator    │
│                         │    │  → FraudScorer           │
│  Salida:                │    │  → TripProfiler          │
│  data/gold/marts/*      │    │                          │
│  data/gold/ml/*         │    │  Salida:                 │
│                         │    │  Redis keys rt:* (TTL 48h)│
└─────────────────────────┘    └─────────────────────────┘
         │                              │
         └──────────┬───────────────────┘
                    ▼
       ┌─────────────────────────┐
       │      MERGE BOUNDARY     │
       │  MergedViewReader       │
       │  batch gold (completo)  │
       │  + Redis (bloque actual)│
       │  → deduplicación por    │
       │    tupla de clave        │
       │  → batch gana en solape  │
       └─────────────────────────┘
```

### Batch layer (ruta lenta)

El batch layer procesa la totalidad del histórico mediante un pipeline
medallion de tres niveles:

```
Bronce ──► Silver calidad ──► Silver esquema ──► Silver carga ──► Oro ──► Oro ML
 (crudo)    (stage + reject)   (dimensiones      (hechos           (marts +   (modelos:
                             estrella)         estrella)        feature     KModes,
                                                                stores)     IF, SARIMAX)
```

#### Bronce

`BronzePipeline` descarga archivos Parquet desde el repositorio público de la TLC
(`https://d37ci6vzurychx.cloudfront.net/trip-data/`) mediante `DownloadClient`
(httpx async, 8 descargas concurrentes con Semaphore). Cada archivo se almacena
en `data/bronze/{category}/{year}-{month:02d}.parquet` con un registro de
auditoría en `data/bronze/audit.parquet`.

**Idempotencia:** si el footer Parquet de un archivo existente es legible, se
omite la descarga. Archivos truncados se re-descargan automáticamente.

#### Silver

Tres sub-fases ejecutadas en orden:

1. **Silver calidad** — `SilverCleaner.clean()` aplica un filtro **reject-only**
   (sin imputación ni corrección). Cinco reglas de rechazo en orden, primera
   regla que falla gana:

   | Regla | Condición de rechazo |
   |---|---|
   | Incomplete | Valor nulo en columna requerida (las nulables se definen en `config.yaml`) |
   | Timeliness | Fecha de recogida fuera del mes que corresponde al archivo |
   | Datetime order | `dropoff_datetime` anterior a `pickup_datetime` |
   | Integrity | `PULocationID` o `DOLocationID` no existen en la tabla de zonas |
   | Duplicate | Filas duplicadas exactas (ventana sobre `COMPOSITE_KEYS`, conserva la primera) |

   Salida: `data/silver/stage/` (datos limpios) + `data/silver/reject/`
   (registros descartados). Auditoría con FK a `bronze_audit_id`.

2. **Silver esquema** — `StarSchemaBuilder.build_dimensions()` construye las
   tablas de dimensiones fijas del modelo estrella: `dim_date` (calendario
   2023–2025, día ISO), `dim_zone`, `dim_ratecode`, `dim_payment_type`.

3. **Silver carga** — `StarSchemaBuilder.build_facts()` lee `silver/stage/` y
   produce tablas de hechos por categoría con columnas estandarizadas. Cada
   hecho incluye un `trip_id` (BIGINT producto de `xxhash64` de las claves
   compuestas) y timestamps `pickup_datetime`/`dropoff_datetime` normalizados.

#### Oro

`GoldPipeline` lee los hechos y dimensiones del modelo estrella y produce:

- **6 marts Power BI** en grano agregado (1 fila por fecha × hora/bloque × zona)
- **3 ML feature stores** en grano viaje

Los marts de grano agregado usan `GoldContext.get_union_facts()` — una **vista
perezosa** (lazy) de proyección estrecha sobre todos los hechos. Nunca se
persiste (ni siquiera `DISK_ONLY` — OOM a ~940M filas). Los marts de grano viaje
subclasifican `TripGrainMart` y usan `partitionOverwriteMode=dynamic` para
escritura idempotente por partición.

Tres pipelines de ML independientes (comando `--gold-ml`) entrenan modelos
sobre los feature stores: **K-Modes** (clustering de perfiles de viaje),
**Isolation Forest** (detección de fraude por RatecodeID) y **SARIMAX** (pronóstico
de viajes por borough × servicio).

### Speed layer (ruta rápida)

El speed layer procesa viajes individuales en tiempo real a través de
`POST /api/v1/ingest`. El flujo dentro de `EventProcessor` es:

```
RideEvent (JSON)
    │
    ▼
Validación: completitud, timeliness, orden datetime, zona válida
    │
    ▼
Enriquecimiento:
  • trip_id = xxhash64(COMPOSITE_KEYS)  ← mismo algoritmo que batch
  • time_blocks (bloque horario, franja, día categoría)
  • generosity (categoría de propina)
  • passenger_groups
  • revenue (normalización de componentes tarifarios)
    │
    ▼
EventBus (pub/sub en proceso)
    │
    ├──► RealtimeAggregator → Redis HINCRBY (5 acumuladores O(1))
    ├──► FraudScorer → features → IsolationForest → score → Redis + pubsub
    └──► TripProfiler → KModes predict → cluster ID → Redis
```

#### Agregadores en Redis

Cinco familias de acumuladores, todas operaciones O(1) mediante pipelines
`HINCRBY`:

| Acumulador | Prefijo Redis | Propósito |
|---|---|---|
| Demand-volume | `rt:dv:` | Viajes, oferta, déficit por bloque horario |
| Financial-performance | `rt:fp:` | Componentes tarifarios agregados |
| Operational-profile | `rt:op:` | Duración, distancia, velocidad |
| Supply-demand | `rt:sd:` | Balance oferta-demanda por zona |
| Tipping-behavior | `rt:tb:` | Propinas por borough, tipo de pago, generosidad |

Todas las claves Redis tienen **TTL de 48h**. Los datos canónicos residen siempre
en los hechos del modelo estrella (el pipeline batch los recoge gracias a la
paridad de `trip_id`).

### Serving layer

La capa de servicio expone dos familias de endpoints mediante FastAPI:

#### Rutas históricas (`GET /api/v1/historic/{mart}`)

Usan `PolarsQueryEngine` que realiza `scan_parquet()` perezoso sobre los
directorios de `data/gold/marts/`. El buscador aplica pushdown de predicados
directamente sobre Parquet (filtro por servicio, año, mes, zona, etc.) sin
cargar los datos completos en memoria. Cache de 60s en metadatos de la consulta.

**No se usa Spark en serving.** El contenedor de servicio ocupa ~500MB frente
a ~2GB con Spark, y Polars `scan_parquet()` ofrece rendimiento comparable para
consultas agregadas con filtro por partición.

#### Rutas en tiempo real (`GET /api/v1/realtime/{view}[/stream]`)

Usan `MergedViewReader` para unir los datos del batch layer (completos hasta el
último bloque cerrado) con el estado actual en Redis (bloque en curso). El
algoritmo de fusión es:

1. Cargar datos históricos desde gold marts (vía `PolarsQueryEngine`)
2. Escanear claves Redis con patrón `rt:{mart_prefix}:*`
3. Parsear cada clave en una fila estructurada mediante funciones `_key_to_row`
   específicas de cada mart
4. **Deduplicar por tupla de clave:** cargar primero los datos batch en un
   diccionario, luego agregar las filas Redis solo si su clave de
   desduplicación **no existe ya** (batch gana en solape)
5. Ordenar por columna temporal descendente, devolver hasta `limit` filas

El endpoint `/stream` emite SSE:
- Evento `snapshot` inicial: vista completa fusionada
- Eventos `increment` subsiguientes: nuevas filas desde el speed layer
  (vía suscripción a `EventBus`)
- Heartbeat cada 30s para mantener la conexión viva

---

## Paridad de trip_id entre batch y speed

El `trip_id` se calcula como `xxhash64` de las columnas compuestas
(`COMPOSITE_KEYS`) que identifican unívocamente cada viaje. El algoritmo es
**idéntico** en ambos lados:

- **Batch (PySpark):** `F.xxhash64(F.concat_ws("||", *keys))`
- **Speed (Python):** `xxhash.xxh64("||".join(values), seed=0).intdigest()`

Esto garantiza que un viaje ingerido en tiempo real genera el **mismo trip_id**
que cuando fluye por el pipeline batch. Consecuencias de diseño:

1. **Deduplicación en el merge:** el batch layer sobreescribe cualquier estado
   Redis transitorio para el mismo viaje
2. **Drill-through:** desde un mart gold agregado se puede navegar al detalle
   del viaje en los hechos del modelo estrella
3. **Unicidad en speed:** `SETNX trip_id` en Redis previene duplicados en
   ingesta en tiempo real

---

## Principios de diseño transversales

### Idempotencia y reanudabilidad

Cada fase del pipeline verifica si su salida ya existe antes de ejecutarse.
Si una ejecución se interrumpe, al relanzar el mismo comando retoma exactamente
donde quedó. Para forzar una re-ejecución basta borrar el directorio de salida
correspondiente:

| Fase | Mecanismo de omisión |
|---|---|
| Bronce | Footer Parquet legible → saltea descarga |
| Profiling | Archivo JSON existente → reutiliza |
| Silver calidad | `_SUCCESS` marker en `stage/` → saltea mes |
| Star facts | `_SUCCESS` marker en partición → saltea |
| Gold incremental | `partitionOverwriteMode=dynamic` → solo escribe particiones faltantes |

### Concurrencia dinámica

Las fases que usan PySpark ajustan el número de workers según el peso del
dataset. Un mismo patrón se repite en profiling, silver, star y gold:

| Categoría | Filas/mes (aprox.) | Workers |
|---|---|---|
| `fhvhv` | ~200M | 1 |
| `yellow` | ~20M | 1 |
| `green` | ~500K | 2–3 |
| `fhv` | ~65K | 2–3 |

Esto evita OOM en datasets grandes mientras acelera el procesamiento de los
ligeros.

### Esquemas heterogéneos

Los nombres de columna varían entre categorías y años (ej.
`tpep_pickup_datetime` vs `lpep_pickup_datetime` vs `pickup_datetime`;
`PULocationID` vs `PUlocationID`). El pipeline resuelve estas diferencias
mediante listas de candidatos y el helper `_first_match`. Columnas que
aparecen solo a partir de cierto año (como `cbd_congestion_fee` desde 2025)
se protegen con `if col in df.columns`.

**Regla:** no hardcodear nunca un solo nombre de columna a través de
categorías. Seguir el patrón de lista de candidatos + `_first_match`.

### Abstracción de almacenamiento

Todas las capas (bronce, silver, gold, profiling, auditoría) usan una
abstracción unificada en `app/utils/storage.py`. La variable de entorno
`STORAGE_BACKEND` conmuta entre:

- `local` (default): `pathlib.Path` estándar sobre el sistema de archivos local
- `s3`: `S3Path` (envoltura sobre `s3fs`) que opera sobre URIs `s3://`

`storage.for_spark(path)` reescribe `s3://` → `s3a://` para compatibilidad con
Spark. El directorio de shuffle/spill de Spark (`spark.local.dir`) permanece
siempre en disco local incluso con backend S3.

### Cadena de auditoría

Tres archivos `audit.parquet` encadenan los metadatos de cada fase:

```
bronze_audit_id ──► silver_audit_id ──► gold_audit_id
(descargas)         (limpieza calidad)    (marts + feature stores)
```

Cada archivo es escrito por Polars (independientemente del backend de
almacenamiento) y contiene: ID único, nombre del archivo/dataset, recuento de
filas, marcas de tiempo de inicio y fin, y estado de la operación. La clave
foránea permite trazar el origen de cualquier registro a través de todo el
pipeline.

---

## Diagrama de flujo completo

```
                         ┌─────────────────────┐
                         │  CloudFront TLC      │
                         │  (parquet público)   │
                         └──────────┬──────────┘
                                    │
                          ┌─────────▼─────────┐
                          │   BRONZE PIPELINE  │
                          │  DownloadClient    │
                          │  (httpx async,     │
                          │   8 concurrencia)   │
                          │                    │
                          │  data/bronze/*     │
                          │  audit.parquet     │
                          └─────────┬─────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
          ┌─────────▼─────────┐    │    ┌──────────▼──────────┐
          │  PROFILING        │    │    │  SILVER QUALITY      │
          │  (PySpark,        │    │    │  (PySpark,           │
          │   8 dimensiones,   │    │    │   reject-only,       │
          │   read-only)       │    │    │   5 reglas)          │
          │                    │    │    │                     │
          │  data/profiling/*  │    │    │  data/silver/stage/ │
          └────────────────────┘    │    │  data/silver/reject/│
                                    │    └──────────┬──────────┘
                                    │               │
                                    │    ┌──────────▼──────────┐
                                    │    │  SILVER ESQUEMA     │
                                    │    │  StarSchemaBuilder  │
                                    │    │  dimensions         │
                                    │    │                     │
                                    │    │  data/silver/star/  │
                                    │    │  dims/              │
                                    │    └──────────┬──────────┘
                                    │               │
                                    │    ┌──────────▼──────────┐
                                    │    │  SILVER CARGA       │
                                    │    │  StarSchemaBuilder  │
                                    │    │  facts (trip_id,    │
                                    │    │  timestamps std)    │
                                    │    │                     │
                                    │    │  data/silver/star/  │
                                    │    │  facts/*            │
                                    │    └──────────┬──────────┘
                                    │               │
                                    │    ┌──────────▼──────────┐
                                    │    │  GOLD PIPELINE      │
                                    │    │  (PySpark)          │
                                    │    │  6 marts +          │
                                    │    │  3 ML feat. stores  │
                                    │    │                     │
                                    │    │  data/gold/marts/*  │
                                    │    │  data/gold/ml/*     │
                                    │    │  audit.parquet      │
                                    │    └──────────┬──────────┘
                                    │               │
                                    │    ┌──────────▼──────────┐
                                    │    │  GOLD ML MODELS     │
                                    │    │  (pandas + sklearn/ │
                                    │    │   kmodes/statsmodels)│
                                    │    │                     │
                                    │    │  data/gold/models/* │
                                    │    └─────────────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │   SERVING LAYER    │
                          │   FastAPI          │
                          │                    │
                          │  /historic/*       │
                          │  (Polars lazy scan)│
                          │                    │
                          │  /realtime/*       │
                          │  (MergedViewReader)│
                          │                    │
                          │  POST /ingest      │
                          │  (speed layer)     │
                          └────────────────────┘
```

---

## Stack tecnológico

| Componente | Versión / Detalle |
|---|---|
| Python | 3.12 |
| Gestor de paquetes | uv |
| Procesamiento batch | PySpark 4.x (local mode, AQE, zstd:9) |
| Descargas + auditoría | Polars, httpx |
| ML models | scikit-learn, kmodes, statsmodels, joblib |
| Serving | FastAPI, Polars (lazy scan), SSE |
| Speed layer | Python, Redis (asyncio), xxhash |
| Almacenamiento | Local (pathlib) o S3 (s3fs → `STORAGE_BACKEND`) |
| Orquestación | Airflow 2.10.5 (Docker Compose, LocalExecutor) |
| Formato de datos | Parquet, codec zstd nivel 9 |
| Contenedores | Docker, docker-compose |

Documentación generada con el marco **Diátaxis** — este documento es de tipo
*Explanation*, orientado a la comprensión del diseño arquitectónico.
