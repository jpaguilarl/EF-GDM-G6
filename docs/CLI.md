# CLI de Referencia

El pipeline batch (bronce → silver → gold) y la capa serving (FastAPI + speed
layer) se controlan desde un solo punto de entrada:

```bash
uv run main.py [flags] [subcomando]
```

Todas las fases del pipeline batch son **idempotentes y reanudables**: si una
ejecución se interrumpe, al relanzar el mismo comando retoma exactamente donde
quedó sin repetir trabajo. Para forzar una re-ejecución, basta borrar el
directorio de salida correspondiente.

Los comandos `--serve` y `--speed` inician **servicios** de ejecución continua
(no fases de pipeline).

---

## Subcomandos

### `uv run main.py` (sin flags)

Pipeline **bronce**: descarga datos TLC desde
`https://d37ci6vzurychx.cloudfront.net/trip-data/`.

1. Descarga `taxi+_zone_lookup.parquet` (mapa de zonas).
2. Itera años × categorías (yellow, green, fhv, fhvhv) × meses 1–12.
3. Omite archivos existentes cuyo footer Parquet sea legible (archivos
   truncados se re-descargan).

| Salida | Ruta |
|---|---|
| Datos fuente | `data/bronze/{category}/{year}-{month:02d}.parquet` |
| Auditoría | `data/bronze/audit.parquet` |

---

### `--all`

Pipeline **completo end-to-end** (7 fases en orden):

```
bronce → verificación de completitud → silver calidad → silver esquema
→ silver carga → gold incremental → profiling
```

- Tras la descarga inicial verifica que todos los archivos esperados tengan
  un footer Parquet legible; si faltan archivos, reintenta la descarga una vez
  (CloudFront puede devolver 403 transitorios en ráfagas grandes). Si tras el
  reintento persisten archivos faltantes, falla con error.
- **Profiling se ejecuta al final** porque es documentación de solo lectura que
  no alimenta a las capas posteriores; los marts de Power BI llegan antes.

Equivalente a encadenar manualmente:

```bash
uv run main.py                                 # bronce
uv run main.py --silver quality                 # silver calidad
uv run main.py --silver schema                  # dimensiones estrella
uv run main.py --silver load                    # hechos estrella
uv run main.py --gold incremental               # gold incremental
uv run main.py --profile                        # profiling
```

---

### `--silver [quality | schema | load]`

#### `quality` (default)

Lee `data/bronze/`, aplica reglas de rechazo y escribe datos limpios más
registros rechazados. Usa **concurrencia dinámica**: 1 worker para fhvhv/yellow
(evitar OOM), 2 para green/fhv.

Reglas de rechazo (primera regla que falla gana):

| Regla | Descripción |
|---|---|
| **Incomplete** | Valor nulo en una columna requerida (las nulables se definen en `config.yaml` → `profiling.rules.nullability`). |
| **Timeliness** | Fecha de recogida fuera del mes que corresponde al archivo. |
| **Datetime order** | `dropoff_datetime` anterior a `pickup_datetime`. |
| **Integrity** | `PULocationID` o `DOLocationID` no existen en la tabla de zonas. |
| **Duplicate** | Filas exactamente duplicadas (todas las columnas). |

Los valores fuente pasan sin modificaciones (no hay truncamiento, imputación ni
re-cálculo de montos). Solo se aplica `_normalize_types` para convertir columnas
de código (`VendorID`, `RatecodeID`, `payment_type`, `passenger_count`) a
enteros.

| Salida | Ruta |
|---|---|
| Datos limpios | `data/silver/stage/{category}/{year}-{month:02d}.parquet` |
| Datos rechazados | `data/silver/reject/{category}/{year}-{month:02d}.parquet` |
| Auditoría | `data/silver/audit.parquet` (FK `bronze_audit_id`) |

#### `schema`

Construye las **dimensiones fijas** del modelo estrella:

| Dimensión | Contenido |
|---|---|
| `dim_vendor` | Catálogo de proveedores (1=Creative Mobile, 2=VeriFone, etc.). |
| `dim_ratecode` | Catálogo de tarifas (1=Standard, 2=JFK, 3=Newark…). |
| `dim_payment_type` | Catálogo de métodos de pago (1=Credit card, 2=Cash…). |
| `dim_date` | Calendario 2023–2025 con día, mes, año, día-semana ISO, día-año. |
| `dim_zone` | Zonas TLC con `LocationID`, `borough`, `zone`, `service_zone`. |

| Salida | Ruta |
|---|---|
| Dimensiones | `data/silver/star/dims/` |

#### `load`

Construye las **tablas de hechos** del modelo estrella para cada categoría y
mes. Usa concurrencia dinámica (1 worker para fhvhv/yellow, 3 para green/fhv).

Cada fila de hechos incluye:

- `trip_id` — **BIGINT** (`xxhash64` de las columnas compuestas); clave
  compacta para drill-through, tratar como `long` aguas abajo.
- `pickup_datetime`, `dropoff_datetime` — timestamps estandarizados.
- FK a todas las dimensiones (`vendor_id`, `ratecode_id`, `payment_type`,
  `pu_location_id`, `do_location_id`, `pu_date_id`, `do_date_id`).
- Métricas numéricas (distancia, tarifa, propina, peajes, etc.).

| Salida | Ruta |
|---|---|
| Hechos | `data/silver/star/facts/{category}/{year}-{month:02d}.parquet` |

**Orden**: `schema` debe ejecutarse antes que `load`. Los hechos dependen de
que las dimensiones existan.

---

### `--gold [full | incremental]`

Capa **gold**: marts Power BI agregados + feature stores ML.

#### `full` (default)

Reconstruye todo el histórico de principio a fin.

#### `incremental`

Escribe solo las particiones mensuales que faltan (idempotente). Usa
`partitionOverwriteMode=dynamic`.

**6 marts Power BI** (grano agregado — 1 fila por fecha × hora/bloque × zona):

| Mart | Descripción |
|---|---|
| `mart_demand_volume` | Viajes, oferta, déficit por bloque horario. |
| `mart_abc_xyz` | Clasificación ABC/XYZ de zonas. |
| `mart_generosity` | Propinas por zona y bloque. |
| `mart_ratecode_compliance` | Cumplimiento de tarifas planas (JFK, Newark). |
| `mart_supply_demand` | Balance oferta-demanda detallado. |
| `mart_sarimax_results` | Resultados del pronóstico SARIMAX. |

**3 ML feature stores** (grano viaje — carga completa cada vez):

| Feature store | Propósito |
|---|---|
| `ml_feat_kmodes_trips` | Perfiles de viaje para K-Modes (fhvhv muestreado al 5%, seed 42). |
| `ml_feat_isolation_fraud` | Atributos para detección de fraude. |
| `ml_feat_arima_trips` | Atributos para pronóstico SARIMAX. |

| Salida | Ruta |
|---|---|
| Marts Power BI | `data/gold/marts/{mart_name}/` |
| ML feature stores | `data/gold/ml/{feat_name}/` |
| Auditoría | `data/gold/audit.parquet` (FK `silver_audit_id`) |

#### `--only <lista>`

Filtro para construir solo un subconjunto de marts/feature stores (usado con
`--gold`). Lista separada por comas:

```bash
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud
```

---

### `--gold-ml [kmodes | isolation | sarimax]`

Entrenamiento de **modelos ML** sobre los feature stores generados por la capa
gold.

#### `kmodes` (default)

K-Modes: clustering de perfiles de viaje. Entrena un modelo por servicio
(yellow, green, fhv, fhvhv) con optimización de codo + silueta para elegir
`k` óptimo (1–`max_k`).

Requiere `ml_feat_kmodes_trips` (ejecutar `--gold --only ml_feat_kmodes_trips`
primero).

| Salida | Ruta |
|---|---|
| Labels | `data/gold/ml/kmodes_model/labels_{service}/` |
| Centroides | `data/gold/ml/kmodes_model/centroids_{service}/` |
| Perfiles | `data/gold/ml/kmodes_model/profiles_{service}/` |
| Modelo | `data/gold/models/kmodes/{service}.joblib` |

#### `isolation`

**Isolation Forest**: detección de anomalías por `RatecodeID`. Entrena un
modelo independiente por cada valor de `RatecodeID` presente en los datos.

Requiere `ml_feat_isolation_fraud` (ejecutar `--gold --only ml_feat_isolation_fraud`
primero).

| Salida | Ruta |
|---|---|
| Scores | `data/gold/ml/ml_isolation_fraud_scores/{ratecode_id}/` |
| Modelo | `data/gold/models/isolation_forest/{ratecode_id}.joblib` |

#### `sarimax`

**SARIMAX**: pronóstico de viajes por borough × `service_id`. Entrena un modelo
por segmento con ciclo diario (periodo 24).

Requiere `ml_feat_arima_trips` (ejecutar `--gold --only ml_feat_arima_trips`
primero).

| Salida | Ruta |
|---|---|
| Predicciones | `data/gold/ml/ml_sarimax_trips_forecast/` |
| Modelo | `data/gold/models/sarimax/{borough}_{service_id}.joblib` |

---

### `--profile`

Pipeline de **profiling**: evalúa la calidad de los datos bronce en 8
dimensiones.

Usa **PySpark** y concurrencia dinámica (1 worker para fhvhv/yellow, 3 para
green/fhv). Cada DataFrame se persiste una sola vez para que las 8 dimensiones
no re-lean el Parquet. Los reportes JSON existentes se reutilizan (no se
recomputan).

8 dimensiones de calidad:

| Dimensión | Evalúa |
|---|---|
| Accuracy | Coherencia de `total_amount` vs. suma de componentes. |
| Completeness | Proporción de valores nulos por columna. |
| Consistency | Tipos de datos y formatos. |
| Integrity | Integridad referencial de IDs de zona. |
| Reasonableness | Rangos físicamente plausibles (distancia, tarifa, etc.). |
| Timeliness | Fechas dentro del período esperado. |
| Uniqueness | Proporción de duplicados. |
| Validity | Cumplimiento de reglas de dominio. |

| Salida | Ruta |
|---|---|
| Reporte por dataset | `data/profiling/{category}/{year}-{month:02d}.json` |
| Resumen HTML | `data/profiling/index.html` |

---

### `--serve`

Inicia la **capa serving** completa: FastAPI con endpoints históricos + tiempo
real + ingesta speed layer. Uvicorn se ejecuta en el host y puerto definidos en
`config.yaml → serving.host/port` (por defecto `0.0.0.0:8000`).

**Requiere:**
- Redis accesible en `config.yaml → speed.redis_url` (por defecto
  `redis://localhost:6379/0`)
- `data/gold/marts/` con los marts Power BI generados por `--gold`

**Endpoints incluidos:**

| Grupo | Ruta | Descripción |
|---|---|---|
| Historic | `GET /api/v1/historic/{mart}` | Consultas sobre gold marts mediante `PolarsQueryEngine` (lazy `scan_parquet()` con pushdown de predicados). Cache de 60s. |
| Realtime | `GET /api/v1/realtime/{view}` | Vista fusionada batch + Redis (`MergedViewReader`). |
| Realtime | `GET /api/v1/realtime/{view}/stream` | SSE: evento `snapshot` inicial + `increment` en cada nuevo viaje. Heartbeat cada 30s. |
| Ingest | `POST /api/v1/ingest` | Recibe `RideEvent` JSON, lo limpia, enriquece y publica en `EventBus` → agregadores Redis + fraude + perfil. |
| Health | `GET /api/v1/health` | Estado del servicio y conexión Redis. |
| Admin | `POST /api/v1/admin/clear-cache` | Invalida la caché de consultas. |

**Paridad trip_id:** el algoritmo `xxhash64` es idéntico al del batch layer, por
lo que un viaje ingerido en tiempo real produce el mismo `trip_id` que cuando
fluye por el pipeline batch.

**No usa Spark:** el contenedor de servicio ocupa ~500MB (vs. ~2GB con Spark).
Polars `scan_parquet()` ofrece rendimiento comparable para consultas agregadas
con filtro por partición.

La misma aplicación se despliega con Docker Compose:

```bash
docker compose up serving redis -d
```

---

### `--speed`

Inicia el **motor de speed** en modo autónomo sin HTTP. Lee eventos de viaje
desde **stdin** como JSON Lines, los procesa a través de `EventProcessor` y
publica en Redis a través de los suscriptores de `EventBus`:

```
RideEvent (JSON línea) → EventProcessor (validar + enriquecer) → EventBus
  ├── RealtimeAggregator  → Redis HINCRBY (5 acumuladores)
  ├── FraudScorer         → IsolationForest → score → Redis
  └── TripProfiler        → SETNX trip_id → cluster ID → Redis
```

Cada línea procesada imprime `{"status": "accepted", "trip_id": <id>}` o
`{"status": "rejected"}` en stdout. Errores de parseo producen
`{"status": "error", "message": "..."}`.

Útil para pruebas de integración o para conectar productores de eventos que ya
tienen su propio transporte (Kafka, RabbitMQ, etc.).

**Requiere:** Redis accesible en `config.yaml → speed.redis_url`.

```bash
echo '{"trip_id":"...","pickup_datetime":"...",...}' | uv run main.py --speed
```

---

## Dependencias entre fases

```
bronce ──→ silver quality ──→ silver schema ──→ silver load ──→ gold ──→ gold-ml
                                                                      │
                                           profiling (independiente) ─┤
                                                                      │
                                           ┌──────────────────────────┘
                                           ▼
                                     ┌────────────┐     ┌────────────┐
                                     │  --serve    │     │  --speed   │
                                     │  (FastAPI + │     │  (stdin →  │
                                     │   ingest +  │     │   Redis)   │
                                     │   realtime) │     └────────────┘
                                     └────────────┘
```

| Comando | Requisito previo |
|---|---|
| `--silver quality` | `data/bronze/` (ejecutar `uv run main.py` una vez) |
| `--silver schema` | Ninguno (solo lookup tables) |
| `--silver load` | `--silver schema` y `--silver quality` |
| `--gold` (cualquier modo) | `data/silver/star/` (schema + load) |
| `--gold --only ml_feat_kmodes_trips` | `data/silver/star/facts/` |
| `--gold --only ml_feat_isolation_fraud` | `data/silver/star/facts/` |
| `--gold --only ml_feat_arima_trips` | `data/silver/star/facts/` |
| `--gold-ml kmodes` | `ml_feat_kmodes_trips` en `data/gold/ml/` |
| `--gold-ml isolation` | `ml_feat_isolation_fraud` en `data/gold/ml/` |
| `--gold-ml sarimax` | `ml_feat_arima_trips` en `data/gold/ml/` |
| `--profile` | `data/bronze/` |
| `--serve` | Redis en `speed.redis_url` + `data/gold/marts/` (ejecutar `--gold` primero) |
| `--speed` | Redis en `speed.redis_url` + `data/bronze/zone-lookup/` |

---

## Comportamiento general

### Idempotencia

Cada fase del pipeline batch escribe sus salidas una sola vez. Si el archivo de
salida ya existe y es válido, se omite:

- **Bronce**: verifica el footer Parquet antes de descargar.
- **Profiling**: reutiliza JSON existentes.
- **Silver**: saltea meses ya presentes en `stage/`.
- **Star facts**: saltea hechos mensuales existentes (fail-loud si falta
  dimensión).
- **Gold**: `incremental` escribe solo particiones faltantes.

Para re-ejecutar una fase, borra el directorio de salida correspondiente.

Los comandos `--serve` y `--speed` son **servicios** (no fases de pipeline):
se ejecutan de forma continua hasta que se interrumpen con Ctrl+C. No son
idempotentes — cada instancia arranca su propio servidor FastAPI o lee eventos
desde stdin.

### Concurrencia dinámica

Las fases que usan PySpark ajustan el número de workers según la categoría:

| Categoría | Workers |
|---|---|
| `fhvhv` | 1 |
| `yellow` | 1 |
| `green` | 2–3 |
| `fhv` | 2–3 |

Esto evita OOM en datasets grandes (fhvhv ~200M filas/mes) mientras acelera
los livianos.

### Fallo en gold

Si un builder individual falla, el error se recolecta y los builders restantes
continúan. Al finalizar, todos los errores se relanzan juntos (fail-loud).

### Esquemas heterogéneos

Los nombres de columna varían entre categorías y años (ej.
`tpep_pickup_datetime` vs `lpep_pickup_datetime` vs `pickup_datetime`). El
pipeline resuelve esto mediante listas de candidatos y el helper `_first_match`.
No se hardcodea un solo nombre de columna entre categorías.

---

## Ejemplos

```bash
# 1. Descargar datos bronce
uv run main.py

# 2. Pipeline completo (equivalente a encadenar todas las fases)
uv run main.py --all

# 3. Solo profiling de calidad sobre datos existentes
uv run main.py --profile

# 4. Silver: limpiar datos (rechazar registros inválidos)
uv run main.py --silver quality

# 5. Silver: construir dimensiones del modelo estrella
uv run main.py --silver schema

# 6. Silver: cargar hechos del modelo estrella
uv run main.py --silver load

# 7. Gold completo (marts + feature stores)
uv run main.py --gold

# 8. Gold solo incremental (particiones faltantes)
uv run main.py --gold incremental

# 9. Solo un mart y un feature store específicos
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud

# 10. Entrenar K-Modes (requiere ml_feat_kmodes_trips primero)
uv run main.py --gold --only ml_feat_kmodes_trips
uv run main.py --gold-ml kmodes

# 11. Entrenar Isolation Forest (requiere ml_feat_isolation_fraud primero)
uv run main.py --gold --only ml_feat_isolation_fraud
uv run main.py --gold-ml isolation

# 12. Entrenar SARIMAX (requiere ml_feat_arima_trips primero)
uv run main.py --gold --only ml_feat_arima_trips
uv run main.py --gold-ml sarimax

# 13. Iniciar capa serving (FastAPI + ingest + realtime SSE)
uv run main.py --serve

# 14. Speed engine standalone (leer eventos desde stdin)
echo '{"vendor_id":1,"pickup_datetime":"2025-01-15T10:30:00","dropoff_datetime":"2025-01-15T10:45:00","PULocationID":236,"DOLocationID":237,"trip_distance":3.5,"RatecodeID":1,"payment_type":1,"fare_amount":12.5,"tip_amount":2.5,"total_amount":15.0,"passenger_count":1,"congestion_surcharge":2.5}' | uv run main.py --speed

# 15. Modo Docker Compose (serving + Redis)
docker compose up serving redis -d
```
