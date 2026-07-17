# Pipeline ETL + Calidad de Datos — NYC TLC

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/packaging-uv-DE5FE9?logo=uv&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?logo=apachespark&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.42-CD792C?logo=polars&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![pytest](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)

Pipeline de datos para los registros de viajes de la **NYC Taxi & Limousine Commission (TLC)**,
construido como una **arquitectura medallón** (bronze → silver → gold) con una etapa independiente
de *profiling* de calidad, más una **capa serving Lambda** (batch + tiempo real) sobre los datos ya
procesados.

Procesa **902 millones de viajes** (2023–2025): los descarga, evalúa su calidad en 8 dimensiones,
los limpia hacia un modelo estrella, produce marts agregados para Power BI y feature stores de ML,
entrena tres modelos (K-Modes, Isolation Forest, SARIMAX) y los expone vía HTTP/SSE.

Las cuatro categorías TLC: **green**, **yellow**, **fhv**, **fhvhv**.

## Características

- **Descarga asíncrona** de parquet desde el CDN oficial de la TLC, con auditoría encadenada.
- **Profiling de calidad** en 8 dimensiones (exactitud, completitud, consistencia, integridad,
  razonabilidad, oportunidad, unicidad, validez) con reporte JSON + `index.html`.
- **Capa silver**: limpieza **solo por rechazo** (los valores de origen no se alteran) y **modelo
  estrella** (dimensiones + hechos por categoría). Nada se pierde: `bronze = stage + reject`.
- **Capa gold**: 6 *marts* de **grano agregado** para Power BI, 3 feature stores de ML y dimensiones
  enriquecidas.
- **Modelos ML integrados**: clustering K-Modes, detección de fraude con Isolation Forest y pronóstico
  de viajes con SARIMAX, entrenados desde los feature stores.
- **Capa serving (Lambda)**: FastAPI con endpoints históricos (Polars lazy sobre los marts) y streams
  SSE en tiempo real que fusionan el batch con el estado en Redis. **Sin Spark** — contenedor ~500MB.
- **Idempotente y reanudable**: cada capa omite las salidas ya materializadas; una corrida interrumpida
  se reanuda donde quedó.
- **Backend intercambiable**: todas las capas leen/escriben en filesystem local (default) o **S3**, con
  solo cambiar `STORAGE_BACKEND`.
- **Orquestación opcional con Airflow sobre Docker Compose**: cada fase es su propio DAG encadenado.
- **Export a Power BI**: consolida el gold particionado en parquets individuales, rematerializando las
  columnas de partición.

## Arquitectura

```
          descarga        profiling         limpieza + estrella       marts BI + features      modelos
 TLC CDN ─────────► BRONZE ─────────► (calidad) ─────────► SILVER ─────────► GOLD ─────────► GOLD-ML
                    parquet    JSON/HTML          stage → star (dims+facts)  marts/ ml/ dims/   models/
                                                                               │                  │
                                                                               ▼                  ▼
                                                                        SERVING (FastAPI) ◄── Redis
                                                                        histórico + SSE      speed layer
```

| Etapa | Entrada | Salida |
|---|---|---|
| **Bronze** | TLC CDN | `data/bronze/{category}/{year}-{month}.parquet` |
| **Profiling** | `data/bronze/` | `data/profiling/**.json` + `index.html` |
| **Silver — quality** | `data/bronze/` | `data/silver/stage/` (+ `reject/`) |
| **Silver — schema** | zone-lookup | `data/silver/star/dims/` |
| **Silver — load** | `data/silver/stage/` | `data/silver/star/facts/` |
| **Gold** | `data/silver/star/` | `data/gold/{marts,ml,dims}/` |
| **Gold — ML** | `data/gold/ml/` | scores/labels/forecast + `data/gold/models/` |
| **Serving** | `data/gold/` + Redis | HTTP `GET /api/v1/{historic,realtime}/*` |

### Trazabilidad punta a punta

El linaje cierra sin un solo registro sin explicar, verificable en `notebooks/revision_05_auditoria.ipynb`:

```
bronze descargado    904,327,862
  − rechazado           2,013,069   (0.22%)
  = limpio            902,314,793
  = star/facts        902,314,793   ← idéntico
  = SUM(viajes)       902,314,793   ← los marts agregados representan todos los viajes
```

## Productos analíticos

La capa gold materializa 9 productos analíticos (ver [`docs/ARQUITECTURA.md`](./docs/ARQUITECTURA.md)):

| Categoría | Producto | Salida gold |
|---|---|---|
| Descriptivo | Volumen y Demanda | `marts/mart_demand_volume` |
| Descriptivo | Rendimiento Financiero | `marts/mart_financial_performance` |
| Descriptivo | Perfil Operativo | `marts/mart_operational_profile` |
| Diagnóstico | Desequilibrio Oferta-Demanda | `marts/mart_supply_demand_balance` |
| Diagnóstico | Análisis ABC/XYZ de zonas | `marts/mart_abc_xyz_zones` |
| Diagnóstico | Comportamiento de Propinas | `marts/mart_tipping_behavior` |
| Predictivo | Predicción de viajes (SARIMAX) | `ml/ml_feat_arima_trips` → `ml/ml_sarimax_trips_forecast` |
| Predictivo | Clustering de perfiles (K-Modes) | `ml/ml_feat_kmodes_trips` → `ml/kmodes_model/` |
| Predictivo | Detección de fraude (Isolation Forest) | `ml/ml_feat_isolation_fraud` → `ml/ml_isolation_fraud_scores` |

> [!NOTE]
> Los marts usan **grano agregado** (1 fila por fecha × hora/bloque × zona/borough), no 1 fila por viaje:
> los dashboards consumen conteos y promedios, y el grano viaje era inviable a escala completa. **No se
> pierde información**: `SUM(viajes)` en el mart == filas de los facts (~44 viajes por fila). El detalle
> viaje a viaje vive íntegro en `data/silver/star/facts/`.

## Requisitos previos

- **Python 3.12**
- **[uv](https://docs.astral.sh/uv/)** para gestión de dependencias
- **Java (JDK 11+)** requerido por PySpark
- **Windows**: `HADOOP_HOME` apuntando a un directorio `bin/` con `hadoop.dll` y `winutils.exe`
  compatibles (p. ej. de [cdarlint/winutils](https://github.com/cdarlint/winutils)).

## Inicio rápido

```bash
# 1. Instalar dependencias
uv sync

# 2. (Windows) configurar Hadoop para Spark
#    PowerShell:  $env:HADOOP_HOME = "C:\ruta\a\hadoop"
#    bash:        export HADOOP_HOME=/ruta/a/hadoop

# 3. Pipeline completo, de una (idempotente: reanuda donde quedó)
uv run main.py --all
```

`--all` encadena: bronze → verificación de completitud → silver → schema → load → gold incremental →
profiling. Para ejecutar las etapas por separado:

```bash
uv run main.py                  # Bronze: descarga zone-lookup + datos de viajes
uv run main.py --profile        # Profiling de calidad
uv run main.py --silver         # Silver: limpieza por rechazo
uv run main.py --silver schema  # Silver: dimensiones del modelo estrella
uv run main.py --silver load    # Silver: tablas de hechos
uv run main.py --gold           # Gold: marts Power BI + features ML

# Entrenar los modelos (cada uno requiere su feature store)
uv run main.py --gold-ml kmodes     # clustering de perfiles de viaje
uv run main.py --gold-ml isolation  # detección de fraude por RatecodeID
uv run main.py --gold-ml sarimax    # pronóstico de viajes por borough
```

Opciones de la capa gold:

```bash
uv run main.py --gold incremental                                       # solo particiones nuevas
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud # subconjunto de builders
```

> [!IMPORTANT]
> El orden importa. `--silver schema` debe ejecutarse antes de `--silver load` (los hechos se unen contra
> las dimensiones), y `--gold` lee de `data/silver/star/`: aborta con un mensaje claro si silver no está
> cargada.

> [!TIP]
> **Todas las etapas son idempotentes**: si una corrida se interrumpe, basta relanzar el mismo comando y
> solo se procesa lo que falta. Para forzar el reprocesamiento de un mes, borra su salida (p. ej. el
> directorio en `data/silver/stage/` o el JSON en `data/profiling/`).

## Export a Power BI

El gold se escribe **particionado estilo Hive**, así que `service_id`/`year`/`month` viven **solo en los
nombres de carpeta, no dentro del parquet**. Concatenar los archivos a mano las pierde en silencio. El
export las rematerializa como columnas reales y consolida cada dataset en un parquet único:

```bash
uv run scripts/export_powerbi.py                # gold → data/powerbi/ (18 parquets)
uv run scripts/export_powerbi.py --skip-heavy   # sin los 2 feature stores trip-grain (>100M filas c/u)
uv run scripts/export_audit_powerbi.py          # auditoría → data/powerbi_audit/ (5 tablas)
```

Para el tablero se usan **9 archivos**: los 6 marts + las 3 dimensiones (`dim_date_gold`,
`dim_zone_gold`, `dim_ratecode_theoretical`), ~540 MB. Relacionar por `pu_location_id` → `LocationID`
y `fecha_viaje` → `date` (las dims conservan el PascalCase del zone-lookup original de la TLC; los
marts usan snake_case).

La carpeta [`powerbi/`](./powerbi/) contiene las medidas DAX (`medidas.dax`) y el tema visual
(`tema_nyc_tlc.json`). El export de datos va a `data/powerbi/`, que está gitignoreado.

> [!WARNING]
> Los feature stores `ml_feat_isolation_fraud` (128M filas) y `ml_feat_kmodes_trips` (164M) son **insumo
> de entrenamiento, no datos de tablero**: son el 92% de los 6.5 GB del export. Usa `--skip-heavy` salvo
> que los necesites explícitamente.

> [!NOTE]
> El export usa codec **snappy**, no el `zstd-9` del resto del proyecto: el conector Parquet de Power
> Query soporta snappy en cualquier versión, mientras que zstd depende de la build de Power BI Desktop.
> Usa `--compression zstd` si tu destino lo soporta.

El export de auditoría marca `es_ultima_corrida` en lugar de deduplicar: los `audit.parquet` son **logs
de tipo append** (cada re-corrida agrega una fila), así que sumarlos crudos infla bronze un 43%. Filtra
por esa columna en el tablero.

## Capa serving (Lambda)

```bash
uv sync --extra serving
uv run main.py --serve                 # FastAPI + speed layer (requiere Redis y data/gold/)
uv run main.py --speed                 # solo motor speed (eventos JSON por stdin)
docker compose up serving redis -d     # vía Docker
```

| Endpoint | Qué hace |
|---|---|
| `GET /api/v1/historic/*` | Los 6 marts vía Polars `scan_parquet()` (lazy, predicate pushdown) |
| `GET /api/v1/realtime/*` | Vista fusionada batch + Redis |
| `GET /api/v1/realtime/*/stream` | Lo mismo, como stream SSE |
| `POST /api/v1/ingest` | Ingesta de un viaje: limpieza → enriquecimiento → agregación → score de fraude |
| `POST /api/v1/admin/reload-models` | Recarga los modelos joblib sin reiniciar |

**Paridad de `trip_id`**: la capa speed calcula `xxhash64` de `SilverCleaner.COMPOSITE_KEYS` en Python
puro, produciendo el **mismo BIGINT** que `F.xxhash64` de Spark. Un viaje ingerido en tiempo real recibe
el mismo `trip_id` que tendría al pasar por el batch — eso es lo que hace segura la fusión.

**Frontera de merge**: el gold batch tiene los bloques pasados completos; Redis tiene el bloque actual
incompleto. `MergedViewReader` los cose deduplicando por tupla de clave (**gana el batch** en solapes).
Las claves de Redis expiran a las 48h; el dato canónico siempre aterriza en los facts de silver.

## Configuración

`config.yaml` controla qué datos se procesan y los parámetros de la capa gold:

```yaml
datasets:
  years: [2023, 2024, 2025]  # años completos (4 categorías × 12 meses c/u)
    # o un módulo puntual:
    # - category: fhvhv
    #   year: 2025
    #   month: 1

gold:
  supply_demand:
    block_minutes: 15        # ventana temporal del análisis oferta/demanda
    deficit_threshold: -10
  abc_xyz:
    class_a_pct: 0.80        # cortes de Pareto ABC
    class_b_pct: 0.15
    xyz_x_max: 0.2           # cortes del coeficiente de variación XYZ
    xyz_y_max: 0.5
  generosity:
    standard_low: 10         # umbrales (%) de categoría de propina
    standard_high: 18
  isolation_fraud:
    contamination: 0.05      # hiperparámetros del Isolation Forest
    n_estimators: 100
    random_state: 42
  kmodes:
    max_k: 8                 # búsqueda de k (codo + silhouette)
    max_sample_per_service: 100000
```

Las columnas requeridas vs. nulables se definen en `profiling.rules.nullability`: una columna **fuera**
de ese conjunto es obligatoria, y un nulo ahí provoca el rechazo de la fila en silver.

## Backend de almacenamiento (local o S3)

Todas las capas (`bronze/silver/gold/profiling` + los 3 `audit.parquet`) pueden vivir en el **filesystem
local** (default) o en **S3**, sin mezclar por capa y sin código duplicado. Se controla con una sola
variable en `.env` (ver `.env.example`):

```bash
cp .env.example .env          # y completar credenciales si se usa S3
uv sync --extra s3            # instala s3fs (Spark usa hadoop-aws aparte)
```

```dotenv
STORAGE_BACKEND=s3            # local (default) | s3
AWS_ACCESS_KEY_ID=...         # leídas del entorno, nunca de config.yaml
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET=mi-bucket
S3_PREFIX=tlc-pipeline
```

La abstracción vive en `app/utils/storage.py` (`get_root()` devuelve un `Path` local o un `S3Path` vía
`s3fs`); el resto del pipeline compone rutas igual que siempre, solo cambia la raíz. Con backend local
el comportamiento es idéntico byte a byte al de antes de introducir la abstracción.

> [!NOTE]
> El shuffle/spill de Spark (`spark.local.dir`) **siempre** se queda en disco local, incluso con
> `STORAGE_BACKEND=s3`: mandarlo a S3 añadiría latencia de red a una operación ya intensiva en memoria.

## Ejecución con Docker + Airflow

El pipeline puede orquestarse con **Airflow sobre Docker Compose**: Postgres (metadatos) + webserver +
scheduler en `LocalExecutor`, más servicios opcionales `jupyter` (notebooks de revisión), `redis` y
`serving`. Spark corre **embebido** en el proceso de cada tarea (no hay clúster Spark aparte).

```bash
uv lock                              # una vez, tras añadir extras (commitear uv.lock)
docker compose build
docker compose up airflow-init       # inicializa la metadata DB (una vez)
docker compose up                    # webserver + scheduler + postgres
```

Cada fase es su **propio DAG** (8 en total). Seis se encadenan con
`TriggerDagRunOperator(wait_for_completion=True)`; los otros dos son `schedule=None` (manuales):

```
dag_01_bronze → dag_02_silver_quality → dag_03_silver_schema → dag_04_silver_load → dag_05_gold → dag_06_profiling
dag_07_gold_ml   # entrenamiento (kmodes/isolation/sarimax), manual desde la UI
dag_08_serving   # levanta la capa serving (--serve), manual desde la UI
```

Todos los DAGs son `BashOperator`s finos que invocan el mismo CLI — no reimplementan lógica. El
*profiling* corre al final a propósito (documentación de solo lectura que no alimenta silver/gold).

> [!IMPORTANT]
> **Despausa los DAGs de la cadena** (`dag_01`…`dag_06`) en la UI antes de disparar `dag_01_bronze`: la
> cadena `TriggerDagRunOperator` queda encolada para siempre si un DAG río abajo está en pausa.

> [!TIP]
> **Memoria (Windows/Docker Desktop)**: el VM de WSL2 necesita **≥8 GB**. `SPARK_DRIVER_MEMORY` y
> `SPARK_MASTER_CORES` (default `8g`/`8`) se ajustan en `.env` según tu VM, porque comparte RAM con
> Airflow + Postgres. El shuffle va a `/tmp/spark-temp` (disco del contenedor), no al bind mount de
> `./data` (I/O lenta vía gRPC-FUSE).

## Estructura del proyecto

```
app/
├── client/download_client.py     # Descargas async (Polars) + auditoría
├── pipeline/
│   ├── bronze.py                  # Etapa Bronze
│   ├── silver.py                  # SilverPipeline (orquestador de la limpieza)
│   ├── silver_impl/
│   │   ├── cleaner.py             #   → SilverCleaner (solo rechazo)
│   │   └── star.py                #   → StarSchemaBuilder (dims + hechos)
│   ├── gold.py                    # GoldPipeline (orquestador)
│   └── gold_impl/
│       ├── mart_builder.py        # Bases: GoldBuilder, TripGrainMart, GoldContext
│       ├── feature_rules/         # Heurísticas reusables (bloques, propina, tarifas)
│       ├── dims/                  # Dimensiones gold enriquecidas
│       ├── marts/                 # 6 marts agregados para Power BI
│       └── ml/                    # 3 feature stores + 3 pipelines de modelos
├── profiling/                     # Profiling de calidad (8 dimensiones)
├── serving/                       # FastAPI: rutas histórico/realtime/admin + merged_view
├── speed/                         # Motor speed: ingest, Redis, fraud scorer, agregación
├── schemas/settings_schema.py     # Validación de config (Pydantic)
└── utils/                         # spark, logger, globals, settings, storage (local/S3)
scripts/                           # Export a Power BI (gold + auditoría)
dags/                              # 8 DAGs de Airflow (6 encadenados + gold-ml y serving manuales)
docs/                              # ARQUITECTURA, CLI, CONFIG, INSTALACION
notebooks/                         # Revisión por capa (bronze → gold → auditoría)
tests/                             # unit / spark / integration (pytest)
config.yaml                        # Configuración del pipeline
Dockerfile / docker-compose.yml    # Ejecución orquestada
main.py                            # Punto de entrada (CLI)
```

Los notebooks `notebooks/revision_01..05` documentan cada capa: inventario del bronce, resultados del
profiling, integridad silver (`bronze = stage + reject`), granos y no-pérdida de los marts gold, y la
cadena de auditoría completa.

## Tests

```bash
PYTHONPATH=. uv run pytest                         # todos
PYTHONPATH=. uv run pytest -m "not integration"    # rápidos: unit + spark
PYTHONPATH=. uv run pytest -m integration          # lentos: e2e del pipeline completo
```

> [!NOTE]
> Los tests muestrean 50 filas de `data/bronze/{yellow,green,fhv,fhvhv}/2025-01.parquet`, por lo que
> requieren haber ejecutado `uv run main.py` al menos una vez.

## Stack

Python 3.12 gestionado con **uv**. **Polars** para descargas, auditoría y las consultas del serving;
**PySpark** para profiling, silver y gold (con AQE, codec parquet `zstd-9` y escrituras coalesceadas);
**scikit-learn / kmodes / statsmodels** para los modelos; **FastAPI + Redis** para la capa serving;
**pyarrow** para metadatos de parquet; **Pydantic** para configuración.

Extras opcionales: `s3` (`s3fs`), `jupyter` (`jupyterlab`), `serving` (`fastapi`, `uvicorn`, `redis`,
`xxhash`, `sse-starlette`).

> [!WARNING]
> Los mensajes de log y de usuario están en **español**; el código, identificadores y comentarios en
> **inglés**. La tarifa plana de JFK en `dim_ratecode_theoretical` (heurística de fraude) debe
> verificarse contra la normativa TLC vigente.
