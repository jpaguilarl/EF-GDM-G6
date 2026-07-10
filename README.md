# Pipeline ETL + Calidad de Datos — NYC TLC

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/packaging-uv-DE5FE9?logo=uv&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?logo=apachespark&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.42-CD792C?logo=polars&logoColor=white)
![pytest](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)

Pipeline de datos para los registros de viajes de la **NYC Taxi & Limousine Commission (TLC)**,
construido como una **arquitectura medallón** (bronze → silver → gold) con una etapa
independiente de *profiling* de calidad. Descarga los datos públicos (2023–2025), evalúa su
calidad en 8 dimensiones, los limpia hacia un modelo estrella, produce **marts agregados para
Power BI** y un **feature store para ML**, y finalmente **entrena los modelos de IA**
(K-Modes, Isolation Forest, SARIMAX).

Las cuatro categorías TLC: **green**, **yellow**, **fhv**, **fhvhv**.

## Características

- **Descarga asíncrona** de parquet desde el CDN oficial de la TLC, con trazabilidad (audit trail).
- **Profiling de calidad** en 8 dimensiones (exactitud, completitud, consistencia, integridad,
  razonabilidad, oportunidad, unicidad, validez) con reporte JSON + `index.html`.
- **Capa silver**: limpieza en dos fases (rechazo + corrección) y **modelo estrella**
  (dimensiones + hechos por categoría). Nada se pierde: `bronce = stage + reject`.
- **Capa gold**: 6 *marts* **de grano agregado** para Power BI (resúmenes por
  fecha/hora/zona/servicio — el detalle viaje a viaje vive en los facts), 3 tablas de
  *features* y dimensiones enriquecidas.
- **Modelos ML integrados**: clustering K-Modes, detección de fraude con Isolation Forest
  y pronóstico de viajes con SARIMAX, entrenados desde el feature store.
- **Idempotente y reanudable**: cada capa omite las salidas ya materializadas (descargas,
  perfiles, stage, facts, particiones gold); una corrida interrumpida se reanuda donde quedó.
- **Auditoría encadenada** entre capas (`bronze_audit_id → silver_audit_id → gold_audit_id`).
- **Backend de almacenamiento intercambiable**: todas las capas leen/escriben en el
  filesystem local (default) o en **S3**, con solo cambiar `STORAGE_BACKEND` — sin duplicar código.
- **Orquestación opcional con Airflow sobre Docker Compose**: cada fase del pipeline es su propio
  DAG encadenado, además del `uv run main.py` de siempre.
- **Configurable** vía `config.yaml` (años/categorías, parámetros de la capa gold e
  hiperparámetros de los modelos).

## Arquitectura

```
            descarga            profiling            limpieza + estrella         marts BI + features        modelos
  TLC CDN ───────────► BRONZE ───────────► (calidad) ───────────► SILVER ───────────► GOLD ───────────► GOLD-ML
                       parquet     JSON/HTML            stage → star (dims+facts)   marts/ + ml/ + dims/   models/
```

| Etapa | Entrada | Salida |
|---|---|---|
| **Bronze** | TLC CDN | `data/bronze/{category}/{year}-{month}.parquet` |
| **Profiling** | `data/bronze/` | `data/profiling/**.json` + `index.html` |
| **Silver — quality** | `data/bronze/` | `data/silver/stage/` (+ `reject/`) |
| **Silver — schema** | zone-lookup | `data/silver/star/dims/` |
| **Silver — load** | `data/silver/stage/` | `data/silver/star/facts/` |
| **Gold** | `data/silver/star/` | `data/gold/{marts,ml,dims}/` |
| **Gold — ML** | `data/gold/ml/` | `data/gold/ml/` (scores/labels/forecast) + `data/gold/models/` |

## Productos analíticos

La capa gold materializa los 9 productos descritos en
[`especificaciones_dashboards_nyc_tlc.md`](./especificaciones_dashboards_nyc_tlc.md):

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
> Los marts para Power BI usan **grano agregado** (1 fila por fecha × hora/bloque × zona/borough),
> no 1 fila por viaje: los dashboards consumen conteos y promedios, y el grano viaje era inviable
> a escala completa (~940M de registros). La prueba de no-pérdida (`SUM(viajes)` == filas de los
> facts) se verifica en `notebooks/revision_04_gold.ipynb`.

## Requisitos previos

- **Python 3.12**
- **[uv](https://docs.astral.sh/uv/)** para gestión de dependencias
- **Java (JDK 11+)** requerido por PySpark
- **Windows**: `HADOOP_HOME` apuntando a un directorio `bin/` con `hadoop.dll` y
  `winutils.exe` compatibles (p. ej. de [cdarlint/winutils](https://github.com/cdarlint/winutils)).

## Inicio rápido

```bash
# 1. Instalar dependencias
uv sync

# 2. (Windows) configurar Hadoop para Spark
#    PowerShell:  $env:HADOOP_HOME = "C:\ruta\a\hadoop"
#    bash:        export HADOOP_HOME=/ruta/a/hadoop

# 3. Ejecutar el pipeline en orden
uv run main.py                  # Bronze: descarga zone-lookup + datos de viajes
uv run main.py --profile        # (opcional) Profiling de calidad
uv run main.py --silver         # Silver: limpieza de calidad
uv run main.py --silver schema  # Silver: dimensiones del modelo estrella
uv run main.py --silver load    # Silver: tablas de hechos
uv run main.py --gold           # Gold: marts Power BI + features ML

# 4. Entrenar los modelos (cada uno requiere su feature store del paso anterior)
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
> El orden importa. `--silver schema` debe ejecutarse antes de `--silver load` (los hechos se
> unen contra las dimensiones), y `--gold` lee de `data/silver/star/`: aborta con un mensaje
> claro si la capa silver no está cargada.

> [!TIP]
> **Todas las etapas son idempotentes**: si una corrida se interrumpe, basta relanzar el mismo
> comando y solo se procesa lo que falta. Para forzar el reprocesamiento de un mes, borra su
> salida (p. ej. el directorio en `data/silver/stage/` o el JSON en `data/profiling/`).

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

## Backend de almacenamiento (local o S3)

Todas las capas (`bronze/silver/gold/profiling` + los 3 `audit.parquet`) pueden vivir en el
**filesystem local** (default) o en **S3**, sin mezclar por capa y sin código duplicado entre
backends. Se controla con una sola variable, `STORAGE_BACKEND`, en `.env` (ver `.env.example`):

```bash
cp .env.example .env          # y completar credenciales si se usa S3
uv sync --extra s3            # instala s3fs (cliente S3 de la app; Spark usa hadoop-aws aparte)
```

```dotenv
STORAGE_BACKEND=s3            # local (default) | s3
AWS_ACCESS_KEY_ID=...         # leídas del entorno, nunca de config.yaml
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET=mi-bucket
S3_PREFIX=tlc-pipeline
```

La abstracción vive en `app/utils/storage.py` (`get_root()` devuelve un `Path` local o un `S3Path`
vía `s3fs`); el resto del pipeline compone rutas igual que siempre, solo cambia la raíz. Con backend
local el comportamiento es idéntico byte a byte al de antes de introducir la abstracción.

> [!NOTE]
> El shuffle/spill de Spark (`spark.local.dir`) **siempre** se queda en disco local, incluso con
> `STORAGE_BACKEND=s3`: mandarlo a S3 añadiría latencia de red a una operación ya intensiva en memoria.

## Ejecución con Docker + Airflow

Además del `uv run main.py` local, el pipeline puede orquestarse con **Airflow sobre Docker Compose**
(`docker-compose.yml` + `Dockerfile`): Postgres (metadatos) + webserver + scheduler en `LocalExecutor`,
más un servicio opcional `jupyter` para los notebooks de revisión. Spark corre **embebido** en el
proceso de cada tarea (no hay clúster Spark aparte).

```bash
uv lock                              # una vez, tras añadir los extras s3/jupyter (commitear uv.lock)
docker compose build
docker compose up airflow-init       # inicializa la metadata DB (una vez)
docker compose up                    # levanta webserver + scheduler (+ postgres)
```

Cada fase es su **propio DAG**, encadenados con `TriggerDagRunOperator(wait_for_completion=True)`:

```
dag_01_bronze → dag_02_silver_quality → dag_03_silver_schema → dag_04_silver_load → dag_05_gold → dag_06_profiling
dag_07_gold_ml   # entrenamiento (kmodes/isolation/sarimax), se dispara manualmente desde la UI
```

Todos los DAGs son `BashOperator`s finos que invocan el mismo CLI (`uv run main.py ...`) — no
reimplementan lógica del pipeline. El *profiling* corre al final a propósito (documentación de solo
lectura que no alimenta silver/gold).

> [!IMPORTANT]
> **Despausa los 7 DAGs** (`dag_01`…`dag_07`) en la UI antes de disparar `dag_01_bronze`: la cadena
> `TriggerDagRunOperator` queda encolada para siempre si un DAG río abajo está en pausa.

> [!TIP]
> **Memoria (Windows/Docker Desktop)**: el VM de WSL2 necesita **≥8 GB**. `docker-compose.yml` baja el
> default de Spark a `8g`/`local[8]` (vía `SPARK_DRIVER_MEMORY`/`SPARK_MASTER_CORES`) porque comparte RAM
> con Airflow + Postgres; ajústalo en `.env` según tu VM. El shuffle va a `/tmp/spark-temp` (disco del
> contenedor), no al bind mount de `./data` (I/O lenta vía gRPC-FUSE).

## Estructura del proyecto

```
app/
├── client/download_client.py     # Descargas async (Polars) + auditoría
├── pipeline/
│   ├── bronze.py                  # Etapa Bronze
│   ├── silver.py                  # Silver: SilverCleaner + SilverPipeline
│   ├── star.py                    # Silver: modelo estrella (dims + hechos)
│   └── gold/                      # Capa Gold
│       ├── gold_pipeline.py       # Orquestador (GoldPipeline)
│       ├── mart_builder.py        # Bases: GoldBuilder, TripGrainMart, GoldContext
│       ├── feature_rules/         # Heurísticas reusables (bloques, propina, tarifas)
│       ├── dims/                  # Dimensiones gold enriquecidas
│       ├── marts/                 # 6 marts agregados para Power BI
│       └── ml/                    # 3 feature stores + 3 pipelines de modelos
├── profiling/                     # Profiling de calidad (8 dimensiones)
├── schemas/settings_schema.py     # Validación de config (Pydantic)
└── utils/                         # spark, logger, globals, settings, storage (local/S3)
dags/                              # 7 DAGs de Airflow (uno por fase del pipeline)
notebooks/                         # Revisión por capa (bronze → gold → auditoría)
tests/                             # unit / spark / integration (pytest)
config.yaml                        # Configuración del pipeline
Dockerfile / docker-compose.yml    # Ejecución orquestada con Airflow
.env.example                       # STORAGE_BACKEND, credenciales AWS, bootstrap Airflow
main.py                            # Punto de entrada (CLI)
```

Los notebooks `notebooks/revision_01..05` documentan cada capa: inventario del bronce,
resultados del profiling, integridad silver (`bronce = stage + reject`), granos y
no-pérdida de los marts gold, y la cadena de auditoría completa.

## Tests

```bash
PYTHONPATH=. uv run pytest                         # todos
PYTHONPATH=. uv run pytest -m "not integration"    # rápidos: unit + spark
PYTHONPATH=. uv run pytest -m integration          # lentos: e2e del pipeline completo
```

> [!NOTE]
> Los tests muestrean 50 filas de `data/bronze/{yellow,green,fhv,fhvhv}/2025-01.parquet`,
> por lo que requieren haber ejecutado `uv run main.py` al menos una vez.

## Stack

Python 3.12 gestionado con **uv**. **Polars** para descargas y auditoría; **PySpark** para
profiling, silver y gold (con AQE, codec parquet `zstd` y escrituras coalesceadas);
**scikit-learn / kmodes / statsmodels** para los modelos; **pyarrow** para metadatos de
parquet; **Pydantic** para configuración.

> [!WARNING]
> Los mensajes de log y de usuario están en **español**; el código, identificadores y comentarios
> en **inglés**. La tarifa plana de JFK en `dim_ratecode_theoretical` (heurística de fraude)
> debe verificarse contra la normativa TLC vigente.
