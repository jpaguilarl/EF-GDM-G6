# Pipeline ETL + Calidad de Datos — NYC TLC

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/packaging-uv-DE5FE9?logo=uv&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?logo=apachespark&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.42-CD792C?logo=polars&logoColor=white)

Pipeline de datos para los registros de viajes de la **NYC Taxi & Limousine Commission (TLC)**,
construido como una **arquitectura medallón** (bronze → silver → gold) con una etapa
independiente de *profiling* de calidad. Descarga los datos públicos, evalúa su calidad en
8 dimensiones, los limpia hacia un modelo estrella y finalmente produce **marts para Power BI**
y un **feature store para modelos de IA**.

Las cuatro categorías TLC: **green**, **yellow**, **fhv**, **fhvhv**.

## Características

- **Descarga asíncrona** de parquet desde el CDN oficial de la TLC, con trazabilidad (audit trail).
- **Profiling de calidad** en 8 dimensiones (exactitud, completitud, consistencia, integridad,
  razonabilidad, oportunidad, unicidad, validez) con reporte JSON + `index.html`.
- **Capa silver**: limpieza en dos fases (rechazo + corrección) y **modelo estrella**
  (dimensiones + hechos por categoría).
- **Capa gold**: 6 *marts* denormalizados para Power BI + 3 tablas de *features* para ML,
  más dimensiones enriquecidas.
- **Auditoría encadenada** entre capas (`bronze_audit_id → silver_audit_id → gold_audit_id`).
- **Configurable** vía `config.yaml` (rango de datos, parámetros de la capa gold).

## Arquitectura

```
            descarga            profiling            limpieza + estrella         marts BI + features ML
  TLC CDN ───────────► BRONZE ───────────► (calidad) ───────────► SILVER ───────────► GOLD
                       parquet     JSON/HTML            stage → star (dims+facts)   marts/ + ml/ + dims/
```

| Etapa | Entrada | Salida |
|---|---|---|
| **Bronze** | TLC CDN | `data/bronze/{category}/{year}-{month}.parquet` |
| **Profiling** | `data/bronze/` | `data/profiling/**.json` + `index.html` |
| **Silver — quality** | `data/bronze/` | `data/silver/stage/` (+ `reject/`) |
| **Silver — schema** | zone-lookup | `data/silver/star/dims/` |
| **Silver — load** | `data/silver/stage/` | `data/silver/star/facts/` |
| **Gold** | `data/silver/star/` | `data/gold/{marts,ml,dims}/` |

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
| Predictivo | Predicción de viajes (ARIMA) | `ml/ml_feat_arima_trips` |
| Predictivo | Clustering de perfiles (K-Modes) | `ml/ml_feat_kmodes_trips` |
| Predictivo | Detección de fraude en taxímetros | `ml/ml_feat_isolation_fraud` |

> [!NOTE]
> El feature store **entrega** las variables listas para entrenar; el entrenamiento de
> ARIMA / K-Modes / Isolation Forest se realiza fuera del pipeline (sin dependencias extra).

## Requisitos previos

- **Python 3.12**
- **[uv](https://docs.astral.sh/uv/)** para gestión de dependencias
- **Java (JDK 11+)** requerido por PySpark
- **Windows**: `HADOOP_HOME` apuntando a un directorio con `hadoop.dll`/`winutils.exe`
  (incluidos en este repo en `lib/hadoop/`).

## Inicio rápido

```bash
# 1. Instalar dependencias
uv sync

# 2. (Windows) configurar Hadoop para Spark
#    PowerShell:  $env:HADOOP_HOME = "$PWD\lib\hadoop"
#    bash:        export HADOOP_HOME="$PWD/lib/hadoop"

# 3. Ejecutar el pipeline en orden
uv run main.py                  # Bronze: descarga zone-lookup + datos de viajes
uv run main.py --profile        # (opcional) Profiling de calidad
uv run main.py --silver         # Silver: limpieza de calidad
uv run main.py --silver schema  # Silver: dimensiones del modelo estrella
uv run main.py --silver load    # Silver: tablas de hechos
uv run main.py --gold           # Gold: marts Power BI + features ML
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

## Configuración

`config.yaml` controla qué datos se procesan y los parámetros de la capa gold:

```yaml
datasets:
  years:
    - category: fhvhv      # un Module: una categoría/año concreto
      year: 2025
      month: 1
    # o un año entero (expande a 4 categorías × 12 meses):
    # - 2024

gold:
  supply_demand:
    block_minutes: 15      # ventana temporal del análisis oferta/demanda
    deficit_threshold: -10
  abc_xyz:
    class_a_pct: 0.80      # cortes de Pareto ABC
    class_b_pct: 0.15
    xyz_x_max: 0.2         # cortes del coeficiente de variación XYZ
    xyz_y_max: 0.5
  generosity:
    standard_low: 10       # umbrales (%) de categoría de propina
    standard_high: 18
```

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
│       ├── marts/                 # 6 marts para Power BI
│       └── ml/                    # 3 feature stores para IA
├── profiling/                     # Profiling de calidad (8 dimensiones)
├── schemas/settings_schema.py     # Validación de config (Pydantic)
└── utils/                         # spark, logger, globals, settings
config.yaml                        # Configuración del pipeline
main.py                            # Punto de entrada (CLI)
```

## Stack

Python 3.12 gestionado con **uv**. **Polars** para descargas y auditoría; **PySpark** para
profiling, silver y gold; **pyarrow** para metadatos de parquet; **Pydantic** para configuración.

> [!WARNING]
> Los mensajes de log y de usuario están en **español**; el código, identificadores y comentarios
> en **inglés**. La tarifa plana de JFK en `dim_ratecode_theoretical` (heurística de fraude)
> debe verificarse contra la normativa TLC vigente.
