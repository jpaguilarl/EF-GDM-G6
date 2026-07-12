# Referencia de Configuración

El pipeline se configura mediante dos mecanismos:

- **`config.yaml`** — archivo YAML en la raíz del proyecto, validado por
  `app/schemas/settings_schema.py` a través de `app/utils/settings.py`.
- **`.env`** — archivo de variables de entorno (ver `.env.example`) para
  credenciales, recursos Spark y bootstrap de Airflow/Docker. Nunca contiene
  lógica de negocio.

---

## `config.yaml`

```yaml
storage:
  backend: local

datasets:
  years: [2023, 2024, 2025]

gold:
  mode: full
  supply_demand:
    block_minutes: 15
    deficit_threshold: -10
  abc_xyz:
    class_a_pct: 0.80
    class_b_pct: 0.15
    xyz_x_max: 0.2
    xyz_y_max: 0.5
  generosity:
    standard_low: 10
    standard_high: 18
  isolation_fraud:
    contamination: 0.05
    n_estimators: 100
    max_samples: auto
    random_state: 42
    min_rows_per_ratecode: 200
  sarimax:
    order: [1, 1, 1]
    seasonal_order: [1, 1, 1, 24]
    min_rows_per_segment: 1000
    forecast_horizon_hours: 168
  kmodes:
    max_k: 8
    max_sample_per_service: 100000
    n_init: 2
    init_method: Cao
    random_state: 42
```

### `storage` — Backend de almacenamiento

Controla dónde se escriben todas las capas (bronze, silver, gold, profiling)
y los tres archivos de auditoría (`audit.parquet`).

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `backend` | `"local"` / `"s3"` | `"local"` | Sistema de archivos raíz. `"local"` usa el directorio del proyecto; `"s3"` usa un bucket S3 (lee credenciales de variables de entorno, nunca de `config.yaml`). |

Con `backend: s3`, el shuffle/spill de Spark (`spark.local.dir`) permanece
siempre en disco local para evitar latencia de red.

### `datasets` — Datasets a procesar

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `years` | `list[int \| Module]` | `[2023, 2024, 2025]` | Años a expandir en 4 categorías × 12 meses. Cada elemento puede ser un entero (expande todas las categorías y meses) o un objeto con `{category, year, month}` para un único archivo. |

Ejemplo con `Module` explícito:

```yaml
datasets:
  years:
    - category: yellow
      year: 2025
      month: 1
    - category: fhvhv
      year: 2025
      month: 1
```

### `gold` — Capa gold (marts, feature stores y modelos)

#### `gold.mode`

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `mode` | `"full"` / `"incremental"` | `"full"` | `"full"` recalcula todo el histórico; `"incremental"` escribe solo las particiones mensuales faltantes (idempotente, respeta `partitionOverwriteMode=dynamic`). |

#### `gold.supply_demand`

Configura el cálculo del balance oferta-demanda y la detección de déficit.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `block_minutes` | `int` | `15` | Duración de cada bloque horario para agregar viajes (en minutos). |
| `deficit_threshold` | `int` | `-10` | Umbral de viajes negativos para clasificar un bloque como déficit. |

#### `gold.abc_xyz`

Umbrales para la clasificación ABC/XYZ de zonas según volumen y variabilidad.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `class_a_pct` | `float` | `0.80` | Percentil acumulado para clase A (mayor volumen). |
| `class_b_pct` | `float` | `0.15` | Percentil acumulado para clase B (volumen medio). |
| `xyz_x_max` | `float` | `0.2` | Coeficiente de variación máximo para clase X (baja variabilidad). |
| `xyz_y_max` | `float` | `0.5` | Coeficiente de variación máximo para clase Y (variabilidad media). |

#### `gold.generosity`

Umbrales para clasificar la propina como categoría nominal.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `standard_low` | `float` | `10.0` | Porcentaje de propina por debajo del cual se considera "Baja". |
| `standard_high` | `float` | `18.0` | Porcentaje de propina por encima del cual se considera "Alta". |

#### `gold.isolation_fraud`

Hiperparámetros del modelo Isolation Forest para detección de fraude.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `contamination` | `float` | `0.05` | Proporción esperada de anomalías en los datos. |
| `n_estimators` | `int` | `100` | Número de árboles de aislamiento. |
| `max_samples` | `str` | `"auto"` | Número de muestras por árbol (`"auto"` usa `min(256, n_samples)`). |
| `random_state` | `int` | `42` | Semilla para reproducibilidad. |
| `min_rows_per_ratecode` | `int` | `200` | Mínimo de filas requeridas por RatecodeID para entrenar un modelo. |

#### `gold.sarimax`

Hiperparámetros del modelo SARIMAX para pronóstico de viajes.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `order` | `list[int]` | `[1, 1, 1]` | Orden ARIMA (p, d, q). |
| `seasonal_order` | `list[int]` | `[1, 1, 1, 24]` | Orden estacional (P, D, Q, s); periodo 24 para ciclo diario. |
| `min_rows_per_segment` | `int` | `1000` | Mínimo de filas por segmento (borough × service_id) para entrenar. |
| `forecast_horizon_hours` | `int` | `168` | Horizonte de pronóstico en horas (default 7 días). |

#### `gold.kmodes`

Hiperparámetros del modelo K-Modes para clustering de perfiles de viaje.

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `max_k` | `int` | `8` | Número máximo de clusters a evaluar (codo + silueta). |
| `max_sample_per_service` | `int` | `100000` | Máximo de filas muestreadas por servicio (fhvhv se muestrea al 5%). |
| `n_init` | `int` | `2` | Número de inicializaciones del algoritmo (elige la de menor costo). |
| `init_method` | `"Cao"` / `"Huang"` | `"Cao"` | Método de inicialización de centroides. |
| `random_state` | `int` | `42` | Semilla para reproducibilidad. |

---

## `profiling.rules` — Reglas de calidad (futuras claves configurables)

Actualmente estas reglas están hardcodeadas en `app/profiling/rules/`. Se
refactorizarán para ser configurables desde `config.yaml` bajo esta sección.

Estructura prevista:

```yaml
profiling:
  rules:
    nullability:
      fhv: ["SR_Flag"]
      fhvhv: ["originating_base_num", "on_scene_datetime", "shared_request_flag",
              "shared_match_flag", "access_a_ride_flag", "wav_request_flag",
              "wav_match_flag"]
      yellow: ["airport_fee", "congestion_surcharge", "cbd_congestion_fee"]
      green: ["ehail_fee", "congestion_surcharge", "cbd_congestion_fee"]

    reasonableness_ranges:
      yellow:
        passenger_count: [0, 9]
        trip_distance: [0, 500]
        fare_amount: [-200, 5000]
        total_amount: [-200, 5000]
        tip_amount: [0, 2000]
        tolls_amount: [0, 100]
        improvement_surcharge: [0, 5]
        congestion_surcharge: [0, 5]
        cbd_congestion_fee: [0, 5]
        airport_fee: [0, 5]
        mta_tax: [0, 5]
        extra: [-5, 20]
      green:
        passenger_count: [0, 9]
        trip_distance: [0, 500]
        fare_amount: [-200, 5000]
        total_amount: [-200, 5000]
        tip_amount: [0, 2000]
        tolls_amount: [0, 100]
        ehail_fee: [0, 10]
        improvement_surcharge: [0, 5]
        congestion_surcharge: [0, 5]
        cbd_congestion_fee: [0, 5]
        mta_tax: [0, 5]
        extra: [-5, 20]
      fhv:
        SR_Flag: [0, 1]
      fhvhv:
        trip_miles: [0, 500]
        trip_time: [0, 86400]
        base_passenger_fare: [-200, 5000]
        driver_pay: [-200, 5000]
        tips: [0, 2000]
        tolls: [0, 100]
        bcf: [0, 5]
        sales_tax: [0, 5]
        congestion_surcharge: [0, 5]
        cbd_congestion_fee: [0, 5]
        airport_fee: [0, 5]

    amount_formulas:
      yellow:
        total: "total_amount"
        components: ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                     "improvement_surcharge", "congestion_surcharge", "airport_fee",
                     "cbd_congestion_fee"]
      green:
        total: "total_amount"
        components: ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                     "ehail_fee", "improvement_surcharge", "congestion_surcharge",
                     "cbd_congestion_fee"]
      fhvhv:
        total: "driver_pay"
        components: ["base_passenger_fare", "tolls", "bcf", "sales_tax",
                     "congestion_surcharge", "airport_fee", "tips"]

    max_trip_duration_minutes: 1440
    amount_tolerance: 0.02
```

### `nullability`

Define qué columnas de cada categoría se consideran opcionales (pueden contener valores nulos).
Cualquier columna que **NO** esté listada aquí se asume como requerida: la presencia de un nulo
en dicha columna causará que el registro sea rechazado en la capa Silver (`SilverCleaner`).
En el profiling, evita falsos positivos en la dimensión de completitud.

### `reasonableness_ranges`

Define los rangos aceptables `[mínimo, máximo]` para variables numéricas.
Utilizado únicamente por la dimensión de calidad *reasonableness* en la etapa de profiling.
**Nota:** Estos rangos ya no se aplican como filtros ni se truncan (clamping) en la capa Silver
para evitar alteración artificial de datos reales.

### `amount_formulas`

Expresa cómo se compone el importe total teórico a partir de sus componentes individuales.
Utilizado en la dimensión de calidad *accuracy* de la etapa de profiling para evaluar
discrepancias.
**Nota:** La capa Silver ya no recalcula ni sobreescribe el total reportado en origen.

- `total`: nombre de la columna que contiene el importe total.
- `components`: lista de columnas que se suman para obtener el total.

### `max_trip_duration_minutes`

Duración máxima razonable de un viaje en minutos. Viajes más largos se
consideran irrazonables en el profiling. Ya no se utiliza para rechazar registros en Silver.

### `amount_tolerance`

Tolerancia absoluta (en USD) para la comparación entre el total declarado
y la suma de componentes. Diferencias menores o iguales a este valor se
consideran precisas en el profiling.

---

## `gold.feature_rules` — Reglas de negocio doradas (futuras claves configurables)

Actualmente hardcodeadas en `app/pipeline/gold/feature_rules/`. Estructura
prevista:

```yaml
gold:
  feature_rules:
    time_blocks:
      bloque_horario:
        Madrugada: [0, 5]
        Punta Mañana: [6, 9]
        Mediodía: [10, 15]
        Punta Tarde: [16, 19]
        Noche: [20, 23]
      franja_horaria:
        Madrugada: [0, 5]
        Mañana: [6, 11]
        Tarde: [12, 18]
        Noche: [19, 23]
      dia_categoria:
        laborable: [1, 5]
        fin_de_semana: [6, 7]

    generosity:
      sin_propina_max: 0
      baja_max: 10.0
      estandar_max: 18.0
      # Por encima de estandar_max se considera "Alta"

    ratecode_tariff:
      flat_fares:
        2: {2023: 70.0, 2024: 70.0, 2025: 70.0}
      ratecode_names:
        1: "Standard rate"
        2: "JFK"
        3: "Newark"
        4: "Nassau/Westchester"
        5: "Negotiated fare"
        6: "Group ride"
        99: "Desconocido"
      jfk_fare_tolerance: 15.0
      max_plausible_speed_mph: 80.0
      max_cost_per_mile: 30.0

    passenger_groups:
      solo_max: 1
      pareja_max: 2
      grupo_pequeno_max: 4
      # Por encima de grupo_pequeno_max se considera "Grupo grande"
```

### `time_blocks`

Define los bloques horarios y categorías de día usados por los marts
y feature stores. Cada bloque tiene un rango `[inicio, fin]` de hora
(0-23). Coincide con la convención ISO para día de semana (1=Lunes…7=Domingo).

| Regla | Propósito |
|---|---|
| `bloque_horario` | 5 bloques operativos para el mart de volumen y demanda (D1.1). |
| `franja_horaria` | 4 buckets amplios para K-Modes (D3.2). |
| `dia_categoria` | División laborable / fin de semana. |

### `generosity`

Umbrales para clasificar el porcentaje de propina en categorías nominales.
Un porcentaje nulo (propina en efectivo no registrada) permanece nulo.

| Desde | Hasta | Categoría |
|---|---|---|
| — | `sin_propina_max` | Sin Propina |
| `> sin_propina_max` | `baja_max` | Baja |
| `> baja_max` | `estandar_max` | Estándar |
| `> estandar_max` | — | Alta |

### `ratecode_tariff`

Tarifas teóricas y heurísticas de anomalía para la dimensión de fraude (D3.3).

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `flat_fares` | `dict[int, dict[int, float]]` | `{2: {2023: 70.0, 2024: 70.0, 2025: 70.0}}` | Tarifas planas por `ratecode_id` y año fiscal (USD). `2` = JFK. |
| `ratecode_names` | `dict[int, str]` | *(ver arriba)* | Nombres descriptivos de cada RatecodeID. |
| `jfk_fare_tolerance` | `float` | `15.0` | Desviación tolerada (USD) frente a la tarifa plana de JFK. |
| `max_plausible_speed_mph` | `float` | `80.0` | Velocidad máxima físicamente plausible en ciudad (mph). |
| `max_cost_per_mile` | `float` | `30.0` | Costo por distancia máximo típico (USD/milla). |

La función `is_anomaly_candidate` usa estas reglas para generar candidatos
a anomalía (no el score final del modelo Isolation Forest).

### `passenger_groups`

Binning del número de pasajeros en categorías nominales para K-Modes.

| Rango | Categoría |
|---|---|
| `NULL` | Desconocido |
| 1 | Solo |
| 2 | Pareja |
| 3 – `grupo_pequeno_max` | Grupo pequeño |
| `> grupo_pequeno_max` | Grupo grande |

---

## Variables de entorno (`.env`)

### AWS / S3 (solo si `storage.backend = s3`)

Leídas directamente del entorno por `s3fs` y `SparkClient`. Nunca se
hardcodean en `config.yaml`.

| Variable | Default | Descripción |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | — | Clave de acceso AWS. |
| `AWS_SECRET_ACCESS_KEY` | — | Clave secreta AWS. |
| `AWS_REGION` | `us-east-1` | Región AWS. |
| `S3_BUCKET` | — | Nombre del bucket S3. |
| `S3_PREFIX` | `tlc-pipeline` | Prefijo (carpeta virtual) dentro del bucket. |

### Recursos Spark (override de valores calibrados)

Aplican en todos los entornos (bare-metal y Docker). Si no se definen,
se usan los defaults del perfil "balanceado" en `app/utils/spark.py`.

| Variable | Default bare-metal | Default Docker | Descripción |
|---|---|---|---|
| `SPARK_DRIVER_MEMORY` | `10g` | `8g` | Heap de la JVM de Spark (driver y executor son la misma JVM en local mode). |
| `SPARK_MASTER_CORES` | `10` | `8` | Núcleos paralelos (`local[N]`). No subir sin subir el heap proporcionalmente. |
| `SPARK_LOCAL_DIR` | `data/.spark_temp` | `/tmp/spark-temp` | Directorio de shuffle/spill. Siempre en disco local, incluso con backend S3. |

### Airflow (Docker Compose bootstrap)

Usadas por `docker-compose.yml` para configurar los servicios de Airflow.

| Variable | Default | Descripción |
|---|---|---|
| `AIRFLOW_UID` | `50000` | UID del usuario `airflow` dentro del contenedor. En Linux, usar `id -u` del host. |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Ejecutor de Airflow. No cambiar (el pipeline no está diseñado para Celery). |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | Cadena de conexión a la base de metadatos. |
| `AIRFLOW__CORE__FERNET_KEY` | — | Clave Fernet para serialización de DAGs. |
| `_AIRFLOW_WWW_USER_USERNAME` | `admin` | Usuario de la interfaz web de Airflow. |
| `_AIRFLOW_WWW_USER_PASSWORD` | `admin` | Contraseña de la interfaz web de Airflow. |

---

## Ejemplo completo

### `config.yaml`

```yaml
storage:
  backend: s3

datasets:
  years: [2023, 2024, 2025]

gold:
  mode: full
  supply_demand:
    block_minutes: 30
    deficit_threshold: -5
  abc_xyz:
    class_a_pct: 0.75
    class_b_pct: 0.20
    xyz_x_max: 0.15
    xyz_y_max: 0.45
  generosity:
    standard_low: 8.0
    standard_high: 20.0
  isolation_fraud:
    contamination: 0.03
    n_estimators: 200
    random_state: 123
  sarimax:
    order: [2, 1, 2]
    seasonal_order: [1, 1, 1, 24]
  kmodes:
    max_k: 10
    max_sample_per_service: 50000
    init_method: Huang
```

### `.env`

```dotenv
STORAGE_BACKEND=s3
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET=my-tlc-data
S3_PREFIX=pipeline

SPARK_DRIVER_MEMORY=12g
SPARK_MASTER_CORES=12
```

---

## Referencia rápida

### Secciones obligatorias de `config.yaml`

| Sección | ¿Obligatoria? | Default |
|---|---|---|
| `storage` | Sí | — (campo `backend` default: `"local"`) |
| `datasets` | Sí | — |
| `gold` | No | Todos los sub-modelos usan sus defaults |

### Convenciones

- Los mensajes al usuario/log están en **español**; el código, identificadores
  y nombres de campo están en **inglés**.
- Los años en `datasets.years` son enteros planos (no strings).
- Los valores monetarios están en **USD**.
- Las semillas (`random_state`) garantizan reproducibilidad entre ejecuciones.
