# Cómo instalar y configurar el pipeline EF-GDM-G6

Guía de instalación completa para ejecutar el pipeline ETL de datos TLC de Nueva York
en bare-metal o con Docker + Airflow, incluyendo la capa serving y backend S3.

---

## 1. Prerrequisitos

| Recurso | Mínimo | Recomendado |
|---|---|---|
| **Python** | 3.12 | 3.12.x |
| **Java JDK** | 11 | 17 (obligatorio para Docker) |
| **uv** | ≥0.5 | Última estable |
| **RAM** | 8 GB | 16 GB (32 GB con S3) |
| **Disco** | 20 GB libres | 100 GB (datos completos 2023–2025) |
| **Docker** (opcional) | 24.0+ | Docker Compose v2 |

Instala `uv` si no lo tienes:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verifica Python y Java:

```bash
python --version   # ≥ 3.12
java -version      # ≥ 11 (17 para Docker)
```

> **Windows:** Además necesitas `HADOOP_HOME` apuntando a un directorio con
> `hadoop.dll` y `winutils.exe` (PySpark los requiere en Windows). Coloca los
> binarios en `lib/` (está en `.gitignore`).

---

## 2. Instalación básica (bare-metal)

### 2.1 Clonar el repositorio

```bash
git clone <repo-url>
cd ef-gdm-g6
```

### 2.2 Crear archivo de entorno

```bash
cp .env.example .env
```

Edita `.env` solo si vas a usar S3 (sección 4). Para almacenamiento local no
necesitas tocar nada.

### 2.3 Sincronizar dependencias

```bash
uv sync
```

Esto crea un `.venv` con todas las dependencias base: `httpx`, `polars`,
`pyspark`, `pandas`, `scikit-learn`, `kmodes`, `statsmodels`, etc.

Para verificar que todo quedó correcto:

```bash
uv run python -c "import pyspark; print('PySpark', pyspark.__version__)"
uv run python -c "import polars; print('Polars', polars.__version__)"
```

### 2.4 Verificar la instalación

Ejecuta la etapa bronze (descarga datos de ejemplo):

```bash
uv run main.py
```

Esto descarga el zone-lookup y los datos de 2023–2025 para las cuatro
categorías (yellow, green, fhv, fhvhv) en `data/bronze/`.

Para una prueba más rápida — solo un mes:

```bash
# Editar config.yaml temporalmente:
# datasets:
#   years: [2025]
#   modules:
#     - category: yellow
#       year: 2025
#       month: 1
```

### 2.5 Ejecutar los tests

```bash
PYTHONPATH=. uv run pytest -m "not integration"
```

Esto corre tests unitarios y de Spark (saltando el e2e completo, que requiere
todos los datos).

---

## 3. Configuración del pipeline

El archivo `config.yaml` define años, categorías, y parámetros de cada etapa:

```yaml
storage:
  backend: "local"          # o "s3"

datasets:
  years: [2023, 2024, 2025] # años a procesar; o usar modules para meses sueltos

gold:
  mode: full                # o "incremental"
  supply_demand:
    block_minutes: 15       # ventana de agregación
    deficit_threshold: -10
  # ... más parámetros gold, ML, speed, serving
```

Consulta la referencia completa en [CONFIG.md](CONFIG.md).

---

## 4. Backend S3 (opcional)

### 4.1 Instalar el extra S3

```bash
uv sync --extra s3
```

Esto añade `s3fs` (fsspec) para que Polars y pandas lean/escriban en S3.
PySpark usa su propio conector Hadoop (descargado vía Maven, no necesita este
extra).

### 4.2 Configurar credenciales

Edita `.env`:

```env
STORAGE_BACKEND=s3
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET=mi-bucket
S3_PREFIX=tlc-pipeline
```

Las credenciales se leen del entorno — nunca se hardcodean en `config.yaml`
ni en el código.

### 4.3 Verificar acceso S3

```bash
uv run python -c "
from app.utils.storage import get_root
r = get_root()
print('Root:', r)
print('Existe:', r.exists())
"
```

Cuando `STORAGE_BACKEND=s3`, `get_root()` retorna un `S3Path` (s3fs) y todas
las escrituras van a `s3://<bucket>/<prefix>/...`.

> **Nota:** Spark usa `s3a://` en lugar de `s3://`. El helper
> `storage.for_spark(path)` hace la conversión automáticamente. El directorio
> de shuffle/spill siempre queda en disco local, incluso con backend S3.

---

## 5. Serving layer + Speed + Redis (opcional)

### 5.1 Instalar el extra serving

```bash
uv sync --extra serving
```

Esto añade `fastapi`, `uvicorn`, `redis`, `xxhash`, `sse-starlette`.

### 5.2 Iniciar Redis

```bash
# Con Docker
docker run -d --name redis -p 6379:6379 redis:7-alpine

# O directamente si tienes redis-server instalado
redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
```

### 5.3 Iniciar el serving layer

```bash
uv run main.py --serve
```

Esto arranca FastAPI en `http://localhost:8000` con:
- Endpoints GET históricos (lectura lazy desde gold marts vía Polars)
- Stream SSE en tiempo real (fusión batch + Redis)

Para probar:

```bash
curl http://localhost:8000/docs     # Swagger UI
curl http://localhost:8000/api/v1/health
```

### 5.4 Speed engine (ingesta en tiempo real)

```bash
uv run main.py --speed
```

Lee líneas JSON desde stdin, las limpia, las agrega en Redis y calcula
fraud score vía Isolation Forest.

---

## 6. Despliegue con Docker + Airflow

Esta sección despliega el pipeline completo orquestado con Airflow
(LocalExecutor) en Docker Compose. Spark corre embebido en cada tarea — no
hay un clúster Spark separado.

### 6.1 Prerrequisitos

- Docker Engine ≥ 24.0
- Docker Compose v2 (plugin `docker compose`)
- Al menos 8 GB de RAM asignados al VM de Docker (WSL2 en Windows;
  configura con `.wslconfig`)

### 6.2 Preparar el lockfile

Si `uv.lock` no existe o está desactualizado, regenerarlo en el host:

```bash
uv lock
```

Commitear `uv.lock` — el `Dockerfile` ejecuta `uv sync --frozen` y falla
si el lockfile falta.

### 6.3 Configurar `.env`

```bash
cp .env.example .env
```

Ajusta `AIRFLOW_UID` en Linux:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

En Windows/Docker Desktop, dejar el default `50000`.

Si usas S3, completa las variables `AWS_*` y `STORAGE_BACKEND=s3`.

### 6.4 Construir la imagen

```bash
docker compose build
```

Esto construye la imagen `tlc-airflow-base` sobre
`apache/airflow:2.10.5-python3.12`, añadiendo JDK 17 y `uv`. La primera
vez puede tomar varios minutos porque descarga dependencias Maven de Spark
(≈200 MB en JARs).

### 6.5 Inicializar Airflow

```bash
docker compose up airflow-init
```

Esto ejecuta `airflow db migrate` y crea el usuario admin. Solo necesario
la primera vez o tras eliminar el volumen de Postgres.

### 6.6 Iniciar servicios

```bash
docker compose up -d
```

Servicios que arrancan:

| Servicio | Puerto | Rol |
|---|---|---|
| `postgres` | — | Metadata DB de Airflow |
| `airflow-webserver` | `8080` | UI de Airflow |
| `airflow-scheduler` | — | Ejecuta los DAGs |
| `redis` | `6379` | Capa speed (opcional) |
| `serving` | `8000` | FastAPI (opcional) |
| `jupyter` | `8888` | Jupyter Lab (opcional) |

### 6.7 Despausar los DAGs

1. Abre http://localhost:8080 (usuario: `admin`, contraseña: `admin`)
2. Despausa **los 7 DAGs**: `dag_01_bronze` … `dag_07_gold_ml`
3. Gatilla `dag_01_bronze` desde la UI (botón ▶)

La cadena de ejecución es automática:
`bronze → silver_quality → silver_schema → silver_load → gold → profiling`
(`dag_07_gold_ml` se lanza manualmente desde la UI).

> **Importante:** La cadena usa `TriggerDagRunOperator(wait_for_completion=True)`.
> Si un DAG downstream está pausado, la cadena se queda encolada
> indefinidamente — por eso deben despausarse todos antes de gatillar.

### 6.8 Servicio Jupyter (opcional)

Accede a http://localhost:8888 para explorar los notebooks de revisión
en `notebooks/`. Sin token.

---

## 7. Solución de problemas

### Spark se queda sin memoria (OOM)

- Aumenta `SPARK_DRIVER_MEMORY` en `.env` o directamente:
  ```bash
  SPARK_DRIVER_MEMORY=12g uv run main.py --silver
  ```
- Reduce `spark.sql.shuffle.partitions` (default 128) editando
  `app/utils/spark.py` o en el entorno.
- En Docker, asegúrate de que el VM de WSL2 tenga suficiente RAM
  (archivo `.wslconfig`: `memory=12GB`).

### Shuffle lento o disco lleno

Spark escribe shuffle/spill en `SPARK_LOCAL_DIR` (`/tmp/spark-temp` en
Docker, `data/.spark_temp` en bare-metal). Verifica que ese directorio
tenga al menos 20 GB libres.

En Docker, el shuffle va a disco del contenedor (no al bind mount de
`./data`), que es más rápido.

### Error de credenciales AWS

```
java.lang.RuntimeException: java.lang.IllegalArgumentException:
  Cannot access to S3 bucket
```

Verifica que `.env` tenga `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
y que el bucket exista. Spark lee estas vars del entorno del proceso.

### Puerto 8080 o 6379 ocupados

Cambia los puertos en `docker-compose.yml` o detén el servicio
conflictivo:

```bash
# Ejemplo: cambiar el puerto de Airflow a 8081
# En docker-compose.yml: "8081:8080"
```

### `uv sync --frozen` falla en Docker

Significa que `uv.lock` no está actualizado respecto a `pyproject.toml`.
Corre `uv lock` en el host y commitea el lockfile.

### No se ven los datos después de correr el pipeline en Docker

Los datos se escriben en `./data/` del host (bind mount). Verifica que
el directorio exista:

```bash
ls -la data/bronze/
```

Si está vacío, revisa los logs del scheduler en la UI de Airflow
(http://localhost:8080 → Browse → Task Instances).

---

## Resumen de comandos

| Qué hacer | Comando |
|---|---|
| Instalar dependencias base | `uv sync` |
| Instalar con S3 | `uv sync --extra s3` |
| Instalar con serving | `uv sync --extra serving` |
| Descargar datos | `uv run main.py` |
| Profilear calidad | `uv run main.py --profile` |
| Pipeline completo | `uv run main.py --silver && uv run main.py --silver schema && uv run main.py --silver load && uv run main.py --gold` |
| Entrenar ML | `uv run main.py --gold-ml isolation` |
| Servir API | `uv run main.py --serve` |
| Tests unitarios | `PYTHONPATH=. uv run pytest -m "not integration"` |
| Construir Docker | `docker compose build` |
| Inicializar Airflow | `docker compose up airflow-init` |
| Arrancar todo | `docker compose up -d` |
| Ver logs | `docker compose logs -f airflow-scheduler` |
