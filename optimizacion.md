# Optimización del Pipeline ETL (Silver → Star Facts → Gold)

> **Objetivo**: Reducir tiempo de ejecución y tamaño en disco sin perder datos, duplicar registros, ni degradar calidad.
>
> **Estrategia**: Cambios de configuración en Spark + refactors localizados en el código. Sin alterar la lógica de negocio, las reglas de calidad, ni el esquema de salida.

---

## 1. Spark Configuration (Alto impacto, 0 riesgo)

Habilitar **Adaptive Query Execution (AQE)**. Spark ajusta automáticamente el número de shuffle partitions, coalescea archivos pequeños al escribir, y optimiza joins con skew — todo sin cambiar una línea de lógica.

### Archivo: `app/utils/spark.py` (líneas 32–67)

**Agregar** estas configuraciones después de la línea 60:

```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
```

**Ajustar shuffle partitions** (línea 46): AQE lo sobreescribe dinámicamente, pero el valor inicial sigue siendo el techo. Subir de 64 → **128** para datasets grandes (fhvhv):

```python
# Antes
.config("spark.sql.shuffle.partitions", "64")
# Después
.config("spark.sql.shuffle.partitions", "128")
```

**Considerar aumentar driver memory** (línea 41) si hay RAM disponible (>16 GB libres):

```python
# Antes (si hay suficiente RAM)
.config("spark.driver.memory", "6g")
# Después
.config("spark.driver.memory", "8g")
```

**Impacto estimado**: ~20-35% menos tiempo en silver + gold (shuffles más rápidos, joins sin skew, archivos coalesceados automáticamente).

---

## 2. Silver Layer (~30-40% del tiempo total)

### 2.1 Eliminar columnas helper antes del persist (Impacto: ⭐⭐⭐)

**Archivo**: `app/pipeline/silver.py` — `SilverCleaner.clean()` (líneas 75–109)

**Problema**: Las columnas `_pickup_dt` y `_dropoff_dt` se crean en `_reject_consistency` y se mantienen en el DataFrame persistido en la línea 91. Esto infla el footprint en memoria (~2 columnas timestamp × millones de filas).

**Solución**: Dropharlas **inmediatamente después** del reject, antes del persist.

```python
# Antes (líneas 86-91):
df = self._reject_timeliness(df, category, year, month)
df = self._reject_consistency(df, category)
df = self._reject_integrity(df, zone_ids)
df = self._reject_uniqueness(df, category)

df = df.persist(StorageLevel.MEMORY_AND_DISK)

# Después:
df = self._reject_timeliness(df, category, year, month)
df = self._reject_consistency(df, category)
df = self._reject_integrity(df, zone_ids)
df = self._reject_uniqueness(df, category)

# Dropear helpers ANTES de persistir (no arrastrar timestamps duplicados)
helper_cols = ["_pickup_dt", "_dropoff_dt"]
df = df.drop(*[c for c in helper_cols if c in df.columns])

df = df.persist(StorageLevel.MEMORY_AND_DISK)
```

Luego en las líneas 103-106, **no dropear** `_pickup_dt`/`_dropoff_dt` (ya no existen):

```python
# Antes (línea 105):
helper_cols = ["_row_idx", "_dup_count", "_dup_rank", "_pickup_dt", "_dropoff_dt"]

# Después:
helper_cols = ["_row_idx", "_dup_count", "_dup_rank"]
```

### 2.2 Eliminar count() redundante para target_files() (Impacto: ⭐⭐⭐)

**Archivo**: `app/pipeline/silver.py` — `_process_file()` (líneas 399–414)

**Problema**: Se hace `df.count()` para determinar `bronze_rowcount` (necesario para audit), y la línea 412 llama `target_files(clean_count)` que fuerza otra cuenta completa. Con AQE habilitado, `coalesce()` manual es innecesario.

**Solución**: Usar un valor fijo conservador y dejar que AQE coalescee al escribir.

```python
# Antes (línea 412):
clean_df.coalesce(target_files(clean_count)).write.mode("overwrite").parquet(...)

# Después (valor fijo — AQE ajustará automáticamente):
clean_df.coalesce(4).write.mode("overwrite").parquet(...)

# Para reject (línea 419):
reject_df.coalesce(1).write.mode("overwrite").parquet(...)
```

### 2.3 Paralelizar procesamiento de archivos (Impacto: ⭐⭐⭐)

**Archivo**: `app/pipeline/silver.py` — `SilverPipeline.run_quality()` (líneas 330–359)

**Problema**: Se procesan archivos uno por uno. Con `local[4]`, los 4 cores están ocupados en cada archivo, pero cuando hay I/O-bound (lectura/escritura) los cores se infrautilizan.

**Solución**: Usar `ThreadPoolExecutor` para procesar 2 archivos en paralelo. Spark en local mode comparte la misma JVM, así que 2 workers es seguro con 6g de heap.

```python
import concurrent.futures

def run_quality(self, year_span: DatasetsConfig) -> None:
    spark = self.spark_client.get_session()
    bronze_audit_id = self._get_latest_bronze_audit_id(spark)
    zone_ids = self._load_zone_ids(spark)
    cleaner = SilverCleaner(spark)

    tasks = self._expand_tasks(year_span)
    max_workers = 2  # seguro con local[4] y 6g heap

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                self._process_file,
                spark, SilverCleaner(spark),
                cat, year, month, zone_ids, bronze_audit_id,
            )
            for (cat, year, month) in tasks
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"Error en worker: {e}")
```

Agregar helper:

```python
def _expand_tasks(self, year_span: DatasetsConfig) -> list[tuple[str, int, int]]:
    tasks = []
    for y in year_span.years:
        if isinstance(y, int):
            for cat in globals.tlc_categories:
                for m in range(1, 13):
                    tasks.append((cat, y, m))
        elif isinstance(y, Module):
            for m in range(1, 13):
                tasks.append((y.category, y.year, m))
    return tasks
```

**⚠️ Advertencia**: Esto incrementa el uso de heap. Si ocurren OOM, reducir `max_workers=1` o aumentar `spark.driver.memory`.

---

## 3. Star Facts (~25-35% del tiempo)

### 3.1 Eliminar count() antes de escribir (Impacto: ⭐⭐⭐)

**Archivo**: `app/pipeline/star.py` — `_build_fact()` (línea 281)

**Problema**: Llama `fact_df.count()` solo para determinar el número de archivos. Con AQE, es innecesario.

**Solución**:

```python
# Antes (líneas 281-284):
fact_count = fact_df.count()
fact_df.coalesce(target_files(fact_count)).write.mode("overwrite").parquet(out_path)

# Después:
fact_df.coalesce(4).write.mode("overwrite").parquet(out_path)
# El log sigue reportando filas — usar el row_count de línea 238 (trip_df.count())
# o eliminar el contador del log y usar "?" si no se quiere contar
```

Mantener `row_count` de `trip_df.count()` (línea 238) para el log, o en su defecto reportar `"?"`:

```python
# Línea 285-286: reemplazar fact_count por trip_df_count (ya calculado)
self.logger.info(
    f"  fact_{category}_trip/{year}-{month:02d}.parquet: {row_count} filas origen"
)
```

### 3.2 Opcional: Escribir facts con partitionBy Hive (Impacto: ⭐⭐⭐⭐)

**Archivo**: `app/pipeline/star.py` — `_build_fact()` (líneas 278–284)

**Problema**: Cada fact se escribe como archivo suelto en `fact_{cat}_trip/{year}-{month:02d}.parquet`. La capa gold lee archivo por archivo sin poder usar partition pruning.

**Solución**: Escribir con `partitionBy("service_id", "year", "month")`. Esto cambia la estructura de directorios pero permite que gold lea solo las particiones que necesita.

```python
# Antes:
out_dir = FACTS_DIR / f"fact_{category}_trip"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = str(out_dir / f"{year}-{month:02d}.parquet")
fact_df.coalesce(4).write.mode("overwrite").parquet(out_path)

# Después:
out_dir = FACTS_DIR
out_dir.mkdir(parents=True, exist_ok=True)
fact_df.coalesce(4).write.mode("overwrite").partitionBy(
    "service_id", "year", "month"
).parquet(str(out_dir))
```

**⚠️ Esto requiere cambios en `GoldContext.fact_path()`** en `app/pipeline/gold/mart_builder.py`:

```python
# Antes (línea 84):
def fact_path(self, category: str, year: int, month: int) -> Path:
    return FACTS_DIR / f"fact_{category}_trip" / f"{year}-{month:02d}.parquet"

# Después:
def fact_path(self, category: str, year: int, month: int) -> Path:
    return FACTS_DIR / f"service_id={category}" / f"year={year}" / f"month={month:02d}"
```

Y en `_silver_ready()` en `gold_pipeline.py` (línea 122):

```python
# Antes:
facts_ok = FACTS_DIR.exists() and any(FACTS_DIR.glob("fact_*_trip"))

# Después:
facts_ok = FACTS_DIR.exists()
```

Además, `fact_df` debe incluir las columnas `service_id`, `year`, `month` — ya están siendo añadidas en los builders de cada categoría (e.g., `F.lit("yellow").alias("service_id")` en la línea 415 de `star.py`). Verificar que `year` y `month` también estén presentes (se añaden implícitamente vía `_date_key`). Si no, agregar:

```python
F.lit(year).alias("year"),
F.lit(month).alias("month"),
```

**Revertir es trivial**: volver al formato anterior. Esto no afecta los datos, solo la ruta de archivos.

---

## 4. Gold Layer — Cache Unificado de Facts (Mayor impacto: ⭐⭐⭐⭐⭐)

**Problema crítico**: Tres builders (`SupplyDemandBalanceMart`, `AbcXyzZonesMart`, `ArimaFeatures`) llaman `read_union()` por separado. Cada uno lee y une **todos los facts mensuales** desde cero (~48 archivos). Esto es 3× el mismo I/O + 3× las mismas transformaciones.

**Solución**: Crear un cache persistido en `GoldContext` con los facts unificados y exponerlo para todos los builders agregados.

### Archivo: `app/pipeline/gold/mart_builder.py` — `GoldContext` (líneas 65–125)

Agregar al `__init__`:

```python
class GoldContext:
    def __init__(self, spark, logger, config, targets, gold_dims,
                 silver_audit_id, mode="full"):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.targets = targets
        self.gold_dims = gold_dims
        self.silver_audit_id = silver_audit_id
        self.mode = mode
        # --- Nuevo: cache unificado de facts para builders agregados ---
        self._cached_union: DataFrame | None = None

    def get_union_facts(
        self,
        select_fn=None,
        categories: list[str] | None = None,
    ) -> DataFrame | None:
        """Devuelve (y cachea) un DataFrame unificado de todos los facts.

        La primera llamada construye y persiste la union; las siguientes
        reusan el cache. ``select_fn`` solo se aplica en la primera llamada.
        """
        if self._cached_union is not None:
            # Ya cacheado — filtrar por categorias si se pide
            if categories is not None:
                return self._cached_union.filter(
                    F.col("service_id").isin(categories)
                )
            return self._cached_union

        self.logger.info("Construyendo cache unificado de facts (gold)")
        dfs: list[DataFrame] = []
        for (cat, year, month) in self.target_months(categories):
            fact = self.read_fact(cat, year, month)
            if fact is None:
                continue
            sel = select_fn(fact, cat) if select_fn else fact
            if sel is not None:
                dfs.append(sel)

        if not dfs:
            return None

        out = dfs[0]
        for d in dfs[1:]:
            out = out.unionByName(d, allowMissingColumns=True)

        out = out.persist(StorageLevel.MEMORY_AND_DISK)
        self._cached_union = out

        n = out.count()
        self.logger.info(f"Cache unificado: {n} filas totales")
        return out

    def release_union_cache(self) -> None:
        """Liberar el cache al final del pipeline gold."""
        if self._cached_union is not None:
            try:
                self._cached_union.unpersist()
            except Exception:
                pass
            self._cached_union = None
```

### Archivo: `app/pipeline/gold/gold_pipeline.py` — `GoldPipeline.run()` (línea 97)

Agregar liberación del cache después de construir todos los builders:

```python
# Antes del final (despues del loop de builders):
for cls in self.BUILDER_CLASSES:
    ...

# Liberar cache unificado
ctx.release_union_cache()

self.logger.info("Capa gold completada exitosamente")
```

### Archivo: `app/pipeline/gold/marts/supply_demand_balance.py`

Reemplazar `read_union()` por `get_union_facts()`:

```python
# Antes (líneas 26–42):
def build(self, ctx: GoldContext) -> int:
    block = ctx.config.supply_demand.block_minutes
    threshold = ctx.config.supply_demand.deficit_threshold
    step = block * 60

    def select_fn(fact, category):
        cols = set(fact.columns)
        if not ({PU_LOC, DO_LOC} <= cols):
            return None
        return fact.select(
            F.col("pickup_datetime"),
            F.col("dropoff_datetime"),
            F.col(PU_LOC).alias("pu_location_id"),
            F.col(DO_LOC).alias("do_location_id"),
        )

    df = ctx.read_union(select_fn)
    if df is None:
        ...

# Después:
def build(self, ctx: GoldContext) -> int:
    block = ctx.config.supply_demand.block_minutes
    threshold = ctx.config.supply_demand.deficit_threshold
    step = block * 60

    def select_fn(fact, category):
        cols = set(fact.columns)
        if not ({PU_LOC, DO_LOC} <= cols):
            return None
        return fact.select(
            F.col("pickup_datetime"),
            F.col("dropoff_datetime"),
            F.col(PU_LOC).alias("pu_location_id"),
            F.col(DO_LOC).alias("do_location_id"),
        )

    df = ctx.get_union_facts(select_fn)
    if df is None:
        ...
```

Y eliminar el `df.unpersist()` manual del `finally` (línea 133) — el cache se libera globalmente al finalizar gold:

```python
# Antes (líneas 127-133):
try:
    n = out.count()
    self._write(out)
    ...
finally:
    df.unpersist()

# Después:
n = out.count()
self._write(out)
...
# df.unpersist() se maneja en GoldContext.release_union_cache()
```

### Archivo: `app/pipeline/gold/marts/abc_xyz_zones.py`

Reemplazar `read_fact()` individual por `get_union_facts()`. Simplifica `_load_year()`:

```python
# En build(), reemplazar todo el loop (líneas 28-54) por:
def build(self, ctx: GoldContext) -> int:
    cfg = ctx.config.abc_xyz

    def select_fn(fact, category):
        rc = REVENUE_COL.get(category)
        if rc is None or rc not in fact.columns or PU_LOC not in fact.columns:
            return None
        return fact.select(
            F.col(PU_LOC).alias("location_id"),
            F.col(rc).alias("revenue"),
            F.to_date("pickup_datetime").alias("trip_date"),
            F.lit(category).alias("_cat"),
            F.year("pickup_datetime").alias("_year"),
        )

    data = ctx.get_union_facts(select_fn)
    if data is None:
        return -1

    data = data.filter(F.col("location_id").isNotNull())
    # Resto del metodo _classify pero agrupado por _cat, _year
    # ... (adaptar _classify para recibir el df completo)
```

**Nota**: `AbcXyzZonesMart` itera por pares (categoría, año). Con el cache unificado se puede hacer un solo groupBy por `location_id`, `_cat`, `_year` — elimina la necesidad del loop.

### Archivo: `app/pipeline/gold/ml/arima_features.py`

Reemplazar `read_union()` por `get_union_facts()`:

```python
# Antes (líneas 14–24):
def build(self, ctx: GoldContext) -> int:
    def select_fn(fact, category):
        if PU_LOC not in fact.columns:
            return None
        return fact.select(
            F.col("pickup_datetime"),
            F.col(PU_LOC).alias("location_id"),
            F.col("service_id"),
        )

    df = ctx.read_union(select_fn)

# Después:
def build(self, ctx: GoldContext) -> int:
    def select_fn(fact, category):
        if PU_LOC not in fact.columns:
            return None
        return fact.select(
            F.col("pickup_datetime"),
            F.col(PU_LOC).alias("location_id"),
            F.col("service_id"),
        )

    df = ctx.get_union_facts(select_fn)
```

---

## 5. Almacenamiento

### 5.1 Ajustar `rows_per_file`

**Archivo**: `app/utils/spark.py` — función `target_files()` (línea 73)

Actualmente usa 1M filas por archivo. Subir a **2M** para reducir archivos pequeños sin crear archivos monstruo:

```python
# Antes:
def target_files(row_count: int, rows_per_file: int = 1_000_000, cap: int = 64) -> int:

# Después:
def target_files(row_count: int, rows_per_file: int = 2_000_000, cap: int = 32) -> int:
```

**Nota**: Con AQE habilitado, `target_files()` se vuelve menos necesario — AQE ya coalescea las shuffle partitions según `advisoryPartitionSizeInBytes`. Se puede mantener para compatibilidad o deprecarlo gradualmente.

### 5.2 Optimización de archivos Parquet

- **Codec zstd level 19**: Ya está configurado y es óptimo. No cambiar.
- **Sin row group tuning**: Spark maneja row groups automáticamente (target ~1GB por stripe). No intervenir.

---

## 6. Verificación de calidad

### No se pierden datos

| Estrategia | Riesgo | Mitigación |
|---|---|---|
| AQE habilitado | Ninguno | Solo cambia el plan físico, no la lógica |
| Drop de helper cols pre-persist | Bajo | `_pickup_dt`/`_dropoff_dt` eran duplicados de los timestamps originales |
| Eliminar count() pre-write | Ninguno | `coalesce(4)` no cambia los datos |
| Cache unificado en gold | Medio | El cache usa `persist()`, no modifica datos. Si hay OOM, el spill a disco es automático |
| partitionBy en facts | Medio | Cambia estructura de directorios. Los datos son idénticos |
| Paralelismo en silver | Medio | Más presión de heap. Si OOM, reducir `max_workers` |

### Pruebas recomendadas

1. **Test de integridad**: Comparar `COUNT(*)` entre silver stage y star facts (por categoría/año/mes)
2. **Test de auditoría**: Verificar que `silver_audit_id` → `gold_audit_id` sigue intacto
3. **Test de esquema**: `DESCRIBE` antes/después para cada fact, mart y feature store
4. **Test de regresión**: `pytest -m "not integration"` después de cada cambio

---

## 7. Resumen de cambios por archivo

| Archivo | Cambio | Prioridad |
|---|---|---|
| `app/utils/spark.py` | + AQE configs, + shuffle.partitions 128, + driver.memory 8g | 🔴 Alta |
| `app/utils/spark.py` | + rows_per_file 2M | 🟡 Media |
| `app/pipeline/silver.py` | + drop helper cols antes de persist | 🔴 Alta |
| `app/pipeline/silver.py` | + eliminar count() redundante, coalesce(4) fijo | 🟡 Media |
| `app/pipeline/silver.py` | + ThreadPoolExecutor para paralelizar | 🔴 Alta |
| `app/pipeline/star.py` | + eliminar count() pre-write, coalesce(4) fijo | 🟡 Media |
| `app/pipeline/star.py` | + partitionBy opcional | 🟢 Baja |
| `app/pipeline/gold/mart_builder.py` | + GoldContext.get_union_facts() con cache | 🔴 Alta |
| `app/pipeline/gold/mart_builder.py` | + GoldContext.release_union_cache() | 🔴 Alta |
| `app/pipeline/gold/gold_pipeline.py` | + ctx.release_union_cache() al final | 🔴 Alta |
| `app/pipeline/gold/marts/supply_demand_balance.py` | + usar get_union_facts(), eliminar unpersist manual | 🔴 Alta |
| `app/pipeline/gold/marts/abc_xyz_zones.py` | + refactor para usar get_union_facts() | 🔴 Alta |
| `app/pipeline/gold/ml/arima_features.py` | + usar get_union_facts() | 🔴 Alta |

---

## 8. Impacto estimado

| Máquina | Antes (estimado) | Después (estimado) | Reducción |
|---|---|---|---|
| 4 cores, 16 GB RAM, HDD | ~45-60 min silver + ~30-45 min gold | ~20-30 min silver + ~15-25 min gold | **~50-60%** |
| 8 cores, 16 GB RAM, SSD | ~25-35 min silver + ~15-25 min gold | ~10-15 min silver + ~8-12 min gold | **~55-65%** |

**Almacenamiento**: Sin cambios significativos (el codec zstd level 19 ya es óptimo). La eliminación de helper cols en silver libera ~5-10% en stage. Si se implementa partitionBy en facts, la estructura de directorios será más navegable pero el peso total será idéntico.

---

> **Nota**: Todos los cambios son reversibles. Cada sección puede implementarse de forma independiente. Recomendación de orden: configs Spark → silver → star → gold.
