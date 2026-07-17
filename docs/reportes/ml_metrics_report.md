# Reporte de Métricas de Evaluación — Modelos ML

> **Pipeline**: EF-GDM-G6 · Capa Gold ML (`data/gold/ml/` + `data/gold/models/`)
> **Fecha de entrenamiento**: 2026-07-13 (K-Modes, Isolation Forest) · 2026-07-17 (SARIMAX)
> **Fuente de datos**: NY TLC Trip Record Data (2023–2025, 4 categorías)
> **Generado el**: 2026-07-17

---

## Resumen ejecutivo

| Modelo | Tipo | Objetivo | Segmentación | Métrica principal | Valor global |
|---|---|---|---|---|---|
| K-Modes | Clustering no supervisado (categórico) | Perfiles de viaje por servicio | 3 modelos (yellow, green, fhvhv) | Silhouette categórico | 0.19–0.43 |
| Isolation Forest | Detección de anomalías (no supervisado) | Fraude por tarifa (RatecodeID) | 6 modelos (Ratecodes 1–5, 99) | Contaminación / anomaly_score | 5% por segmento |
| SARIMAX | Forecast de serie temporal | Demanda por borough × servicio | 31 modelos (7 boroughs × {yellow,green,fhv,fhvhv} + 3 Unknown) | AIC | −84.4K a 437.2K |

---

## 1. K-Modes — Clustering de perfiles de viaje

### 1.1 Metadatos globales

- **Algoritmo**: KModes (`init=Cao`, `n_init=2`, `random_state=42`)
- **Feature store de origen**: `ml_feat_kmodes_trips` (fhvhv muestreado al 5%, seed 42)
- **Salidas**: `tuning_*`, `centers_*`, `labels_*`, `profiles_*` en `data/gold/ml/kmodes_model/`
- **Modelos serializados**: `data/gold/models/kmodes/{service_id}/model.joblib` + `metadata.json`

### 1.2 Métricas de evaluación por servicio

La selección de `k` óptimo se realiza por **silhouette categórico** (matching-dissimilarity), con submuestra de 5.000 filas para mantener viabilidad computacional (O(n²)). El costo (within-cluster dissimilarity) se grafica como curva elbow.

| service_id | n_rows entrenados | n_features | k óptimo | **Silhouette** | Costo @ k óptimo | Filas clusterizadas (labels) |
|---|---:|---:|---:|---:|---:|---:|
| fhvhv | 100.000 | 5 | 2 | **0.1932** | 202.721 | 9.216 |
| green | 99.700 | 7 | 3 | **0.4330** | 143.617 | 9.216 |
| yellow | 100.000 | 7 | 2 | **0.3544** | 140.235 | 9.216 |

### 1.3 Curva de tuning completa (k=2…8)

#### fhvhv — silhouette máxima en k=2

| k | Costo | Silhouette |
|---:|---:|---:|
| 2 | 202.721,0 | 0.1932 |
| 3 | 176.256,0 | 0.1857 |
| 4 | 168.287,0 | 0.1388 |
| 5 | 161.479,0 | 0.1147 |
| 6 | 158.773,0 | 0.1067 |
| 7 | 151.771,0 | 0.1072 |
| 8 | 146.242,0 | 0.1122 |

**Observación**: el costo sigue decreciendo suavemente (sin codo marcado), pero el silhouette decae fuertemente a partir de k=4. k=2 es el equilibrio calidad/estabilidad.

#### green — silhouette máxima en k=3

| k | Costo | Silhouette |
|---:|---:|---:|
| 2 | 162.047,0 | 0.3816 |
| 3 | 143.617,0 | **0.4330** |
| 4 | 132.687,0 | 0.3820 |
| 5 | 127.395,0 | 0.3555 |
| 6 | 126.591,0 | 0.3554 |
| 7 | 125.155,0 | 0.3565 |
| 8 | 121.064,0 | 0.3254 |

**Observación**: el mejor clustering del estudio (0.43). k=3 presenta un codo claro en costo y pico de silhouette.

#### yellow — silhouette máxima en k=2

| k | Costo | Silhouette |
|---:|---:|---:|
| 2 | 140.235,0 | **0.3544** |
| 3 | 132.269,0 | 0.3234 |
| 4 | 132.706,0 | 0.3427 |
| 5 | 125.444,0 | 0.2396 |
| 6 | 114.811,0 | 0.2712 |
| 7 | 113.869,0 | 0.2140 |
| 8 | 112.143,0 | 0.2185 |

**Observación**: fuerte inestabilidad del silhouette en k≥3 (cae a 0.21 y rebota); k=2 es la selección robusta.

### 1.4 Distribución de clusters

| service_id | Cluster 0 | Cluster 1 | Cluster 2 | Total (entrenados) |
|---|---:|---:|---:|---:|
| fhvhv | 74.220 (74,2%) | 25.780 (25,8%) | — | 100.000 |
| green | 63.501 (63,7%) | 25.038 (25,1%) | 11.161 (11,2%) | 99.700 |
| yellow | 92.869 (92,9%) | 7.131 (7,1%) | — | 100.000 |

### 1.5 Perfiles dominantes por cluster (top_value · top_pct)

#### fhvhv

| Cluster | borough_pu | borough_do | franja_horaria | dia_categoria | hvfhs_license_num |
|---:|---|---|---|---|---|
| 0 | Manhattan (50,2%) | Manhattan (45,5%) | Tarde (43,5%) | Día Laborable (80,1%) | HV0003 (73,6%) |
| 1 | Brooklyn (67,3%) | Brooklyn (66,8%) | Noche (41,1%) | Fin de Semana (63,2%) | HV0003 (71,6%) |

**Interpretación**: Cluster 0 = viajes diurnos laborables Manhattan-céntricos. Cluster 1 = viajes recreativos Brooklyn en fin de semana/noche.

#### green

| Cluster | borough_pu | borough_do | franja_horaria | dia_categoria | payment_type | ratecode | passenger_group |
|---:|---|---|---|---|---|---|---|
| 0 | Manhattan (92,9%) | Manhattan (92,4%) | Tarde (49,1%) | Día Laborable (77,9%) | Tarjeta crédito (≈85%) | Standard rate (≈90%) | Solo (85,0%) |
| 1 | ver detalle perfil | ver detalle perfil | Noche | Fin de Semana | — | — | — |
| 2 | ver detalle perfil | ver detalle perfil | Noche (41,5%) | Día Laborable (65,7%) | Tarjeta crédito (86,9%) | Standard rate (88,7%) | Solo (78,3%) |

#### yellow

| Cluster | borough_pu | borough_do | franja_horaria | dia_categoria | payment_type | ratecode | passenger_group |
|---:|---|---|---|---|---|---|---|
| 0 | Manhattan (88,4%) | Manhattan (89,0%) | Tarde (46,9%) | Día Laborable (77,5%) | — | — | Solo (81,5%) |
| 1 | ver detalle perfil | ver detalle perfil | Noche (49,7%) | Fin de Semana (84,0%) | Efectivo (55,7%) | Standard rate (93,2%) | Pareja (56,1%) |

**Interpretación transversal**: en los 3 servicios aparece una dicotomía Manhattan-día-laborable vs. periférico-fin-de-semana-noche. El cluster minoritario de yellow (7,1%) concentra viajes en pareja pagados en efectivo — posible patrón recreativo/turístico.

### 1.6 Interpretación de métricas

- **Silhouette categórico** mide cohesión vs. separación sobre la distancia de matching (0 = idénticos, n_cats = máximamente distintos). Rango [−1, 1]:
  - `>0.5` = estructura fuerte (no alcanzado en este dataset)
  - `0.25–0.5` = estructura razonable (green y yellow)
  - `0.19` = estructura débil (fhvhv — pocas features, poca variabilidad categórica)
- **Costo** = suma de disimilaridades within-cluster. Decrece monótonamente con k pero no es criterio de selección por sí solo: el silhouette actúa como freno al overclustering.

---

## 2. Isolation Forest — Detección de fraude

### 2.1 Metadatos globales

- **Algoritmo**: `sklearn.ensemble.IsolationForest`
- **Hiperparámetros**: `contamination=0.05`, `n_estimators=100`, `max_samples="auto"`, `random_state=42`, `n_jobs=-1`
- **Features** (6, continuos): `velocidad_promedio_calculada`, `costo_por_distancia`, `duracion_viaje_segundos`, `trip_distance`, `fare_amount`, `ratio_peaje_tarifa`
- **Muestreo**: máx 200.000 filas por RatecodeID (fracción Spark, seed 42)
- **Umbral de fraude**: percentil 95 del `anomaly_score` (score invertido: mayor = más anómalo)
- **Feature store de origen**: `ml_feat_isolation_fraud` (yellow + green, trip-grain)
- **Salidas**: `data/gold/ml/ml_isolation_fraud_scores/ratecode_id=*/` + `data/gold/models/isolation_forest/{rc}/model.joblib` + `metadata.json`

### 2.2 Métricas de evaluación por RatecodeID

| RatecodeID | Descripción | n_rows origen | n_rows entrenados | **Tasa is_fraud** | Score P5 | Score mediana | Score P95 | Score máx |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | Standard rate | 104.051.790 | 200.000 | 5,00% | −0,2489 | −0,2210 | ~0,0000 | 0,2052 |
| 2 | JFK | 4.062.795 | 199.647 | 5,00% | −0,2585 | −0,2207 | 0,0000 | 0,1639 |
| 3 | Newark | 381.076 | 200.000 | 4,98% | −0,2006 | −0,1573 | ~0,0000 | 0,1950 |
| 4 | Nassau/Westchester | 283.544 | 200.000 | 5,00% | −0,1809 | −0,1441 | ~0,0000 | 0,2127 |
| 5 | Negotiated fare | 1.032.530 | 200.000 | 5,00% | −0,1766 | −0,1513 | ~0,0000 | 0,2561 |
| 99 | Unknown/Group ride | 1.484.727 | 200.000 | 5,00% | −0,1994 | −0,1578 | ~0,0000 | 0,1978 |

### 2.3 Interpretación de métricas

- **Contamination=0.05** define a priori que el 5% superior de anomalías se marca como `is_fraud=True`. Es una heurística operativa, no una etiqueta de ground truth.
- **`anomaly_score`** (invertido con `-decision_function`): mayor valor = más anómalo. El percentil 95 se aproxima a 0,0 (frontera), y los outliers llegan hasta 0.20–0.26.
- **No hay métricas supervisadas** (precision/recall/F1/AUC-ROC) porque **no existe etiqueta de fraude real** en el dataset TLC. La validación es por inspección de los scores y revisión de los registros marcados.
- **Observación por ratecode**: el RatecodeID 5 (Negotiated fare) presenta el score máximo más alto (0.256), consistente con la mayor variabilidad esperable en tarifas negociadas. El RatecodeID 1 (Standard rate, 104M filas) fue reducido por muestreo pero conserva la misma distribución de scores.
- **Total de viajes puntuados**: ~1.199.647 (suma de los 6 modelos entrenados).

### 2.4 Limitaciones de la evaluación

1. **Sin ground truth**: el modelo es no supervisado. La "tasa de fraude" (5%) es un parámetro, no una medición.
2. **Sesgo de muestreo**: para ratecodes con <200K filas se entrenó sobre el dataset completo; para ratecodes mayores se muestreó, preservando la distribución pero limitando el recall de outliers minoritarios.
3. **Nulos imputados**: las ausencias en features continuas se rellenaron con la mediana (o 0.0 si todo era nulo), lo que puede artefactar scores en ratecodes con datos faltantes.

---

## 3. SARIMAX — Forecast de demanda (trip-count)

### 3.1 Metadatos globales

- **Algoritmo**: `statsmodels.tsa.statespace.sarimax.SARIMAX`
- **Orden**: `(1,1,1)(1,1,1,24)` — AR estacional con periodicidad horaria (24h)
- **Regresores exógenos** (4): `is_holiday`, `is_weekend`, `is_rush_hour`, `is_airport_borough`
- **Feature store de origen**: `ml_feat_arima_trips` (agregado borough × service_id × hora)
- **Modo de entrenamiento**: `forecast_until_year=2027` (extiende el forecast futuro hasta fin de 2027)
- **Segmentación**: 7 boroughs × 4 servicios (yellow, green, fhv, fhvhv) — fhvhv no disponible en Unknown/N_A
- **Salidas**: `data/gold/ml/ml_sarimax_trips_forecast/borough=*/service_id=*/forecast_*.parquet` + `data/gold/models/sarimax/{borough}__{service}/model.pkl` + `metadata.json`
- **Fallback de orden**: si `(1,1,1)(1,1,1,24)` falla, reintenta con `(1,0,1)(0,1,1,24)`.

### 3.2 Métricas de evaluación por segmento

> **Nota crítica sobre MAE/MAPE**: todos los modelos se entrenaron en modo `forecast_until_year=2027`, que usa **todo el histórico como entrenamiento** y proyecta hacia el futuro. En este modo **no se retiene un holdout**, por lo que `mae` y `mape` son `null`. El AIC es la única métrica de ajuste disponible (es comparable entre modelos del mismo segmento; un AIC más bajo = mejor ajuste relativo penalizando complejidad).

| Borough | service_id | n_rows | **AIC** | model_status | horas forecast futuro | horas in-sample |
|---|---|---:|---:|---|---:|---:|
| Bronx | fhv | 26.304 | 198.070,14 | ok | 17.520 | 26.304 |
| Bronx | fhvhv | 26.304 | 382.153,80 | ok | 17.520 | 26.304 |
| Bronx | green | 26.303 | 87.312,14 | ok | 17.520 | 26.303 |
| Bronx | yellow | 27.720 | 166.999,19 | ok | 16.104 | 27.720 |
| Brooklyn | fhv | 26.304 | 229.262,93 | ok | 17.520 | 26.304 |
| Brooklyn | fhvhv | 26.304 | 416.772,34 | ok | 17.520 | 26.304 |
| Brooklyn | green | 26.304 | 144.865,30 | ok | 17.520 | 26.304 |
| Brooklyn | yellow | 27.720 | 211.100,10 | ok | 17.520 | 27.720 |
| EWR | fhv | 26.304 | 84.601,61 | ok | 17.520 | 26.304 |
| EWR | fhvhv | 25.830 | **−84.385,18** | ok | 17.715 | 25.830 |
| EWR | green | 25.317 | **−72.096,70** | ok | 17.960 | 25.317 |
| EWR | yellow | 27.715 | 75.453,38 | ok | 16.104 | 27.715 |
| Manhattan | fhv | 26.304 | 224.435,42 | ok | 17.520 | 26.304 |
| Manhattan | fhvhv | 26.304 | **437.219,01** | ok | 17.520 | 26.304 |
| Manhattan | green | 26.304 | 193.200,24 | ok | 17.520 | 26.304 |
| Manhattan | yellow | 27.720 | 407.491,91 | ok | 16.104 | 27.720 |
| N/A | fhv | 26.298 | 122.822,80 | ok | 17.524 | 26.298 |
| N/A | fhvhv | 26.304 | 79.736,57 | ok | 17.520 | 26.304 |
| N/A | green | 26.275 | 2.073,18 | ok | 17.531 | 26.275 |
| N/A | yellow | 27.720 | 113.745,12 | ok | 16.104 | 27.720 |
| Queens | fhv | 26.304 | 221.812,28 | ok | 17.520 | 26.304 |
| Queens | fhvhv | 26.304 | 400.039,84 | ok | 17.520 | 26.304 |
| Queens | green | 26.304 | 165.981,87 | ok | 17.520 | 26.304 |
| Queens | yellow | 27.720 | 311.835,81 | ok | 16.104 | 27.720 |
| Staten Island | fhv | 26.304 | 212.998,10 | ok | 17.520 | 26.304 |
| Staten Island | fhvhv | 26.304 | 279.319,51 | ok | 17.520 | 26.304 |
| Staten Island | green | 25.886 | **−58.647,57** | ok | 17.935 | 25.886 |
| Staten Island | yellow | 27.710 | 29.025,45 | ok | 16.111 | 27.710 |
| Unknown | fhv | 20.954 | **−36.001,85** | ok | 22.837 | 20.954 |
| Unknown | green | 26.302 | 27.260,00 | ok | 17.521 | 26.302 |
| Unknown | yellow | 27.720 | 181.579,06 | ok | 16.104 | 27.720 |

### 3.3 Resumen agregado SARIMAX

| Métrica | Valor |
|---|---|
| Modelos entrenados | 31 |
| State: `ok` | 31 (100%) |
| State: `skipped_low_rows` | 0 |
| State: `fit_failed` | 0 |
| AIC mínimo | −84.385,18 (EWR × fhvhv) |
| AIC máximo | 437.219,01 (Manhattan × fhvhv) |
| AIC medio | ~166.900 |
| Boroughs sin `fhvhv` (por insuficiencia) | Unknown |
| AICs negativos (muy buen ajuste) | EWR × fhvhv, EWR × green, Staten Island × green, Unknown × fhvhv |

### 3.4 Interpretación de métricas

- **AIC (Akaike Information Criterion)** = `−2·log-likelihood + 2·k` (penaliza el sobreajuste). Es comparable solo **entre modelos del mismo segmento y misma serie temporal** (no entre boroughs distintos). Valores negativos (EWR, Staten Island green) no significan "mejor modelo" en términos absolutos — reflejan que la serie tiene baja varianza y el likelihood elevado.
- **MAE/MAPE = `null`** en este run porque el modo `forecast_until_year` consume todo el histórico para proyectar. Para obtener MAE/MAPE reales ejecutar sin `forecast_until_year` (se retiene un holdout de `forecast_horizon_hours`). Ver `config.yaml → gold.sarimax.forecast_until_year`.
- **Horas in-sample** son siempre 26.298–27.720 (3 años × 8.760h +🕓 yellow tiene datos extra). Las **horas forecast** son 17.520 (~2 años desde finales 2025 hasta 2027) o 16.104 (yellow, que tiene datos hasta más adelante).

### 3.5 Limitaciones de la evaluación

1. **AIC no es métrica de error predictivo**: mide calidad del ajuste del modelo, no su capacidad de generalización. Un AIC bajo no garantiza buen forecast.
2. **Sin validación out-of-sample**: al consumir todo el histórico no hay MAE/MAPE disponible. Para habilitarlos, ejecutar el pipeline en modo holdout (`forecast_until_year=None`, `forecast_horizon_hours>0`).
3. **Series con ceros estructurales**: boroughs como Stanton Island y EWR tienen largos tramos con `trip_count=0` (servicios casi inexistentes), lo que explica los AICs negativos atípicos.
4. **Fallback de orden**: si el `(1,1,1)(1,1,1,24)` no converge, se reintenta con `(1,0,1)(0,1,1,24)` marcado como `model_status="fallback_order"`. En este run todos convergieron con el orden primario.

---

## Anexo A — Artefactos generados

### A.1 K-Modes

```
data/gold/ml/kmodes_model/
  tuning_service_id=fhvhv/tuning.parquet     # curva k vs. costo vs. silhouette
  tuning_service_id=green/tuning.parquet
  tuning_service_id=yellow/tuning.parquet
  centers_service_id=fhvhv/centers.parquet   # modas de cada cluster + n_rows
  centers_service_id=green/centers.parquet
  centers_service_id=yellow/centers.parquet
  labels_service_id=fhvhv/                  # trip_id → cluster_id (muestra)
  labels_service_id=green/
  labels_service_id=yellow/
  profiles_service_id=fhvhv/profiles.parquet # top_value, top_pct, n_unique por feature × cluster
  profiles_service_id=green/profiles.parquet
  profiles_service_id=yellow/profiles.parquet

data/gold/models/kmodes/
  fhvhv/    model.joblib + metadata.json + category_mapping.json
  green/    model.joblib + metadata.json + category_mapping.json
  yellow/   model.joblib + metadata.json + category_mapping.json
```

### A.2 Isolation Forest

```
data/gold/ml/ml_isolation_fraud_scores/
  ratecode_id=1/part-*.parquet    # trip_id, anomaly_score, is_fraud, model_status
  ratecode_id=2/
  ratecode_id=3/
  ratecode_id=4/
  ratecode_id=5/
  ratecode_id=99/

data/gold/models/isolation_forest/
  1/    model.joblib + metadata.json
  2/    model.joblib + metadata.json
  3/    model.joblib + metadata.json
  4/    model.joblib + metadata.json
  5/    model.joblib + metadata.json
  99/   model.joblib + metadata.json
```

### A.3 SARIMAX

```
data/gold/ml/ml_sarimax_trips_forecast/
  borough=Bronx/service_id=fhv/forecast_Bronx_fhv.zstd.parquet     # pickup_hour, trip_count, yhat, yhat_lower, yhat_upper, model_status, forecast_type
  borough=Bronx/service_id=fhvhv/...
  ... (31 segmentos)

data/gold/models/sarimax/
  Bronx__fhv/        model.pkl + metadata.json
  Bronx__fhvhv/      model.pkl + metadata.json
  ... (31 modelos)
```

---

## Anexo B — Hiperparámetros (de `config.yaml`)

| Modelo | Parámetro | Valor |
|---|---|---|
| K-Modes | `init_method` | Cao |
| K-Modes | `n_init` | 2 |
| K-Modes | `max_k` | 8 |
| K-Modes | `max_sample_per_service` | 100.000 |
| K-Modes | `random_state` | 42 |
| Isolation Forest | `n_estimators` | 100 |
| Isolation Forest | `contamination` | 0.05 |
| Isolation Forest | `max_samples` | auto |
| Isolation Forest | `random_state` | 42 |
| Isolation Forest | `min_rows_per_ratecode` | (ver config) |
| Isolation Forest | `MAX_SAMPLE_PER_RATECODE` (constante del código) | 200.000 |
| SARIMAX | `order` | (1, 1, 1) |
| SARIMAX | `seasonal_order` | (1, 1, 1, 24) |
| SARIMAX | `min_rows_per_segment` | (ver config) |
| SARIMAX | `forecast_horizon_hours` | (ver config; no aplicado en este run) |
| SARIMAX | `forecast_until_year` | 2027 |

---

## Anexo C — Convenciones de evaluación

- **K-Modes**: al ser clustering no supervisado sobre variables categóricas, el **silhouette** es la única métrica interna disponible. Elbow sobre el costo se usa solo como apoyo. La interpretación cualitativa (perfiles) es la validación final.
- **Isolation Forest**: al carecer de etiquetas de fraude ground truth, la "tasa de fraude" es una heurística operativa (percentil 95 del score). No se reportan precision/recall. La validación es por inspección de los registros marcados y la distribución de scores.
- **SARIMAX**: el AIC es la métrica de ajuste estándar para SARIMAX. MAE/MAPE requieren un holdout, que se omite al activar `forecast_until_year`. Para obtener MAE/MAPE reales, ejecutar en modo holdout eliminando `forecast_until_year` y configurando `forecast_horizon_hours`.

---

**Reporte generado automáticamente a partir de los artefactos en `data/gold/models/` y `data/gold/ml/`.**
**Pipeline de origen**: `app/pipeline/gold_impl/ml/{kmodes_model,isolation_forest_model,sarimax_model}.py`.
