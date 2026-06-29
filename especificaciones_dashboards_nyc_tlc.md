# Especificaciones Técnicas de Dashboards - Dataset NYC TLC
Este documento contiene la arquitectura de datos y la especificación de requisitos para el desarrollo de la suite de dashboards analíticos basados en los datasets de la NYC Taxi and Limousine Commission (TLC), incluyendo Yellow Taxi, Green Taxi, High Volume FHV (HVFHV) y la tabla de Zone Lookup.

---

## 1. CATEGORÍA: DASHBOARDS DESCRIPTIVOS

### 1.1 Dashboard de Volumen y Demanda
* **Propósito:** Analizar la distribución temporal y espacial de la demanda de viajes en la ciudad de Nueva York, identificando patrones de tráfico, horas pico y la adopción de servicios tradicionales versus plataformas de gran volumen (Uber/Lyft).
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green Taxi`: `tpep_pickup_datetime`, `lpep_pickup_datetime`, `PULocationID`.
  * `High Volume FHV (HVFHV)`: `pickup_datetime`, `request_datetime`, `on_scene_datetime`, `PULocationID`, `hvfhs_license_num`.
  * `Taxi Zone Lookup`: `LocationID`, `Borough`, `Zone`.
* **Columnas Computadas (Feature Engineering):**
  * `fecha_viaje`: Extraída de `pickup_datetime` (Formato: YYYY-MM-DD).
  * `bloque_horario`: Categorización de la hora de `pickup_datetime` en rangos (ej. Madrugada [00-05], Hora Punta Mañana [06-09], Mediodía [10-15], Hora Punta Tarde [16-19], Noche [20-23]).
  * `dia_semana`: Día de la semana (1-7) extraído de `pickup_datetime`.
  * `tiempo_espera_minutos` (Exclusivo de HVFHV): `(on_scene_datetime - request_datetime) / 60`.

### 1.2 Dashboard de Rendimiento Financiero
* **Propósito:** Monitorear la salud financiera del sistema de transporte, evaluando los ingresos brutos, la distribución de costos (tarifas, peajes, recargos por congestión) y los márgenes de retención de las plataformas tecnológicas.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green Taxi`: `fare_amount`, `tolls_amount`, `congestion_surcharge`, `total_amount`.
  * `High Volume FHV (HVFHV)`: `base_passenger_fare`, `tolls`, `congestion_surcharge`, `sales_tax`, `driver_pay`, `tips`.
* **Columnas Computadas (Feature Engineering):**
  * `margen_plataforma` (Exclusivo de HVFHV): `base_passenger_fare - driver_pay` (Representa la ganancia bruta de Uber/Lyft antes de impuestos corporativos).
  * `ingreso_bruto_por_milla`: `total_amount / trip_distance` (Taxis) o `base_passenger_fare / trip_miles` (HVFHV). 
  * `ratio_pago_conductor` (Exclusivo de HVFHV): `driver_pay / base_passenger_fare`.

### 1.3 Dashboard de Perfil Operativo
* **Propósito:** Evaluar la eficiencia operativa de la flota en términos de distancias recorridas, velocidades de tránsito promedio en la ciudad y el impacto de las iniciativas de viajes compartidos (pooling).
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green Taxi`: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `lpep_pickup_datetime`, `lpep_dropoff_datetime`, `trip_distance`.
  * `High Volume FHV (HVFHV)`: `pickup_datetime`, `dropoff_datetime`, `trip_miles`, `shared_request_flag`, `shared_match_flag`.
* **Columnas Computadas (Feature Engineering):**
  * `duracion_viaje_minutos`: `(dropoff_datetime - pickup_datetime) / 60`.
  * `velocidad_promedio_mph`: `trip_distance / (duracion_viaje_minutos / 60)`. (Requiere limpieza: omitir si la duración es <= 0 o la distancia es 0).
  * `tasa_ocupacion_compartida` (Exclusivo de HVFHV): Ratio incremental calculado como `COUNT(viajes donde shared_match_flag == 'Y') / COUNT(total_viajes)`.

---

## 2. CATEGORÍA: DASHBOARDS DIAGNÓSTICOS

### 2.1 Dashboard de Desequilibrio Oferta-Demanda
* **Propósito:** Diagnosticar la brecha geográfica y temporal entre la disponibilidad de vehículos y las solicitudes de pasajeros. Permite identificar zonas de "atrapamiento" de taxis y desiertos de servicio.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green / HVFHV`: `PULocationID` (Origen), `DOLocationID` (Destino), `pickup_datetime`, `dropoff_datetime`.
  * `Taxi Zone Lookup`: `LocationID`, `Borough`.
* **Columnas Computadas (Feature Engineering):**
  * `bloque_temporal_t`: Ventanas de tiempo parametrizables (ej. intervalos de 15 o 30 minutos) basadas en las fechas de pickup y dropoff.
  * `taxis_entrantes_zona`: Agregación (COUNT) de `DOLocationID` en la zona Z durante el bloque temporal t.
  * `taxis_salientes_zona` (Demanda): Agregación (COUNT) de `PULocationID` en la zona Z durante el bloque temporal t+1.
  * `flujo_neto_oferta`: `taxis_entrantes_zona (t) - taxis_salientes_zona (t+1)`. Un valor negativo crítico diagnostica un déficit severo de oferta inminente en la zona.

### 2.2 Dashboard de Análisis ABC/XYZ (Zonas de Origen)
* **Propósito:** Clasificar las zonas de captación de Nueva York bajo el marco de inventario ABC/XYZ. Determina qué zonas generan el mayor volumen de facturación (ABC) y cuáles presentan una demanda altamente predecible vs. errática (XYZ) para optimizar el despacho de flotas.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green / HVFHV`: `PULocationID`, `total_amount`, `base_passenger_fare`, `pickup_datetime`.
* **Columnas Computadas (Feature Engineering):**
  * `ingresos_totales_zona`: Suma agregada por `PULocationID` de `total_amount` o `base_passenger_fare` en un periodo histórico.
  * `porcentaje_acumulado_ingresos`: Cálculo de Pareto ordenando zonas de mayor a menor ingreso para asignar clases:
    * **Clase A:** Zonas que acumulan el 80% de los ingresos totales.
    * **Clase B:** Siguiente 15% de los ingresos.
    * **Clase C:** Último 5% de los ingresos.
  * `coeficiente_variacion_xyz`: Calculado a nivel de zona sobre la métrica de cantidad de viajes diarios: `Desviación Estándar de Viajes Diarios / Promedio de Viajes Diarios`.
    * **Clase X (Estable):** Coeficiente de variación bajo (ej. < 0.2).
    * **Clase Y (Fluctuante):** Coeficiente de variación moderado (0.2 - 0.5).
    * **Clase Z (Errática):** Coeficiente de variación alto (> 0.5).

### 2.3 Dashboard de Comportamiento de Propinas
* **Propósito:** Analizar la generosidad y el comportamiento de pago de los usuarios cruzando modalidades de servicio, áreas geográficas y tipos de pago, aislando el sesgo del efectivo.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green Taxi`: `fare_amount`, `tip_amount`, `payment_type`.
  * `High Volume FHV (HVFHV)`: `base_passenger_fare`, `tips`.
* **Columnas Computadas (Feature Engineering):**
  * `porcentaje_propina`: `(tips / base_passenger_fare) * 100` para HVFHV, o `(tip_amount / fare_amount) * 100` para Taxis. (Filtrar `payment_type == 1` [Tarjeta de Crédito] en taxis para evitar distorsiones por propinas en efectivo no registradas).
  * `propina_por_milla`: `tips / trip_miles`.
  * `categoria_generosidad`: Variable categórica basada en umbrales (ej. Sin Propina [0%], Baja [<10%], Estándar [10-18%], Alta [>18%]).

---

## 3. CATEGORÍA: DASHBOARDS PREDICTIVOS / AVANZADOS

### 3.1 Dashboard de Predicción de Cantidad de Viajes (ARIMA)
* **Propósito:** Pronosticar el volumen total de viajes a nivel de macro-región o ciudad con horizontes horarios/diarios para planificar la distribución logística general.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green / HVFHV`: `pickup_datetime`.
  * `Taxi Zone Lookup`: `LocationID`, `Borough`.
* **Columnas Computadas (Feature Engineering):**
  * `serie_temporal_viajes`: Remuestreo y agregación temporal paso a paso (ej. resample count por hora) estructurada como un vector univariado indexado por tiempo, segmentado por `Borough` de origen para alimentar el estimador autorregresivo integrado de media móvil (ARIMA/SARIMAX).

### 3.2 Dashboard de Clustering de Perfiles de Viaje (K-Modes)
* **Propósito:** Segmentar los viajes en arquetipos o perfiles de comportamiento operativo basados puramente en interacciones y características categóricas, sin depender de coordenadas geográficas continuas.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green / HVFHV`: `PULocationID`, `DOLocationID`, `pickup_datetime`, `hvfhs_license_num` (para apps) o `VendorID` (para taxis).
  * `Taxi Zone Lookup`: `LocationID`, `Borough` (Origen y Destino correspondientes).
* **Columnas Computadas (Feature Engineering):**
  * `franja_horaria`: Atributo categórico nominal (Mañana, Tarde, Noche, Madrugada) extraído de la hora de inicio.
  * `dia_categoria`: Atributo categórico dicotómico (Día Laborable, Fin de Semana) derivado del día de la semana.
  * *Nota de Ingeniería:* Las variables numéricas continuas (como distancia o tarifa) se excluyen de este modelo; K-Modes calcula la proximidad basándose en la coincidencia de modas de variables categóricas (`PULocationID`, `DOLocationID`, `Borough_PU`, `Borough_DO`, `franja_horaria`, `dia_categoria`, `hvfhs_license_num`).

### 3.3 Dashboard de Detección de Anomalías / Fraude en Taxímetros
* **Propósito:** Detectar transacciones fraudulentas, taxímetros adulterados ("fast meters") o cobros abusivos mediante la identificación de desviaciones extremas frente a las tarifas reguladas por la TLC.
* **Datasets y Columnas Involucradas:**
  * `Yellow / Green Taxi` (Enfoque principal debido a tarifas físicas y reguladas): `fare_amount`, `trip_distance`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `RatecodeID` (Códigos oficiales: 1=Standard, 2=JFK, 3=Newark, etc.), `Extra`, `mta_tax`, `improvement_surcharge`.
* **Columnas Computadas (Feature Engineering / Target Features):**
  * `duracion_viaje_segundos`: `tpep_dropoff_datetime - tpep_pickup_datetime`.
  * `velocidad_promedio_calculada`: `trip_distance / (duracion_viaje_segundos / 3600)`.
  * `costo_por_distancia`: `fare_amount / (trip_distance + 0.001)`.
  * `desviacion_tarifa_teorica`: Métrica lógica basada en condicionales matemáticos según el `RatecodeID`. Ejemplos:
    * Si `RatecodeID == 2` (Tarifa plana JFK), `fare_amount` debe ser exactamente el valor estipulado por la ley para ese año fiscal. Cualquier variación notable se computa como anomalía flagrante.
    * Si `RatecodeID == 1` (Tarifa estándar), la fórmula teórica es basada en distancia y tiempo detenido. Un viaje con velocidad promedio normal pero con un `costo_por_distancia` extremadamente elevado indica una alteración del taxímetro.
  * *Nota de Implementación:* Estas features alimentarán un pipeline no supervisado (Isolation Forest) o supervisado (XGBoost entrenado con flags históricos de auditorías) para generar un Score de Probabilidad de Fraude en el dashboard.
