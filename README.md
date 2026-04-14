# TP Inteligencia Artificial En Producción - Pipeline De Pronóstico De Producción De Hidrocarburos

## Descripción

Este proyecto implementa un pipeline completo de Machine Learning en producción para pronosticar la producción de gas y petróleo de pozos no convencionales. El sistema integra Airflow para orquestación, MLFlow para tracking de experimentos, Feast como feature store, y una API REST para consumo externo.

---

## Arquitectura

```
Dataset (CSV)
    ↓
Airflow DAG
    ├── download_dataset → descarga el CSV
    ├── prepare_offline_store → calcula features y genera el parquet + feast apply
    ├── populate_online_store → materializa features recientes al SQLite
    ├── split_data → obtiene features del feature store y splitea
    ├── train_model (x N) → entrena experimentos en serie
    ├── evaluate_model (x N) → evalúa y loguea en MLFlow
    └── select_best_model → promueve el mejor modelo a producción

Feature Store (Feast)
    ├── Offline Store (parquet) → features históricos para entrenamiento
    └── Online Store (SQLite) → features más recientes para inferencia

MLFlow
    ├── Experiment tracking → métricas y artefactos por experimento
    └── Model Registry → versiones y alias de producción

API REST (FastAPI)
    ├── GET /api/v1/forecast → pronóstico de producción de un pozo (gas o petróleo)
    └── GET /api/v1/wells → listado de pozos disponibles
```

---

## Screenshots

### DAG corriendo exitosamente (26 tasks, todo verde)
![DAG Success I](screenshots/DAG_Success_I.png)
![DAG Success II](screenshots/DAG_Success_II.png)

### MLflow - Experimentos y Runs
![MLflow Runs](screenshots/MLFlow_Runs.png)

### MLflow - Model Registry con alias production
![MLflow Model Registry](screenshots/MLFlow_Model_Registry.png)

### API REST - Endpoint `/api/v1/wells`

El endpoint devuelve los pozos disponibles para una fecha dentro del período de entrenamiento. Los pozos disponibles dependen del parquet generado por el DAG.

![GET Listado Pozo](screenshots/GET_Listado_Pozo.png)

### API REST - Endpoint `/api/v1/forecast` — fechas futuras

El DAG fue entrenado con datos de 2023. Al pedir predicción para los 3 meses siguientes (2024), el modelo usa los features del online store y devuelve la misma predicción para cada mes — comportamiento esperado para un modelo single-step sin actualización autoregresiva.

![GET Forecast](screenshots/GET_Forecast.png)

### API REST - Endpoint `/api/v1/forecast` — fechas históricas

Al pedir predicción para 4 meses dentro del período de entrenamiento (2023), el modelo usa los features reales de cada mes desde el parquet. Cada mes tiene su propio `avg_prod_gas_10m` y `last_prod_gas` calculados con producción real, por lo que los valores predichos varían. Esto permite evaluar el comportamiento del modelo sobre el training set.

![GET Forecast II](screenshots/GET_Forecast_II.png)

---

## Setup

### 1. Requisitos

- Docker y Docker Compose
- Python 3.9+

### 2. `.gitignore` recomendado

Este repositorio solo versiona el código fuente (`.py`, `.yaml`, `.yml`, `.md`) necesario para entender y reproducir el proyecto. Las carpetas generadas automáticamente al correr el DAG no se suben al repo ya que pueden pesar varios GBs y se regeneran solas con un solo `docker compose up`.

```
mlruns/
feature_store/data/
feature_store/registry/
feature_store/online_store/
logs/
__pycache__/
*.pyc
.env
```

### 3. Dependencias (`.env`)

Crear un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```
AIRFLOW_UID = 501
_PIP_ADDITIONAL_REQUIREMENTS = pandas scikit-learn mlflow feast fastapi uvicorn==0.40.0
```

> **Nota:** `uvicorn==0.40.0` está pineado para evitar un conflicto de dependencias entre `feast` y `apache-airflow-core 3.1.7`, que requiere `uvicorn>=0.37.0`. Sin este pin, `feast` instala una versión incompatible que rompe el api-server de Airflow.

### 4. Volúmenes en `docker-compose.yaml`

Todos los servicios corren dentro de contenedores Docker. Para que los archivos persistan en disco y los contenedores puedan acceder a ellos, se montan los siguientes volúmenes:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./logs:/opt/airflow/logs
  - ./config:/opt/airflow/config
  - ./plugins:/opt/airflow/plugins
  - ./mlruns:/mlflow/mlruns
  - ./feature_store:/opt/airflow/feature_store
```

Esto hace que cada carpeta local sea visible dentro del contenedor en `/opt/airflow/...`. Por eso todos los paths en el código usan `/opt/airflow/...` y no `./`.

### 5. Estructura de carpetas

```
tp_final/
├── dags/
│   └── dag_oil_and_gas.py ← DAG principal
├── feature_store/
│   ├── data/ ← CSV descargado y parquet con features (generado, no se sube)
│   ├── registry/ ← metadata de Feast (generado, no se sube)
│   ├── online_store/ ← features recientes (generado, no se sube)
│   ├── feature_store.yaml ← configuración de Feast
│   └── features.py ← definición de entidades y feature views
├── api/
│   └── main.py ← API REST con FastAPI
├── screenshots/ ← capturas de pantalla del sistema funcionando
├── mlruns/ ← artefactos de MLFlow (generado, no se sube)
├── logs/ ← logs de Airflow (generado, no se sube)
├── plugins/
├── config/
├── .env ← variables de entorno (no se sube al repo)
├── .gitignore
└── docker-compose.yaml
```

### 6. `feature_store.yaml`

```yaml
project: oil_gas_production
registry: /opt/airflow/feature_store/registry/registry.db
provider: local
offline_store:
  type: file
online_store:
  type: sqlite
  path: /opt/airflow/feature_store/online_store/online.db
```

- **registry**: Feast guarda aquí la metadata (qué features existen, qué esquema tienen, dónde está el parquet)
- **offline_store**: archivo parquet con toda la historia de features por pozo y por mes
- **online_store**: base de datos SQLite con la última fila de cada pozo, lista para inferencia instantánea

### 7. Cómo levantar el sistema

```bash
docker compose up -d
```

El sistema tarda aproximadamente 2-3 minutos en estar completamente operativo.

### 8. Acceso a los servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| MLFlow UI | http://localhost:9090 | - |
| API Swagger | http://localhost:8000/docs | - |

---

## Cómo reproducir el entrenamiento

Desde la UI de Airflow en http://localhost:8080, triggerear el DAG `ml_pipeline_oil_and_gas` con los parámetros:

- `date_from`: fecha de inicio del rango de entrenamiento (formato `YYYY-MM-DD`)
- `date_to`: fecha de fin del rango de entrenamiento (formato `YYYY-MM-DD`)

Si no se especifican fechas, se usa el dataset completo. Ver [Decisiones de Diseño](#decisiones-de-diseño) para la justificación del rango recomendado.

**Importante:** el rango de fechas elegido condiciona el comportamiento posterior de la API. El parquet generado por el DAG contiene features históricos solo para el período entrenado, y el online store queda con el estado del último mes de ese período. Ver sección [Relación entre el entrenamiento y la API](#relación-entre-el-entrenamiento-y-la-api) para más detalle.

---

## Nota sobre MLFlow y seguridad de red

MLFlow 3.5+ incluye un middleware de seguridad que por defecto solo acepta conexiones desde localhost. Para permitir conexiones entre contenedores Docker es necesario deshabilitar este middleware con la variable de entorno `MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE=true` y arrancar el servidor con `--host 0.0.0.0`. Esto está configurado en el `docker-compose.yaml`.

---

## Feature Store

### ¿Por qué un Feature Store?

Cuando el modelo necesita predecir la producción de un pozo, requiere features como el promedio de producción de los últimos 10 meses. Calcularlo en el momento de cada inferencia sería lento y costoso. El feature store resuelve esto separando el problema en dos partes:

- **Offline Store**: almacena los features históricos de todos los pozos en todos los períodos. Se usa para entrenamiento.
- **Online Store**: almacena solo el estado más reciente de cada pozo. Se usa para inferencia.

### Offline Store vs Online Store

**Offline Store** (una fila por pozo por mes, toda la historia):

| idpozo | fecha | avg_prod_gas_10m | last_prod_gas | n_readings |
|--------|-------|-----------------|---------------|------------|
| 132879 | 2020-01 | 430.2 | 445.0 | 5 |
| 132879 | 2020-02 | 438.5 | 460.1 | 6 |
| 132879 | 2020-03 | 441.0 | 455.3 | 7 |

**Online Store** (una sola fila por pozo, lo más reciente):

| idpozo | avg_prod_gas_10m | last_prod_gas | n_readings |
|--------|-----------------|---------------|------------|
| 132879 | 441.0 | 455.3 | 48 |

### ¿Por qué el online store está en SQLite y no en el parquet?

El parquet puede tener millones de filas — una por pozo por mes durante años. Leerlo completo en cada request de inferencia sería inviable. El online store resuelve esto copiando solo la última fila de cada pozo al SQLite. Cuando la API necesita los features de un pozo, hace un lookup por clave primaria (`idpozo`) en O(1) — sin importar cuántos pozos o cuánta historia exista en el parquet.

En resumen: el offline store *calcula* los features, el online store los *sirve rápido*.

### Materialización

Es el proceso de copiar los features más recientes del offline store al online store. Se ejecuta una vez por mes junto con el reentrenamiento. En Feast se hace con `write_to_online_store`.

### `features.py`

Define el contrato de features que Feast espera encontrar en el parquet. Tiene tres componentes:

- **Entity**: el "sujeto" de los features. En este caso el pozo (`idpozo`).
- **FileSource**: le dice a Feast dónde está el parquet y qué columna usar como timestamp para el point-in-time lookup.
- **FeatureView**: une la entidad con la fuente y define el esquema (nombre y tipo de cada feature). Los nombres tienen que coincidir exactamente con las columnas del parquet.

---

## Features del Modelo

### Dataset original

El dataset contiene lecturas mensuales de producción por pozo. Las columnas relevantes son:

- `idpozo`: identificador único del pozo
- `anio` y `mes`: año y mes de la lectura (se combinan en `fecha`)
- `prod_gas`: producción de gas (target)
- `prod_pet`: producción de petróleo (target)
- `prod_agua`: producción de agua (feature)
- `tef`: tiempo efectivo de flujo (feature)
- `profundidad`: profundidad del pozo (feature)
- `tipoextraccion`: tipo de extracción, variable categórica encodada con LabelEncoder (feature)

### Features calculados

| Feature | Descripción | Justificación |
|---------|-------------|---------------|
| `avg_prod_gas_10m` | Promedio de prod_gas de las últimas 10 lecturas | Captura la tendencia reciente del pozo |
| `avg_prod_pet_10m` | Promedio de prod_pet de las últimas 10 lecturas | Ídem para petróleo |
| `last_prod_gas` | Última producción de gas conocida | Punto de partida para la predicción |
| `last_prod_pet` | Última producción de petróleo conocida | Ídem para petróleo |
| `n_readings` | Cantidad de lecturas acumuladas del pozo | Indica madurez del pozo: un pozo nuevo tiene features menos confiables |

### ¿Por qué `shift(1)`?

Todos los features de ventana se calculan con `shift(1)`, que desplaza los valores un período hacia adelante. Esto evita **data leakage**: cuando el modelo está parado en marzo 2020, solo puede ver datos hasta febrero 2020.

```python
df['avg_prod_gas_10m'] = df.groupby('idpozo')['prod_gas'].transform(
    lambda x: x.shift(1).rolling(10, min_periods=1).mean()
)
```

### Fila futura para el online store

Para cada pozo, se genera una fila extra con fecha del próximo mes y sin target (`prod_gas = None`, `prod_pet = None`). Esta fila es la que se materializa en el online store y representa el estado actual del pozo listo para inferencia.

---

## DAG: `ml_pipeline_oil_and_gas`

El DAG corre automáticamente el **primer día de cada mes** (`schedule="0 0 1 * *"`), alineado con la frecuencia natural del dataset. También se puede triggerear manualmente con parámetros opcionales `date_from` y `date_to`.

### Flujo de tasks

```
start
  ↓
download_dataset
  ↓
prepare_offline_store
  ↓
populate_online_store
  ↓
split_data
  ↓
train_model → evaluate_model (x10 experimentos, en serie)
  ↓
select_best_model
```

### Task 1: `download_dataset`

Descarga el CSV completo desde la URL del gobierno y lo guarda en `/opt/airflow/data/pozos.csv`. Se descarga completo cada vez para asegurar que los datos estén actualizados.

### Task 2: `prepare_offline_store`

Lee el CSV, aplica todas las transformaciones y genera el parquet del offline store. Transformaciones en orden:

1. Construye `fecha` a partir de `anio` y `mes`
2. Selecciona columnas relevantes y dropea nulos
3. Encodea `tipoextraccion` con `LabelEncoder`
4. Calcula features de ventana sobre el **dataset completo** (`groupby + rolling + shift`)
5. Aplica el filtro de fechas (`date_from` / `date_to`) después de los features
6. Genera la fila futura por pozo para el online store
7. Borra el registry y el SQLite antes de `feast apply` para garantizar consistencia
8. Guarda el parquet y ejecuta `feast apply`

### Task 3: `populate_online_store`

Lee el parquet, se queda con la última fila de cada pozo (la fila futura sin target) y la escribe en el SQLite via `write_to_online_store`. Siempre tiene exactamente una fila por pozo.

### Task 4: `split_data`

Obtiene los features históricos del offline store via Feast usando `get_historical_features` (point-in-time lookup). Descarta las filas futuras y divide en train/test con split temporal: 80% fechas más antiguas como train, 20% más recientes como test.

El split se hace acá y no en `train_model` para que todos los experimentos se evalúen sobre el mismo conjunto de test.

### Tasks 5 y 6: `train_model` y `evaluate_model`

Entrena **dos modelos independientes**: uno para `prod_gas` y otro para `prod_pet`. El loop recorre 10 experimentos en total (5 por target), variando `n_estimators`, `max_depth` y el conjunto de features.

`evaluate_model` loguea en MLFlow las métricas `mae`, `mse`, `rmse` y `r2`, y registra el modelo en el Model Registry bajo `oil_gas_prod_gas` u `oil_gas_prod_pet`.

### Task 7: `select_best_model`

Consulta MLFlow y compara **todas las versiones históricas acumuladas** de cada modelo — no solo las de la corrida actual. Promueve la versión con mejor `r2` global con el alias `"production"`:

- `oil_gas_prod_gas@production` → mejor modelo para gas
- `oil_gas_prod_pet@production` → mejor modelo para petróleo

---

## Relación entre el entrenamiento y la API

El rango de fechas elegido al triggerear el DAG condiciona directamente el comportamiento de la API. Entender esta relación es clave para interpretar correctamente las respuestas del endpoint `/forecast`.

Cuando el DAG corre con `date_from=2023-01-01` y `date_to=2023-12-31`:

- El **parquet** contiene features históricos para todos los pozos entre enero y diciembre de 2023, más una fila futura por pozo (enero 2024) con `prod_gas = None`.
- El **online store** queda con los features del estado más reciente de cada pozo — los correspondientes a diciembre 2023.
- El **modelo** fue entrenado con datos de 2023.

Esto define dos comportamientos en la API al momento de predecir:

**Fechas dentro del período de entrenamiento (ej: 2023-03-01):** la API encuentra esa fecha en el parquet con datos reales y usa sus features. Cada mes tiene su propio `avg_prod_gas_10m` calculado con producción real, por lo que las predicciones varían mes a mes. Esto permite evaluar el comportamiento del modelo sobre datos conocidos, pero no constituye una predicción genuina del futuro.

**Fechas posteriores al período de entrenamiento (ej: 2024-02-01):** la fecha no existe en el parquet (o existe como fila futura con target nulo). La API usa el online store — siempre los mismos features de diciembre 2023 — y devuelve la misma predicción para todos los meses del rango. Es el caso de uso principal del sistema: predecir producción futura a partir del estado más reciente del pozo.

**Fechas anteriores al período de entrenamiento (ej: 2022-02-01):** la fecha no existe en el parquet (o existe como fila futura con target nulo). La API devuelve un error (HTTP 400) y no realiza la predicción.

En resumen:

| Fecha pedida | Fuente de features | Comportamiento |
|---|---|---|
| Anterior al período de entrenamiento | - | Error (no permitido) |
| Dentro del período de entrenamiento | Offline store | Predicción basada en features históricos |
| Posterior al período de entrenamiento | Online store | Predicción basada en el estado más reciente |

---

## API REST

### `GET /api/v1/forecast`

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `id_well` | string | SI | Identificador del pozo |
| `date_start` | string (YYYY-MM-DD) | SI | Fecha de inicio del rango |
| `date_end` | string (YYYY-MM-DD) | SI | Fecha de fin del rango |
| `target` | string | NO | `"gas"` (default) o `"pet"` |

**Funcionamiento:** genera el rango mensual entre `date_start` y `date_end` y para cada mes decide la fuente de features según la tabla de la sección anterior. Devuelve un punto por cada mes del rango.

**Ejemplo de respuesta — 3 meses futuros (mismo valor repetido):**
```json
{
  "id_well": "3640",
  "data": [
    {
      "date": "2024-01-01",
      "prod": 435.73
    },
    {
      "date": "2024-02-01",
      "prod": 435.73
    },
    {
      "date": "2024-03-01",
      "prod": 435.73
    }
  ]
}
```

### `GET /api/v1/wells`

Devuelve el listado de pozos disponibles para una fecha dada (`date_query`). La fecha debe ser el primer día del mes (ej: `2023-01-01`) y debe estar dentro del período de entrenamiento — el endpoint consulta el parquet del offline store.

---

## Decisiones de Diseño

### 1. Orden del filtro de fechas en `prepare_offline_store`

El filtro se aplica después de calcular los features de ventana. Si se filtrara primero, el `avg_prod_gas_10m` de la primera fila del rango perdería todo el historial anterior. Así, aunque se entrene desde 2023, el rolling de enero 2023 igual refleja los 10 meses previos de 2022.

### 2. Split temporal en lugar de split aleatorio

Para series de tiempo, un split aleatorio introduce data leakage: el modelo podría entrenarse con datos de 2023 para predecir datos de 2020. El split temporal garantiza que el modelo solo ve el pasado durante el entrenamiento (80% fechas más antiguas = train, 20% más recientes = test).

### 3. Rango de fechas recomendado

| Contexto | Rango | Motivo |
|---|---|---|
| **Entorno local** | `2023-01-01` / `2023-12-31` | `get_historical_features` carga el parquet en memoria. Con 9 contenedores corriendo el worker dispone de ~1.5-2GB — insuficientes para más de un año de datos |
| **Producción** | `2021-01-01` / `2023-12-31` | Con un backend distribuido (BigQuery, Spark) Feast puede manejar el dataset completo sin OOM |

**Por qué 2021 como inicio:** 2020 fue atípico por COVID-19. A partir de 2021 Vaca Muerta retomó crecimiento sostenido. Además, las técnicas de completación cambiaron radicalmente entre 2012 y 2021 (de 1.500 a 2.500 lb de proppant por pie), por lo que datos anteriores representan una realidad operativa distinta que introduce ruido.

**Fuentes:**
- Ben Salah, A. (2025). The Impact of COVID‐19 on Oil and Natural Gas Production. *OPEC Energy Review*. https://onlinelibrary.wiley.com/doi/10.1111/opec.12321
- U.S. Energy Information Administration. (2024). *Argentina's crude oil and natural gas production near record highs*. https://www.eia.gov/todayinenergy/detail.php?id=63924
- Rystad Energy. (2023). *Argentina's Vaca Muerta shale patch could produce 1 million bpd in 2030*. https://www.rystadenergy.com/news/argentina-s-vaca-muerta-shale-patch
- AAPG Wiki. *Vaca Muerta play*. https://wiki.aapg.org/Vaca_Muerta_play

### 4. Dos modelos independientes: `prod_gas` y `prod_pet`

En pozos no convencionales, gas y petróleo no siempre están correlacionados según la formación geológica. Cada modelo tiene sus propios features de ventana, experimentos y alias `production` en MLFlow.

### 5. Alias `production` en lugar de stages en MLFlow

`transition_model_version_stage` está deprecado en versiones recientes de MLFlow. El alias permite que si el DAG vuelve a correr y encuentra un modelo mejor, el alias se mueve automáticamente — la API siempre sirve el mejor modelo sin cambiar el código. Además, `select_best_model` compara todas las versiones históricas acumuladas en MLFlow, no solo las de la última corrida.

### 6. `get_historical_features` de Feast para el entrenamiento

El entrenamiento consume del feature store vía `get_historical_features`, cumpliendo el requerimiento de la consigna. Esta función hace un point-in-time lookup que garantiza que para cada par `(idpozo, fecha)` se devuelven los features tal como eran en esa fecha exacta, evitando data leakage entre períodos.

### 7. Predicción autoregresiva descartada en la API

Para fechas futuras la API repite la misma predicción en lugar de actualizar `avg_prod_10m` con valores predichos. Alimentar el modelo con sus propias predicciones cambiaría la distribución del feature respecto al entrenamiento (distribution shift). En pozos shale con decline pronunciado, el error se autocorrelacionaría. Repetir la predicción del estado más reciente es más honesto respecto a las limitaciones de un modelo single-step.