# TP Final - IA en Producción: Pipeline de Pronóstico de Producción de Hidrocarburos

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
    ├── GET /api/v1/forecast → pronóstico de producción de un pozo (gas o petróleo - a elección)
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
![GET Listado Pozo](screenshots/GET_Listado_Pozo.png)

### API REST - Endpoint `/api/v1/forecast`
![GET Forecast](screenshots/GET_Forecast.png)

---

## Setup

### 1. Requisitos

- Docker y Docker Compose
- Python 3.9+

### 2. `.gitignore` recomendado

Este repositorio solo versiona el código fuente (`.py`, `.yaml`, `.yml`, `.md`) necesario para entender y reproducir el proyecto. Las carpetas generadas automáticamente al correr el DAG no se suben al repo ya que pueden pesar varios GBs y se regeneran solas con un solo `docker compose up`.

Antes de hacer el primer commit, asegurate de tener un `.gitignore` en la raíz con al menos:

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
AIRFLOW_UID=501
_PIP_ADDITIONAL_REQUIREMENTS=pandas scikit-learn mlflow feast fastapi uvicorn==0.40.0
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
  - ./mlruns:/mlflow/mlruns # Persiste artefactos de MLFlow
  - ./feature_store:/opt/airflow/feature_store # Expone el feature store al contenedor
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
# Paths absolutos del contenedor Docker
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

El sistema tarda aproximadamente 2-3 minutos en estar completamente operativo (Airflow necesita inicializar su base de datos).

### 8. Acceso a los servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| MLFlow UI | http://localhost:9090 | - |
| API Swagger | http://localhost:8000/docs | - |

---

## Nota sobre MLFlow y seguridad de red

MLFlow 3.5+ incluye un middleware de seguridad que por defecto solo acepta conexiones desde localhost. Para permitir conexiones entre contenedores Docker es necesario deshabilitar este middleware con la variable de entorno `MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE=true` y arrancar el servidor con `--host 0.0.0.0`. Esto está configurado en el `docker-compose.yaml`.

---

## Feature Store

### ¿Por qué un Feature Store?

Cuando el modelo necesita predecir la producción de un pozo, requiere features como el promedio de producción de los últimos 10 meses. Calcularlo en el momento de cada inferencia sería lento y costoso. El feature store resuelve esto separando el problema en dos partes:

- **Offline Store**: almacena los features históricos de todos los pozos en todos los períodos. Se usa para entrenamiento.
- **Online Store**: almacena solo el estado más reciente de cada pozo. Se usa para inferencia. Es rápido porque los features ya están precomputados.

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

### Materialización

Es el proceso de copiar los features más recientes del offline store al online store. Se ejecuta una vez por mes junto con el reentrenamiento. En Feast se hace con `write_to_online_store`.

### `features.py`

Define el contrato de features que Feast espera encontrar en el parquet. Tiene tres componentes:

- **Entity**: el "sujeto" de los features. En este caso el pozo (`idpozo`). Feast necesita saber quién es el protagonista para buscar features por ID.
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

Además de las columnas del dataset original, se calculan features de ventana que capturan el comportamiento reciente de cada pozo:

| Feature | Descripción | Justificación |
|---------|-------------|---------------|
| `avg_prod_gas_10m` | Promedio de prod_gas de las últimas 10 lecturas | Captura la tendencia reciente del pozo |
| `avg_prod_pet_10m` | Promedio de prod_pet de las últimas 10 lecturas | Ídem para petróleo |
| `last_prod_gas` | Última producción de gas conocida | Punto de partida para la predicción |
| `last_prod_pet` | Última producción de petróleo conocida | Ídem para petróleo |
| `n_readings` | Cantidad de lecturas acumuladas del pozo | Indica madurez del pozo: un pozo nuevo tiene features menos confiables |

### ¿Por qué `shift(1)`?

Todos los features de ventana se calculan con `shift(1)`, que desplaza los valores un período hacia adelante. Esto evita **data leakage**: cuando el modelo está parado en marzo 2020, solo puede ver datos hasta febrero 2020. Sin el shift, estaría usando datos del mes actual para predecir ese mismo mes.

```python
df['avg_prod_gas_10m'] = df.groupby('idpozo')['prod_gas'].transform(
    lambda x: x.shift(1).rolling(10, min_periods = 1).mean()
)
```

### Fila futura para el online store

Para cada pozo, se genera una fila extra con fecha del próximo mes y sin target (`prod_gas = None`, `prod_pet = None`). Esta fila es la que se materializa en el online store y representa el estado actual del pozo listo para inferencia.

---

## DAG: `ml_pipeline_oil_and_gas`

El DAG orquesta todo el pipeline. Se puede triggerear desde la UI de Airflow con parámetros opcionales `date_from` y `date_to` para filtrar el dataset por rango de fechas, lo que permite reproducir el entrenamiento para cualquier fecha histórica con un solo comando.

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

Descarga el CSV desde la URL del gobierno y lo guarda en `/opt/airflow/data/pozos.csv`.

**Decisión de diseño:** Se descarga el CSV completo cada vez que corre el DAG para asegurar que los datos estén actualizados. El filtro por fechas se aplica después en `prepare_offline_store`.

### Task 2: `prepare_offline_store`

Lee el CSV, aplica todas las transformaciones y genera el parquet del offline store. Al finalizar ejecuta `feast apply` para registrar las definiciones en el registry.

**Transformaciones en orden:**
1. Construye la columna `fecha` a partir de `anio` y `mes`
2. Selecciona las columnas relevantes y dropea nulos
3. Filtra por `date_from` y `date_to` si se especificaron (usando `.pipe()` para mantener el method chaining)
4. Encodea `tipoextraccion` con `LabelEncoder` (texto → número)
5. Calcula los features de ventana con pandas vectorizado (`groupby + rolling + shift`)
6. Genera la fila futura por pozo para el online store
7. Guarda el parquet en `/opt/airflow/feature_store/data/well_features.parquet`
8. Ejecuta `feast apply` para registrar el parquet en el registry de Feast

**Decisión de diseño:** Se usa pandas vectorizado (`groupby + rolling`) en lugar de un loop por pozo. Esto es más eficiente porque pandas procesa todas las filas en paralelo internamente, evitando la sobrecarga de iterar fila por fila en Python.

### Task 3: `populate_online_store`

Lee el parquet, se queda con la última fila de cada pozo (la fila futura sin target) y la escribe en el SQLite via `write_to_online_store`.

**Decisión de diseño:** Corre después de `prepare_offline_store` para garantizar que el parquet ya existe. El online store siempre tiene exactamente una fila por pozo.

### Task 4: `split_data`

Obtiene los features históricos del offline store via Feast usando `get_historical_features`, que hace un **point-in-time lookup**: para cada fila del `entity_df`, busca los features tal como eran en esa fecha, evitando data leakage.

Después dropea las filas futuras (`prod_gas = None`), define `X` e `y` para cada target, y splitea en train/test con `random_state=42` para reproducibilidad.

**Por qué el split se hace acá y no en `train_model`:** Todos los experimentos se evalúan sobre el mismo conjunto de test. Si el split se hiciera dentro de cada `train_model`, cada experimento podría tener un test set distinto y las métricas no serían comparables.

### Tasks 5 y 6: `train_model` y `evaluate_model`

El DAG entrena **dos modelos completamente independientes**: uno para predecir `prod_gas` y otro para predecir `prod_pet`. No es un modelo que predice ambos targets a la vez. Cada modelo tiene sus propios experimentos, sus propias versiones en el Model Registry y su propio alias `production` en MLFlow.

El loop recorre 10 experimentos en total: 5 para `prod_gas` y 5 para `prod_pet`. Cada experimento varía `n_estimators`, `max_depth` y el conjunto de features. Por cada experimento, `train_model` entrena el modelo y `evaluate_model` lo evalúa y loguea en MLFlow.

**Features por target:**

Para `prod_gas`:
- `tipoextraccion`, `tef`, `profundidad`, `prod_agua`
- `avg_prod_gas_10m`, `last_prod_gas`, `n_readings`

Para `prod_pet`:
- `tipoextraccion`, `tef`, `profundidad`, `prod_agua`
- `avg_prod_pet_10m`, `last_prod_pet`, `n_readings`

**Decisión de diseño sobre features separadas:** `avg_prod_gas_10m` no entra como feature para predecir `prod_pet` y viceversa. En pozos no convencionales, la producción de gas y petróleo no siempre están correlacionadas: un pozo puede ser predominantemente gasífero o petrolífero dependiendo de la formación geológica. Mezclar las features de un fluido para predecir el otro podría introducir ruido en lugar de señal.

`evaluate_model` loguea en MLFlow las métricas `mae`, `mse`, `rmse` y `r2`, y registra el modelo bajo el nombre `oil_gas_prod_gas` u `oil_gas_prod_pet` según el target. Cada run agrega una nueva versión al modelo registrado correspondiente.

**Cómo se ve en MLFlow:**
```
Experimento: ml_pipeline_oil_and_gas
  ├── Run: prod_gas_est50_depthNone_featall → versión 1 de oil_gas_prod_gas
  ├── Run: prod_gas_est100_depth5_featall → versión 2 de oil_gas_prod_gas
  ├── Run: prod_pet_est50_depthNone_featall → versión 1 de oil_gas_prod_pet
  └── ...

Model Registry:
  ├── oil_gas_prod_gas → versiones 1 a 5
  └── oil_gas_prod_pet → versiones 1 a 5
```

### Task 7: `select_best_model`

Corre **una sola vez al final** de todos los experimentos. Consulta MLFlow, compara todas las versiones registradas de cada modelo por `r2`, y promueve la mejor usando `set_registered_model_alias` con el alias `"production"`.

El resultado son **dos modelos en producción**:
- `oil_gas_prod_gas@production` → el mejor modelo para predecir gas
- `oil_gas_prod_pet@production` → el mejor modelo para predecir petróleo

**Decisión de diseño:** Se usa alias en lugar de stages porque `transition_model_version_stage` está deprecado en versiones recientes de MLFlow. El alias `"production"` permite cargar el modelo desde la API con:

```python
model = mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production")
```

---

## API REST

La API expone dos endpoints conforme a la especificación OpenAPI del enunciado.

### `GET /api/v1/forecast`

Devuelve el pronóstico de producción de un pozo para el próximo mes.

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `id_well` | string | SI | Identificador del pozo |
| `date_start` | string (YYYY-MM-DD) | SI | Fecha de inicio del rango |
| `date_end` | string (YYYY-MM-DD) | SI | Fecha de fin del rango |
| `target` | string | NO | `"gas"` (default) o `"pet"` |

**Funcionamiento:**
1. Consulta el online store de Feast para obtener los features más recientes del pozo
2. Selecciona el modelo en producción según el target: `oil_gas_prod_gas@production` o `oil_gas_prod_pet@production`
3. Predice y devuelve el resultado

**Ejemplo de respuesta:**
```json
{
  "id_well": "3640",
  "target": "gas",
  "data": [
    {
      "date": "2017-02-01",
      "prod": 498.94
    }
  ]
}
```

**Decisión de diseño:** Para la entrega parcial se devuelve solo el próximo mes disponible independientemente del rango solicitado. Esto está documentado en el swagger.

### `GET /api/v1/wells`

Devuelve el listado de pozos disponibles para una fecha dada.

**Parámetros:** `date_query`

**Funcionamiento:** Consulta el parquet del offline store y devuelve los pozos que tienen registros para la fecha solicitada.

---

## MLFlow

MLFlow trackea todos los experimentos con las siguientes métricas:

- `mae`: Mean Absolute Error
- `mse`: Mean Squared Error
- `rmse`: Root Mean Squared Error
- `r2`: R² Score

Cada run tiene un nombre descriptivo que incluye el target, los estimadores, la profundidad máxima y si se usaron features completas o reducidas. Ejemplo: `prod_gas_est100_depth5_featall`.

El modelo con mejor `r2` por target queda taggeado con el alias `production` en el Model Registry.

---

## Cómo reproducir el entrenamiento

Desde la UI de Airflow en http://localhost:8080, triggerear el DAG `ml_pipeline_oil_and_gas` con los parámetros:

- `date_from`: fecha de inicio del dataset (opcional, formato `YYYY-MM-DD`)
- `date_to`: fecha de fin del dataset (opcional, formato `YYYY-MM-DD`)

Si no se especifican fechas, se usa el dataset completo.