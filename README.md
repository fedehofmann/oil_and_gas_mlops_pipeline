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

## Cómo reproducir el entrenamiento

Desde la UI de Airflow en http://localhost:8080, triggerear el DAG `ml_pipeline_oil_and_gas` con los parámetros:

- `date_from`: fecha de inicio del rango de entrenamiento (formato `YYYY-MM-DD`)
- `date_to`: fecha de fin del rango de entrenamiento (formato `YYYY-MM-DD`)

**Rango recomendado para entornos locales:** `date_from=2023-01-01`, `date_to=2023-12-31`.

Ver sección [Decisiones de Diseño](#decisiones-de-diseño) para la justificación completa del rango de fechas.

---

## Decisiones de Diseño

### 1. Rango de fechas de entrenamiento: local vs. producción

| Contexto | Rango | Motivo |
|---|---|---|
| **Entorno local** | `2023-01-01` / `2023-12-31` | `get_historical_features` de Feast carga el parquet completo en memoria para el point-in-time join. Con 9 contenedores corriendo, el worker dispone de ~1.5-2GB libres — insuficientes para más de un año de datos |
| **Producción** | `2021-01-01` / `2023-12-31` | Con un backend distribuido (BigQuery, Spark), Feast puede manejar el dataset completo sin OOM |

**Por qué 2021 como inicio en producción y no antes:** 2020 fue atípico por COVID-19 (caída de producción documentada en 14 países). A partir de 2021 Vaca Muerta retomó crecimiento sostenido. Además, las técnicas de completación cambiaron radicalmente entre 2012 y 2021 (de 1.500 a 2.500 lb de proppant por pie; costos de USD 20M a USD 11M por pozo), por lo que datos de pozos anteriores representan una realidad operativa distinta e introducen ruido.

Los features de ventana (`avg_prod_gas_10m`, `last_prod_gas`) igualmente incorporan el historial completo previo a `date_from` gracias al orden de operaciones en `prepare_offline_store`.

**Fuentes:**

*Sobre el impacto de COVID-19 en 2020:*
- Ben Salah, A. (2025). The Impact of COVID‐19 on Oil and Natural Gas Production. *OPEC Energy Review*. Wiley. https://onlinelibrary.wiley.com/doi/10.1111/opec.12321 — Confirma impacto negativo en producción de petróleo y gas en 14 países productores entre enero 2020 y diciembre 2021.
- U.S. Energy Information Administration. (2024). *Argentina's crude oil and natural gas production near record highs*. https://www.eia.gov/todayinenergy/detail.php?id=63924 — Reporta que la producción de Vaca Muerta retomó crecimiento sostenido recién a partir de 2021.

*Sobre la madurez de Vaca Muerta a partir de 2021:*
- Rystad Energy. (2023). *Argentina's Vaca Muerta shale patch could produce 1 million bpd in 2030*. https://www.rystadenergy.com/news/argentina-s-vaca-muerta-shale-patch — Señala que el desarrollo regional se aceleró en 2021 post-COVID y que las proyecciones toman como referencia el desempeño de pozos completados en 2021-2022.
- OilPrice.com. (2023). *Vaca Muerta's Sweet Crude Attracts Global Energy Giants*. https://oilprice.com/Energy/Crude-Oil/Vaca-Muertas-Sweet-Crude-Attracts-Global-Energy-Giants.html — Para junio 2023, Vaca Muerta producía 296.577 bbl/día de shale oil, un 24% más que el mismo período de 2022.

*Sobre la evolución de técnicas de completación (pozos 2012-2013 no son comparables a 2021+):*
- Rystad Energy. (2023). Op. cit. — Documenta la adopción de la filosofía "bigger-is-better": de 1.500 a 2.500 lb de proppant por pie y reducción del espaciado entre etapas de 250 a 210 pies entre 2018 y 2022.
- AAPG Wiki. *Vaca Muerta play*. https://wiki.aapg.org/Vaca_Muerta_play — Costos de completación cayeron de USD 20-25M por pozo (2012) a USD 11-12M (2020) por cambios tecnológicos; los datos de pozos tempranos reflejan una realidad operativa distinta.
- Pan American Energy. (2021). Applying State-of-the-Art Completion Techniques in Vaca Muerta Formation. *SPE/AAPG/SEG URTC*. https://onepetro.org/URTECONF/proceedings-abstract/21URTC/1-21URTC/D011S007R002/465470
- Mawad, D. et al. (2023). From Exploration to Development: The Completion Evolution in Vaca Muerta. SPE-212574-MS. https://www.academia.edu/115677720

*Contraargumento considerado — los modelos de decline curve se benefician de historiales largos:*
- Lei, Z. et al. (2024). Production decline curve analysis of shale oil wells: A case study of Bakken, Eagle Ford and Permian. *ScienceDirect*. https://www.sciencedirect.com/science/article/pii/S1995822624002139 — Pozos con más de 2 años de historial permiten mayor precisión en la estimación del EUR con modelos tipo SEPD + Arps.
- MDPI Energies. (2024). An Improved Decline Curve Analysis Method via Ensemble Learning for Shale Gas Reservoirs. https://www.mdpi.com/1996-1073/17/23/5910 — El modelo SEPD requiere datos históricos sustanciales para estimar parámetros de decline de forma confiable.

> **Nota:** Este contraargumento es válido para modelos de decline curve clásicos. En este trabajo se usa un modelo de ML supervisado (RandomForest), donde la heterogeneidad tecnológica entre pozos de distintas épocas introduce ruido que puede perjudicar la generalización. Se priorizó la homogeneidad del período de entrenamiento sobre la extensión del historial.

### 2. Orden del filtro de fechas en `prepare_offline_store`

El filtro se aplica **después** de calcular los features de ventana, no antes. Si se filtrara primero, el `avg_prod_gas_10m` de la primera fila del rango solo tendría contexto desde `date_from`, perdiendo todo el historial anterior. El orden correcto garantiza que el rolling usa el dataset completo antes de recortar.

### 3. Split temporal en lugar de split aleatorio

Para series de tiempo, un split aleatorio introduce data leakage: el modelo podría entrenarse con datos de 2023 para predecir datos anteriores. El split temporal garantiza que el modelo solo ve el pasado durante el entrenamiento (80% fechas más antiguas = train, 20% más recientes = test).

### 4. `get_historical_features` de Feast para el entrenamiento

El entrenamiento consume del feature store vía `get_historical_features`, que hace un point-in-time lookup: para cada par `(idpozo, fecha)` devuelve los features tal como eran en esa fecha, evitando data leakage entre períodos. El online store (SQLite) se usa para inferencia.

### 5. Dos modelos independientes: `prod_gas` y `prod_pet`

En pozos no convencionales, la producción de gas y petróleo no siempre están correlacionadas — un pozo puede ser predominantemente gasífero o petrolífero según la formación geológica. Mezclar features de un fluido para predecir el otro introduciría ruido. Cada modelo tiene sus propios features de ventana, sus experimentos y su alias `production` en MLFlow.

### 6. Alias `production` en lugar de stages en MLFlow

`transition_model_version_stage` está deprecado en versiones recientes de MLFlow. El alias `"production"` es el mecanismo recomendado y permite cargar el modelo con `mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production")`.

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

El DAG orquesta todo el pipeline. Corre automáticamente el **primer día de cada mes** (`schedule="0 0 1 * *"`), alineado con la frecuencia natural del dataset (producción mensual por pozo). También se puede triggerear manualmente desde la UI de Airflow con parámetros opcionales `date_from` y `date_to` para filtrar el dataset por rango de fechas, lo que permite reproducir el entrenamiento para cualquier fecha histórica con un solo comando.

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
3. Encodea `tipoextraccion` con `LabelEncoder` (texto → número)
4. Calcula los features de ventana con pandas vectorizado (`groupby + rolling + shift`) sobre el **dataset completo**
5. **Aplica el filtro de fechas** (`date_from` / `date_to`) recién aquí, después de los features
6. Genera la fila futura por pozo para el online store
7. Guarda el parquet en `/opt/airflow/feature_store/data/well_features.parquet`
8. Ejecuta `feast apply` para registrar el parquet en el registry de Feast

**Decisión de diseño — orden del filtro de fechas:** El filtro se aplica *después* del cálculo de features para evitar data leakage. Si se filtrara primero, el rolling de la primera fila del rango solo tendría contexto desde `date_from`, perdiendo todo el historial anterior. Por ejemplo, si se entrena con datos desde 2021, el `avg_prod_gas_10m` de enero 2021 igual refleja los 10 meses previos (2020), no solo los datos del rango de entrenamiento.

**Decisión de diseño — pandas vectorizado:** Se usa `groupby + rolling` en lugar de un loop por pozo. Pandas procesa todas las filas internamente sin iterar en Python puro, lo que es significativamente más eficiente con datasets de cientos de miles de filas.

### Task 3: `populate_online_store`

Lee el parquet, se queda con la última fila de cada pozo (la fila futura sin target) y la escribe en el SQLite via `write_to_online_store`.

**Decisión de diseño:** Corre después de `prepare_offline_store` para garantizar que el parquet ya existe. El online store siempre tiene exactamente una fila por pozo.

**Decisión de diseño — borrado del registry y el SQLite antes de `feast apply`:** Al inicio de `prepare_offline_store` se borran `registry/registry.db` y `online_store/online.db` antes de correr `feast apply`. Esto garantiza que el online store siempre quede sincronizado con el parquet de la corrida actual.

Sin este borrado, pueden ocurrir dos problemas encadenados:
1. **Datos rancios en el online store:** Feast no sobreescribe entradas cuyo timestamp almacenado sea más reciente que el nuevo dato. Si una corrida anterior usó un rango de fechas más amplio (timestamps más nuevos), los valores viejos persisten aunque el parquet haya cambiado.
2. **"no such table" en `populate_online_store`:** `feast apply` solo crea las tablas del SQLite si detecta cambios en el registry. Si el registry dice que la infraestructura ya existe, no recrea las tablas aunque el SQLite haya sido borrado — y `write_to_online_store` falla. Borrando también el registry, `feast apply` trata todo como instalación nueva y recrea las tablas correctamente.

### Task 4: `split_data`

Obtiene los features históricos del offline store via Feast usando `get_historical_features`, que hace un **point-in-time lookup**: para cada par `(idpozo, fecha)`, devuelve los features tal como eran en esa fecha exacta, garantizando que no hay data leakage entre el pasado y el futuro.

Después descarta las filas futuras (`prod_gas = None`) y divide en train/test con un **split temporal**.

**Decisión de diseño — split temporal en lugar de split aleatorio:** Para series de tiempo, un split aleatorio introduce data leakage: el modelo podría entrenarse con datos de 2023 para predecir datos de 2020. El split temporal garantiza que el modelo solo ve datos pasados durante el entrenamiento. Se usa el 80% de fechas más antiguas como train y el 20% más reciente como test.

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
1. Genera el rango mensual entre `date_start` y `date_end` (primer día de cada mes)
2. Para cada fecha del rango, elige la fuente de features:
   - **Fecha dentro del parquet (histórica):** usa los features reales del offline store. `avg_prod_10m` fue calculado con `shift(1)` en `prepare_offline_store`, por lo que no contiene información de la fecha consultada ni de fechas posteriores (sin leakage)
   - **Fecha futura (posterior al último dato del parquet):** usa los features del online store (estado más reciente del pozo). El modelo predice un solo paso; se repite la misma predicción para todos los meses futuros sin actualización autoregresiva, para evitar distribution shift en `avg_prod_10m`
3. Devuelve un punto por cada mes del rango

**Ejemplo: rango histórico (pozo 3640, enero–junio 2023)**

Los primeros dos meses tienen registros reales en el parquet; a partir de marzo el pozo ya no tiene datos y la API usa el online store repitiendo la misma predicción:

```json
{
  "id_well": "3640",
  "data": [
    { "date": "2023-01-01", "prod": 27.8  },
    { "date": "2023-02-01", "prod": 25.02 },
    { "date": "2023-03-01", "prod": 27.8  },
    { "date": "2023-04-01", "prod": 27.8  },
    { "date": "2023-05-01", "prod": 27.8  },
    { "date": "2023-06-01", "prod": 27.8  }
  ]
}
```

Enero y febrero varían porque cada mes usa sus propios features históricos (producción real de los meses anteriores). A partir de marzo, el pozo no tiene más registros en el dataset: la API toma el estado más reciente del online store y repite la misma predicción para todos los meses futuros del rango.

**Cómo identificar el límite en la respuesta:** el primer valor que se repite consecutivamente con el mismo número indica la transición del offline store al online store para ese pozo.

**Decisión de diseño — predicción autoregresiva descartada:** Actualizar `avg_prod_10m` con valores predichos introduce distribution shift (el feature fue calculado sobre producción real en entrenamiento). En pozos shale con decline pronunciado, el error se autocorrela. La alternativa de repetir la misma predicción para fechas futuras es más honesta respecto a las limitaciones del modelo single-step.

**Nota sobre consistencia del online store:** En versiones anteriores del código se observaba una diferencia llamativa entre la predicción del último mes histórico y la del primer mes futuro (online store), causada por datos rancios en el SQLite de corridas anteriores. Este comportamiento fue corregido: `prepare_offline_store` borra el registry y el SQLite antes de cada `feast apply`, garantizando que el online store siempre refleje el estado actual del parquet.

**Limitación conocida — el modelo tiende a converger a la media del dataset para fechas futuras:** Al consultar fechas futuras (online store), pozos con perfiles muy distintos pueden recibir la misma predicción. Esto fue verificado empíricamente:

| idpozo | avg_prod_gas_10m | last_prod_gas | pred online store |
|--------|-----------------|---------------|-------------------|
| 164588 | 15.263 Mm³ | 20.236 Mm³ | 611.19 |
| 163700 | 15.579 Mm³ | 12.050 Mm³ | 611.19 |
| 3640   | 7.5 Mm³    | 0.0 Mm³    | 611.19 |

Los features del online store son correctos y distintos para cada pozo — el problema está en el modelo. RandomForest tiende a predecir valores cercanos a la media del dataset de entrenamiento cuando los inputs están fuera de la distribución vista durante el entrenamiento, o cuando el dataset de entrenamiento tiene alta concentración de filas en ese rango de producción. 611.19 es esencialmente el valor promedio de `prod_gas` en el dataset de entrenamiento.

Esto es una limitación del modelo (no del pipeline) y tiene dos causas posibles:
1. **Distribución sesgada del dataset:** la mayoría de los pozos en el dataset de entrenamiento producen en el rango 500-700 Mm³/mes, lo que ancla las predicciones del RandomForest hacia esa zona.
2. **Features insuficientes para diferenciar pozos en el online store:** el modelo aprendió a distinguir pozos usando su historia reciente, pero cuando esa historia no está disponible (online store usa una sola fila), la capacidad discriminativa se reduce.

Para la entrega final se evaluará si agregar features adicionales (formación geológica, empresa operadora, ubicación) o cambiar el modelo mejora la diferenciación en inferencia futura.

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

