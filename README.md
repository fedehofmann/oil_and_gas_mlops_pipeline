# TP Inteligencia Artificial En Producción - Pipeline De Pronóstico De Producción De Hidrocarburos

## Descripción

Este proyecto implementa un pipeline completo de Machine Learning en producción para pronosticar la **producción mensual de gas y petróleo por pozo** en yacimientos no convencionales (Vaca Muerta, Argentina). El sistema integra Airflow para orquestación, MLFlow para tracking de experimentos, Feast como feature store, y una API REST para consumo externo.

### Objetivo

Dado el historial de producción de un pozo, predecir cuántos m³ de gas o petróleo producirá ese pozo en un mes dado. El sistema entrena y actualiza los modelos mensualmente de forma automática, expone las predicciones a través de una API REST, y mantiene trazabilidad completa de experimentos y versiones de modelo.

### Datos

El RFC especifica dos datasets del [Ministerio de Energía de Argentina](http://datos.energia.gob.ar):

- **Dataset 1 — Producción por pozo** ✅ en uso: lecturas mensuales de producción de gas, petróleo y agua por pozo no convencional.
- **Dataset 2 — Listado de pozos** ❌ pendiente de integración: metadata estática por pozo (empresa operadora, formación geológica, cuenca, coordenadas). Ver [issue #18](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/18).

El dataset 2 es especialmente relevante para resolver la limitación documentada del modelo: al usar features estáticos del pozo (formación, cuenca) en inferencia futura, el modelo podría diferenciar pozos mejor que con el estado actual donde converge a la media. Las variables principales del dataset 1 son:

| Variable | Tipo | Rol |
|---|---|---|
| `prod_gas` | Numérica (m³/mes) | **Target** — producción de gas |
| `prod_pet` | Numérica (m³/mes) | **Target** — producción de petróleo |
| `tipoextraccion` | Categórica | Feature — tipo de extracción (ej: shale, tight) |
| `profundidad` | Numérica (m) | Feature — profundidad del pozo |
| `tef` | Numérica (días) | Feature — tiempo efectivo de flujo en el mes |
| `prod_agua` | Numérica (m³/mes) | Feature — producción de agua asociada |
| `avg_prod_gas_10m` | Numérica | Feature calculado — promedio de producción de gas de los últimos 10 meses |
| `avg_prod_pet_10m` | Numérica | Feature calculado — ídem para petróleo |
| `last_prod_gas` | Numérica | Feature calculado — última producción de gas conocida |
| `last_prod_pet` | Numérica | Feature calculado — ídem para petróleo |
| `n_readings` | Entera | Feature calculado — cantidad de lecturas acumuladas del pozo (proxy de madurez) |

Se recomienda filtrar el dataset a partir de 2021 pasando `date_from=` al triggerear el DAG, para excluir la distorsión de COVID-19 (2020) y la heterogeneidad tecnológica de pozos anteriores a la maduración de Vaca Muerta. El filtro no es automático — ver [Cómo reproducir el entrenamiento](#cómo-reproducir-el-entrenamiento) para los rangos recomendados según el entorno.

### Modelo y métricas

Se entrenan dos modelos independientes (`prod_gas` y `prod_pet`) usando **RandomForestRegressor**. Se evalúan 10 experimentos por target variando `n_estimators`, `max_depth` y el conjunto de features. El modelo con mejor score es promovido automáticamente a producción en MLFlow.

Las métricas de evaluación son **R²** (coeficiente de determinación), **RMSE** y **MAE** sobre un test set temporal (20% de fechas más recientes).

---

## Índice

- [Arquitectura](#arquitectura)
- [Posicionamiento MLOps](#posicionamiento-mlops)
  - [Arquitectura FTI](#arquitectura-fti)
  - [Nivel de madurez](#nivel-de-madurez)
  - [Deuda técnica identificada](#deuda-técnica-identificada)
  - [Reproducibilidad](#reproducibilidad)
- [Screenshots](#screenshots)
- [Setup](#setup)
- [Cómo reproducir el entrenamiento](#cómo-reproducir-el-entrenamiento)
- [Decisiones de Diseño](#decisiones-de-diseño)
- [Feature Store](#feature-store)
- [Features del Modelo](#features-del-modelo)
- [DAG: `ml_pipeline_oil_and_gas`](#dag-ml_pipeline_oil_and_gas)
- [API REST](#api-rest)
- [MLFlow](#mlflow)

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

## Posicionamiento MLOps

### Arquitectura FTI

El proyecto implementa el patrón **Feature → Training → Inference (FTI)**, que separa el sistema en tres pipelines con responsabilidades distintas:

| Pipeline | Responsabilidad | Implementación en este proyecto |
|---|---|---|
| **Feature Pipeline** | Ingerir, transformar y almacenar features | Tareas `download_dataset` + `prepare_offline_store` + `populate_online_store` del DAG |
| **Training Pipeline** | Leer features históricos, entrenar y guardar el modelo | Tareas `split_data` + `train_model` + `evaluate_model` + `select_best_model` del DAG |
| **Inference Pipeline** | Usar el modelo y features en tiempo real para predecir | API REST (FastAPI) + online store (SQLite) |

El Feature Store es el contrato entre los tres pipelines: garantiza que training e inference lean exactamente las mismas features con la misma lógica de transformación, eliminando el **training-serving skew** (problema que ocurre cuando el modelo se entrena con features calculados de una forma pero en producción se calculan de otra, lo que genera predicciones sesgadas aunque el modelo en sí sea correcto) por inconsistencia de datos.

### Nivel de madurez

Este proyecto implementa **Nivel 1 de MLOps (Continuous Training)** según la clasificación de Google:

| Característica | Nivel 0: Manual | Nivel 1: CT | Nivel 2: CI/CD | Este proyecto |
|---|---|---|---|---|
| Construcción del modelo | Manual (notebooks) | Automatizada | Automatizada | **Automatizada** (DAG en Airflow) |
| Entrenamiento | Manual | Automatizado (CT) | Automatizado (CT) | **Automatizado** (schedule mensual) |
| Feature Store | No | Sí | Sí | **Sí** (Feast con offline + online store) |
| Gestión de metadatos | No | Sí | Sí | **Sí** (MLFlow Model Registry) |
| Despliegue | Manual | Manual/Scripts | Automatizado | **Manual** (API levantada con Docker) |
| CI/CD | No | No | Integración total | **No** |
| Monitoreo | No | Métricas básicas | Métricas de sistema y modelo | **Parcial** (métricas de entrenamiento en MLFlow) |

Para alcanzar **Nivel 2** se necesitaría agregar: tests automáticos por cada commit (datos, modelo e infraestructura), canary o blue-green deployment (estrategias de despliegue que sirven el modelo nuevo solo a una fracción del tráfico antes de reemplazar el anterior por completo, reduciendo el riesgo de rollouts fallidos), y monitoreo activo en producción (prediction bias, feature distribution shift).

### Deuda técnica identificada

Aplicando el checklist de Sculley et al. (2015) al proyecto:

| Pregunta | Estado |
|---|---|
| ¿Puedo describir qué features usa este modelo sin leer el código? | ✅ Features documentadas en `features.py` y en este README |
| ¿Puedo reentrenar con un solo comando? | ✅ Un trigger desde la UI de Airflow corre el pipeline completo |
| ¿Cuántos lenguajes distintos necesito para el pipeline? | ✅ Python únicamente |
| ¿Hay tests para los datos de entrada? | ❌ No hay validación de schema del CSV descargado |

**Deuda de modelo conocida:** RandomForest tiende a converger a la media del dataset cuando los inputs del online store están fuera de la distribución de entrenamiento. Ver [limitación documentada en la sección API REST](#get-apiv1forecast). Esta deuda es del modelo, no del pipeline: el feature store entrega los features correctos, pero el modelo carece de capacidad discriminativa suficiente para diferenciar pozos en inferencia futura con una sola fila de contexto.

**Deuda de monitoreo:** No hay detección automática de **prediction bias** (tendencia sistemática del modelo a sobreestimar o subestimar respecto a los valores reales) ni **feature distribution shift** (cambio en la distribución estadística de los features de entrada respecto a lo visto durante el entrenamiento, que puede degradar silenciosamente la calidad de las predicciones). El pipeline detectaría degradación del modelo solo al comparar el `r2` (R² o coeficiente de determinación: mide qué proporción de la varianza del target explica el modelo; 1.0 es predicción perfecta, 0 equivale a predecir siempre la media del dataset) del próximo reentrenamiento mensual contra versiones anteriores en MLFlow.

**Deuda de artefactos:** El `LabelEncoder` de `tipoextraccion` se re-entrena en cada corrida dentro de `prepare_offline_store` pero no se guarda como artefacto en MLFlow junto al modelo. Usando la taxonomía de transformaciones de la Clase 3: `prepare_offline_store` aplica transformaciones *model-independent* (filtrado, agrupación por pozo/mes, agregaciones) que son reutilizables y se almacenan en el feature store. El `LabelEncoder` es una transformación *model-dependent* (codificación categórica parametrizada por los datos de entrenamiento) que debería persistirse en MLFlow junto al modelo. Si el CSV upstream incorpora nuevas categorías de extracción entre una corrida y la siguiente, el mapeo puede cambiar, generando training-serving skew latente.

**Deuda de Point-in-Time:** El pipeline construye el dataset de entrenamiento con un join convencional entre features y fechas. No se aplica corrección *point-in-time*, lo que significa que no se reconstruye el estado exacto de cada pozo justo antes del evento de entrenamiento. Si el CSV upstream actualiza retroactivamente filas históricas (el gobierno republica datos corregidos), el modelo podría haberse entrenado con información que no existía en el momento del evento, introduciendo **data leakage** implícito (filtración de información del futuro hacia el pasado durante el entrenamiento: el modelo aprende patrones que no podría haber visto en producción, lo que infla artificialmente las métricas de evaluación y genera un modelo que rinde peor de lo esperado en producción).

**Deuda de sesgos:** El modelo tiene tres sesgos estructurales no mitigados:

1. **Sesgo histórico en los datos:** El dataset refleja decisiones operativas pasadas, no la capacidad de producción real de cada pozo. Pozos con mayor historial de inversión o mantenimiento aparecen con features de ventana (`avg_prod_gas_10m`, `last_prod_gas`) inflados respecto a su producción basal. El modelo aprende esas condiciones operativas codificadas, no la geología del pozo. Esto es sesgo de medición: la variable medida (producción registrada) no captura de forma neutral el fenómeno objetivo (capacidad real del yacimiento).

2. **Convergencia a la media como sesgo diferencial por grupo:** La limitación documentada de RandomForest (tiende a predecir cercano a la media del training set cuando los inputs están fuera de distribución) no afecta de forma uniforme a todos los pozos. En pozos de alta producción el modelo sistemáticamente subestima; en pozos de baja producción sobreestima. Esto es el equivalente en regresión al **disparate impact** (impacto diferencial del modelo sobre distintos grupos: cuando el error sistemático no es aleatorio sino que varía de forma consistente según una característica del grupo, el modelo trata desigualmente a grupos que deberían recibir predicciones igualmente precisas), donde el grupo está definido por nivel de producción en lugar de un atributo demográfico.

3. **Métricas de evaluación no desagregadas:** `evaluate_model` calcula R² y RMSE globales sobre el test set completo. Las métricas agregadas pueden ocultar errores sistemáticos por subgrupo: un R² de 0.85 global es compatible con R² de 0.95 para pozos convencionales y 0.60 para pozos no convencionales. Calcular R² y RMSE por `tipoextraccion` es el análogo en regresión a las métricas de equidad grupales (equal error rates across groups): permite detectar si el modelo comete errores diferenciados según el tipo de extracción, y es especialmente relevante porque `tipoextraccion` es una de las features del modelo.

4. **No fairness through unawareness:** Remover `tipoextraccion` de los features no eliminaría el sesgo diferencial por tipo de extracción. `avg_prod_gas_10m` y `last_prod_gas` son proxies directos de ella: distintos tipos de extracción tienen perfiles de producción histórica muy distintos, por lo que esas variables de ventana ya codifican implícitamente el tipo de extracción. Un modelo entrenado sin `tipoextraccion` aprendería igualmente el patrón a través de los proxies.

**Por qué las técnicas estándar de debiasing no aplican:** Las técnicas de mitigación de sesgo (reweighing, adversarial debiasing, threshold optimizer) están diseñadas para casos donde el atributo sensible *no debería* correlacionar con el target — por ejemplo, cuando la correlación es un artefacto de discriminación histórica en crédito, salud o justicia penal. En el TP, `tipoextraccion` *sí debería* correlacionar con la producción: distintos tipos de extracción producen volúmenes genuinamente distintos de gas/petróleo por razones físicas (presión de yacimiento, permeabilidad, método de recuperación). Aplicar reweighing para descorrelacionar `tipoextraccion` de las predicciones eliminaría una señal causal real y degradaría el modelo. La mitigación correcta en este contexto es la **transparencia evaluativa**: calcular métricas desagregadas por grupo y loguear feature importance, no corregir las predicciones.

### Reproducibilidad

La reproducibilidad de un modelo en producción requiere tres condiciones simultáneas: mismo código, mismos datos y mismo entorno. El proyecto cumple dos de tres:

| Pilar | Estado | Detalle |
|---|---|---|
| **Código** | ✅ | Pipeline definido como código Python en Git, versionado en Airflow como DAG |
| **Datos** | ❌ | CSV descargado de URL pública sin hashear ni versionar — si el gobierno actualiza el dataset retroactivamente, no hay forma de saber qué datos generaron un modelo específico en producción |
| **Entorno** | ✅ | Docker Compose con dependencias pineadas; `uvicorn==0.40.0` pineado explícitamente para evitar conflicto conocido con Feast |

El gap de datos implica que **la linaje de datos es parcial**: MLFlow registra los parámetros y métricas de cada run, pero no hay un hash o snapshot del CSV asociado a cada versión del modelo. Para cerrar esta brecha se podría hashear el CSV al inicio del DAG y loguear ese hash como parámetro en MLFlow, de modo que cada versión del modelo quede vinculada a una versión concreta del dataset.

Lo que sí está bien cubierto en términos de tracking y empaquetado:
- **Experiment tracking**: cada run de Airflow registra en MLFlow los hiperparámetros (`n_estimators`, `max_depth`, `test_size`), las métricas (`r2`, `mse`, `rmse`) y el modelo como artefacto
- **Rollback**: el alias `production` en MLFlow Model Registry puede reasignarse a cualquier versión anterior con un solo comando, sin necesidad de reentrenar
- **Pipeline as Code**: el DAG en Airflow define el orden de ejecución, las dependencias entre tareas y el schedule en código versionado, no en configuración manual de un servidor

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
oil_and_gas_mlops_pipeline/
├── dags/
│   └── dag_oil_and_gas.py ← DAG principal
├── feature_store/
│   ├── data/ ← parquet con features históricos (generado, no se sube)
│   ├── registry/ ← metadata de Feast (generado, no se sube)
│   ├── online_store/ ← features recientes en SQLite (generado, no se sube)
│   ├── feature_store.yaml ← configuración de Feast
│   └── features.py ← definición de entidades y feature views
├── api/
│   └── main.py ← API REST con FastAPI
├── screenshots/ ← capturas de pantalla del sistema funcionando
├── mlruns/ ← artefactos de MLFlow (generado, no se sube)
├── logs/ ← logs de Airflow (generado, no se sube)
├── plugins/
├── config/
│   └── airflow.cfg ← configuración de Airflow (generado por airflow-init)
├── roadmap.md ← funcionalidades pendientes para la entrega final
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

El feature store resuelve dos necesidades incompatibles con backends distintos: el offline store optimiza para alto ancho de banda y gran volumen (leer miles de filas por pozo durante entrenamiento), mientras que el online store optimiza para baja latencia en lecturas key-value (recuperar la última fila de un pozo en milisegundos durante inferencia). Un solo backend no puede optimizar ambas a la vez.

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

### 9. Nota sobre MLFlow y seguridad de red

MLFlow 3.5+ incluye un middleware de seguridad que por defecto solo acepta conexiones desde localhost. Para permitir conexiones entre contenedores Docker es necesario deshabilitar este middleware con la variable de entorno `MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE=true` y arrancar el servidor con `--host 0.0.0.0`. Esto está configurado en el `docker-compose.yaml`.

---

## Cómo reproducir el entrenamiento

Desde la UI de Airflow en http://localhost:8080, triggerear el DAG `ml_pipeline_oil_and_gas` con los parámetros:

- `date_from`: fecha de inicio del rango de entrenamiento (formato `YYYY-MM-DD`)
- `date_to`: fecha de fin del rango de entrenamiento (formato `YYYY-MM-DD`)

El DAG puede correr con el dataset completo (sin especificar fechas), pero **se recomienda acotar el rango según la memoria disponible**. `get_historical_features` de Feast carga el parquet completo en memoria para el point-in-time join — si el worker no tiene suficiente RAM, el DAG falla con OOM (Out of Memory). Un año de datos es un punto de partida razonable para entornos con recursos limitados. El rango exacto depende de la memoria disponible en el entorno donde corra el pipeline.

Ver [Decisiones de Diseño](#decisiones-de-diseño) para la justificación del rango recomendado.

---

## Decisiones de Diseño

### 1. Rango de fechas de entrenamiento: local vs. producción

| Contexto | Rango | Motivo |
|---|---|---|
| **Entorno con recursos limitados** (ej: laptop con 9 contenedores corriendo) | `2023-01-01` / `2023-12-31` | `get_historical_features` carga el parquet completo en memoria. Con ~1.5-2GB libres disponibles, un año de datos es lo que entra sin OOM. El rango exacto varía según la RAM del entorno |
| **Producción / entorno con más recursos** | `2021-01-01` / hasta la fecha más reciente disponible | Con más memoria o un backend distribuido (BigQuery, Spark), Feast puede manejar el dataset completo sin OOM |

**Por qué se recomienda 2021 como inicio y no antes:** 2020 fue atípico por COVID-19 (caída de producción documentada en 14 países). A partir de 2021 Vaca Muerta retomó crecimiento sostenido. Además, las técnicas de completación cambiaron radicalmente entre 2012 y 2021 (de 1.500 a 2.500 lb de proppant por pie; costos de USD 20M a USD 11M por pozo), por lo que datos de pozos anteriores representan una realidad operativa distinta que puede introducir ruido. Dicho esto, el pipeline puede entrenarse con datos anteriores a 2021 o con el dataset completo sin ningún cambio en el código — es una recomendación de calidad de datos, no una restricción técnica.

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

`transition_model_version_stage` está deprecado en versiones recientes de MLFlow. El alias `"production"` es el mecanismo recomendado y permite cargar el modelo con `mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production")`. Si el DAG vuelve a correr y encuentra un modelo mejor, el alias se mueve automáticamente — la API siempre sirve el mejor modelo sin cambiar el código. `select_best_model` compara solo las versiones generadas en la corrida actual — ver decisión #8.

### 8. `select_best_model` compara solo versiones del run actual

`select_best_model` filtra por los `run_id` generados en el DAG actual, en lugar de comparar todas las versiones históricas acumuladas en MLFlow.

**Trade-off considerado:** comparar contra el historial completo podría conservar en producción un modelo con R² más alto entrenado en una corrida anterior. Se descartó por dos razones:

1. **Las métricas no son comparables entre runs**: un R² calculado sobre el test set del run A (entrenado con dataset completo) y un R² del run B (entrenado con 2023 únicamente) se evalúan sobre distribuciones distintas. Comparar esos valores directamente no tiene significado estadístico.

2. **Rompe la reproducibilidad del pipeline**: si el modelo en `@production` depende del historial acumulado en MLFlow, dos instancias del mismo sistema (o dos colaboradores) pueden terminar con modelos distintos en producción aunque hayan corrido el mismo código con los mismos datos.

**Separación de responsabilidades:** `select_best_model` elige el mejor modelo *de esta corrida*. Si ese modelo degrada respecto al ciclo anterior, el model decay report (pendiente) lo detecta y alerta. Son dos preguntas distintas que no deben mezclarse en la misma función.

### 7. Predicción autoregresiva descartada en la API

Para fechas futuras la API repite la misma predicción en lugar de actualizar `avg_prod_10m` con valores predichos. Alimentar el modelo con sus propias predicciones cambiaría la distribución del feature respecto al entrenamiento (distribution shift). En pozos shale con decline pronunciado, el error se autocorrelacionaría. Repetir la predicción del estado más reciente es más honesto respecto a las limitaciones de un modelo single-step.


---

## Feature Store

### ¿Por qué un Feature Store?

Cuando el modelo necesita predecir la producción de un pozo, requiere features como el promedio de producción de los últimos 10 meses. Calcularlo en el momento de cada inferencia sería lento y costoso. El feature store resuelve esto separando el problema en dos partes:

- **Offline Store**: almacena los features históricos de todos los pozos en todos los períodos. Se usa para entrenamiento. Internamente es un archivo **parquet** (`well_features.parquet`): un formato de tabla binario (similar a un CSV pero comprimido y de lectura mucho más rápida, especialmente al seleccionar columnas). `prepare_offline_store` toma el CSV de pozos, calcula todos los features y guarda el resultado en ese parquet. Feast lo lee para servir features al entrenamiento.
- **Online Store**: almacena solo el estado más reciente de cada pozo. Se usa para inferencia. Es rápido porque los features ya están precomputados. Internamente es una base de datos **SQLite** (`online.db`).


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
- `tipoextraccion`: tipo de extracción, variable categórica encodeada con LabelEncoder (feature)

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

Descarga el CSV desde la URL del gobierno y lo guarda en `/opt/airflow/data/pozos.csv` dentro del contenedor. Esta ruta no tiene volume mount, por lo que el CSV es efímero: se recrea en cada ejecución del DAG y no persiste en disco en el host.

**Decisión de diseño:** Se descarga el CSV completo cada vez que corre el DAG para asegurar que los datos estén actualizados. El filtro por fechas se aplica después en `prepare_offline_store`.


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

El loop recorre **10 experimentos en total** (5 por target), todos con `RandomForestRegressor`. Cada experimento varía `n_estimators`, `max_depth` y el conjunto de features. Por cada experimento, `train_model` entrena el modelo y `evaluate_model` lo evalúa y loguea en MLFlow.

| # | Target | `n_estimators` | `max_depth` | Features |
|---|---|---|---|---|
| 1 | `prod_pet` | 50 | sin límite | todas (7) |
| 2 | `prod_pet` | 100 | sin límite | todas (7) |
| 3 | `prod_pet` | 100 | 5 | todas (7) |
| 4 | `prod_pet` | 100 | 10 | todas (7) |
| 5 | `prod_pet` | 100 | sin límite | reducidas (3: `tipoextraccion`, `tef`, `profundidad`) |
| 6 | `prod_gas` | 50 | sin límite | todas (7) |
| 7 | `prod_gas` | 100 | sin límite | todas (7) |
| 8 | `prod_gas` | 100 | 5 | todas (7) |
| 9 | `prod_gas` | 100 | 10 | todas (7) |
| 10 | `prod_gas` | 100 | sin límite | reducidas (3: `tipoextraccion`, `tef`, `profundidad`) |

El experimento con features reducidas sirve como baseline de ablación: verifica si el modelo colapsa sin los features de ventana temporal.

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

Devuelve el listado de pozos disponibles para una fecha dada (`date_query`). La fecha debe ser el primer día del mes (ej: `2023-01-01`) y debe estar dentro del período de entrenamiento — el endpoint consulta el parquet del offline store.

