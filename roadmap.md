# Roadmap — Entrega Final (28/5)

## Requerimientos obligatorios pendientes

Estos dos puntos son DEBE en la especificación del trabajo integrador y aún no están implementados.

---

### 1. Model decay / data drift report

**Requisito:** El sistema DEBE dar un reporte de model decay, data drift / concept drift con al menos dos métricas que permitan observar cuándo la performance del modelo se aleja de la esperada.

**Estado:** ❌ No implementado.

**Propuesta de implementación:**

Agregar una tarea `monitor_model` al final del DAG (después de `select_best_model`) que compare el modelo recién entrenado contra la versión anterior en producción y genere un reporte con al menos estas dos métricas:

| Métrica | Qué mide | Cómo calcularlo |
|---|---|---|
| **R² delta** | Model decay: si el nuevo modelo performa peor que el anterior | Comparar `r2` del run actual contra el `r2` del modelo con alias `production` en MLFlow |
| **Feature distribution shift** | Data drift: si la distribución del input cambió respecto al ciclo anterior | Comparar media y desvío de cada feature numérico del parquet actual contra el snapshot del ciclo anterior; loguear como métricas en MLFlow |

El reporte puede ser un artefacto JSON o HTML logueado en MLFlow al final del run. Si el delta de R² supera un umbral configurable (ej. `R²_new < R²_prev - 0.05`), el DAG puede emitir una advertencia via log o Airflow alert.

---

### 2. Arquitectura escalable para inferencia (Ray)

**Requisito:** El sistema DEBE implementar una arquitectura escalable para responder la inferencia de la API (ej: Ray).

**Estado:** ❌ No implementado. La API actual corre con un único proceso uvicorn dentro del contenedor Airflow.

**Propuesta de implementación:**

Reemplazar el servidor uvicorn simple por Ray Serve:

1. Agregar `ray[serve]` a `_PIP_ADDITIONAL_REQUIREMENTS` en `.env`
2. Envolver el modelo en una clase de deployment de Ray Serve
3. El deployment carga el modelo desde MLFlow (alias `production`) al iniciar
4. Los endpoints `/api/v1/forecast` y `/api/v1/wells` se convierten en handlers de Ray Serve
5. Ray maneja el pool de réplicas y el balanceo de carga internamente

Esto permite escalar horizontalmente agregando réplicas sin cambiar el código del modelo.

---

## Mejoras técnicas identificadas desde la bibliografía de la clase

Estos ítems no son requisitos obligatorios pero cierran deudas documentadas en el README y elevarían el proyecto a Nivel 2 de MLOps.

---

### 3. Guardar LabelEncoder como artefacto en MLFlow

**Origen:** Clase 2 (empaquetado de modelo) + Clase 3 (transformaciones model-dependent)

**Problema:** El `LabelEncoder` de `tipoextraccion` se re-fitea en cada corrida sin persistirse. Si el CSV upstream incorpora nuevas categorías, el mapeo cambia entre training e inference (training-serving skew latente).

**Implementación:** En la tarea `train_model`, serializar el encoder con `pickle` o `joblib` y loguearlo como artefacto en MLFlow junto al modelo. En la API, cargar el encoder del mismo run que el modelo en producción.

---

### 4. Hashear el CSV y loguearlo como parámetro en MLFlow

**Origen:** Clase 2 (linaje de datos / reproducibilidad)

**Problema:** El pilar Datos de la triada de reproducibilidad está roto: no hay forma de saber qué versión del CSV generó un modelo específico en producción.

**Implementación:** En la tarea `download_dataset`, calcular el MD5 del archivo descargado y loguearlo como parámetro en el run de MLFlow (`mlflow.log_param("dataset_hash", md5)`). Cada versión del modelo quedaría vinculada a un hash concreto del dataset.

---

### 5. Validación de schema del CSV al ingestar

**Origen:** Clase 1 (data quality checklist de Sculley) + Clase 3 (data contracts)

**Problema:** No hay validación de schema del CSV descargado. Si el gobierno cambia columnas o tipos, el pipeline falla en una tarea posterior con un error críptico.

**Implementación:** En la tarea `download_dataset` (o en una nueva tarea `validate_data`), validar el schema del CSV con `pandera` o un chequeo manual: columnas requeridas presentes, tipos correctos, valores no negativos en producción. Si falla, el DAG falla en esta tarea con un mensaje de error claro.

---

### 6. CI/CD básico (para alcanzar Nivel 2 MLOps)

**Origen:** Clase 1 (niveles de madurez MLOps de Google)

**Problema:** El proyecto está en Nivel 1. Para Nivel 2 se necesitan tests automáticos por commit.

**Implementación mínima:** Agregar un workflow de GitHub Actions que ejecute por cada PR:
- Tests unitarios del código del DAG y la API
- Lint con `ruff` o `flake8`
- Validación del schema de `features.py` (que los campos definidos existan en el parquet esperado)

---

### 7. Corrección Point-in-Time en el dataset de entrenamiento

**Origen:** Clase 3 (feature stores — data leakage)

**Problema:** El pipeline construye el dataset de entrenamiento con un join convencional. Si el CSV upstream actualiza retroactivamente filas históricas, el modelo podría haberse entrenado con datos del futuro (data leakage implícito).

**Implementación:** En Feast, usar `get_historical_features` con el parámetro `entity_df` que incluye un timestamp por entidad. Feast aplicaría automáticamente el point-in-time correct join. Requiere agregar una columna `event_timestamp` al feature group y pasarla correctamente en la tarea `split_data`.

---

### 8. Evaluación desagregada por grupo (`tipoextraccion`)

**Origen:** Ética de la IA — métricas grupales / equal error rates across groups

**Problema:** `evaluate_model` calcula R² y RMSE sobre el test set completo. Las métricas agregadas pueden ocultar errores sistemáticos por subgrupo: un R² global bueno es compatible con R² bajo en pozos no convencionales o en pozos de alta producción. Esto impide detectar si el modelo tiene disparate impact según el tipo de extracción.

**Implementación:** En la tarea `evaluate_model`, después de calcular las métricas globales, calcular R² y RMSE por cada valor único de `tipoextraccion` en el test set y loguearlos como métricas en MLFlow (`mlflow.log_metric(f"r2_{tipo}", valor)`). Con eso, cada run en MLFlow mostraría si la performance se degrada de forma diferencial entre grupos.

---

### 9. Loguear feature importance en MLFlow

**Origen:** Ética de la IA — transparencia y explicabilidad (XAI)

**Problema:** RandomForest calcula internamente `feature_importances_` en cada entrenamiento, pero ese dato no se persiste en ningún lado. Sin él, no hay forma de saber qué features empujaron la predicción en un run dado, ni de detectar si la importancia relativa de los features cambia entre versiones del modelo (lo que podría indicar data drift o un cambio en la dinámica del yacimiento).

**Implementación:** En la tarea `evaluate_model`, después de calcular las métricas, iterar sobre `zip(feature_cols, model.feature_importances_)` y loguear cada valor como métrica en MLFlow (`mlflow.log_metric(f"importance_{feature}", valor)`). Una línea adicional por target. Con esto, cada run en MLFlow expone qué features usa el modelo de forma explicable y auditable.

---

### 10. Loguear predicciones de la API (prediction logging)

**Origen:** Monitoreo en producción — detección de degradación real vs. degradación en entrenamiento.

**Problema:** El ítem 1 (model decay report) compara métricas de entrenamiento entre runs mensuales. Pero si el modelo se degrada *en producción* entre reentrenamientos — porque los pozos evolucionan o la API recibe pozos con distribución distinta a la de entrenamiento — no hay forma de detectarlo. El R² de entrenamiento puede ser estable mientras el modelo falla silenciosamente en producción.

**Implementación:** En la API (`main.py`), después de cada predicción, loguear en un archivo CSV o tabla SQLite: `id_well`, `date`, `target`, `pred`, `features_used`, `timestamp`. Con ese log, se puede calcular prediction bias real (distribución de predicciones a lo largo del tiempo) y compararlo contra el rango esperado de entrenamiento. En una iteración más avanzada, el DAG podría leer ese log y calcularlo como parte del reporte de model decay del ítem 1.

---

### 11. Threshold de alerta configurable vía variable de entorno

**Origen:** Operabilidad — configurabilidad sin cambios de código.

**Problema:** El ítem 1 propone emitir advertencia si `R²_new < R²_prev - 0.05`. Ese umbral hardcodeado en el código es deuda de configurabilidad: si se quiere ajustar (por ejemplo, ser más estrictos con `0.02` o más tolerantes con `0.10`), hay que modificar el DAG y redeployar.

**Implementación:** Agregar `MODEL_DECAY_THRESHOLD=0.05` como variable en `.env`. El DAG la lee con `os.getenv("MODEL_DECAY_THRESHOLD", "0.05")` al inicio de la tarea `monitor_model`. Así el umbral es configurable por entorno sin tocar código.

---

### 12. Documentar el contrato de datos de la API (OpenAPI descriptions)

**Origen:** Operabilidad — usabilidad del Swagger UI en `/docs`.

**Problema:** Los endpoints tienen docstrings en Python, pero los parámetros individuales no tienen descriptions en el schema OpenAPI. En el Swagger UI (`http://localhost:8000/docs`), los campos aparecen sin explicación de formato, valores válidos ni ejemplos, lo que dificulta el uso por parte de otros equipos o evaluadores.

**Implementación:** En `main.py`, reemplazar los parámetros simples por `Query(...)` con `description` y `example`:

```python
from fastapi import Query

def get_forecast(
    id_well: str = Query(..., description="Identificador del pozo", example="12345"),
    date_start: str = Query(..., description="Fecha de inicio en formato YYYY-MM-DD", example="2023-01-01"),
    date_end: str = Query(..., description="Fecha de fin en formato YYYY-MM-DD", example="2023-12-01"),
    target: str = Query("gas", description="Fluido a predecir: 'gas' o 'pet'", example="gas"),
):
```

Una línea por parámetro. El Swagger UI refleja los cambios automáticamente.
