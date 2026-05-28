# Roadmap — Entrega Final (28/5)

## Estado actual

| Estado | Feature | Fecha | Notas |
|---|---|---|---|
| ✅ Mergeado | #5 + metadata MLflow Model Registry | 2026-04-14 | Tags y descripción legible en Registry |
| ✅ Mergeado | #19 `select_best_model` solo run actual | 2026-04-14 | Comparación determinística por DAG run |
| ✅ Mergeado | #21 Decisiones de diseño en README | 2026-04-14 | 9 decisiones explícitas |
| ✅ Mergeado | #22 Cleanup README + ajustes DAG | 2026-04-16 | Asimetría training-inference documentada |
| ✅ Mergeado | #24 Cierre de #12 (CSV hash) | 2026-04-30 | Supuesto de inmutabilidad aceptado |
| ✅ Mergeado | #29 Ray Serve (cubre #6 obligatorio) | 2026-04-30 | `num_replicas=2` unificado |
| ✅ Mergeado | #25 Filtro automático COVID | 2026-04-30 | Default `exclude_years=[2020]` |
| ✅ Mergeado | #31 Evidently AI (consolida #7+#8+#30 obligatorios) | 2026-05-01 | Mergeado en PR #35. Cubre obligatorio del RFC con R² delta + drift_share via PSI |
| ✅ Mergeado | Incremental learning XGBoost (#36) | 2026-05-27 | PR #37 mergeado. Migración RandomForest → XGBoost con training incremental en chunks mensuales. R² del modelo en producción: gas 0.869 / pet 0.895 (validado end-to-end). Resuelve OOM estructural y habilita entrenar con histórico completo. |
| ✅ Mergeado | #14 Validación de schema CSV | 2026-05-27 | Task `validate_dataset` entre `download_dataset` y `prepare_offline_store`. Valida columnas requeridas, campos críticos no vacíos, rango de años y mínimo de filas. Falla con mensaje claro antes de que datos corruptos lleguen al feature store. Decisión #16 en README. |
**Pendientes consolidados en el [Backlog priorizado](#backlog-priorizado) más abajo** — incluye los issues del roadmap original (#9, #10, #11, #13, #14, #15, #16, #17, #18) y los detectados durante reviews de las clases 6, 7 y 8 (#26, #27, #28, #38, #39, #40-44, #46-48).

**Notas sobre la evolución del scope:**

- **#6 (Ray Serve):** ya implementado en [PR #29](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/pull/29). Era obligatorio del RFC (arquitectura escalable para inferencia).
- **#7 + #8 + #30 consolidados en #31 (Evidently AI):** inicialmente se planteaban tres issues separados — reporte propio de model decay (#7), threshold configurable (#8) y drift detection con `alibi-detect` (#30). Al evaluar la implementación apareció Evidently AI, que cubre las tres cosas con una única librería (regression performance + data drift + tests asertivos con umbrales). Se cerró #30 y se consolidó todo el scope en #31, lo cual simplifica la arquitectura y reduce el mantenimiento.
- **#23 (filtro COVID 2020):** cubierto en [PR #25](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/pull/25) con un mecanismo más general (`exclude_years` en lugar de hardcodear 2020).

---

## Backlog priorizado

Issues abiertos ordenados por importancia. Los dos obligatorios del RFC ya están cerrados (#29 Ray Serve + #31 Evidently), por lo que esta priorización refleja **valor incremental** sobre el sistema, no urgencia para la entrega.

Criterio de los niveles:

- **P1 — Operativos críticos**: previenen fallos silenciosos y endurecen el sistema. Bajo esfuerzo, alto impacto preventivo.
- **P2 — Observabilidad y monitoreo**: dan visibilidad sobre qué pasa en producción y completan el monitoreo iniciado en #31.
- **P3 — Calidad del pipeline**: cierran gaps de best practices identificados en el README y reviews.
- **P4 — Robustez estructural**: cambios más invasivos pero importantes para escala (memoria, CI/CD, point-in-time).
- **P5 — Optimizaciones data-driven**: requieren datos previos (latencia medida, asimetría observada) para justificarse — abrir solo cuando haya evidencia.

| Prioridad | Issue | Notas |
|---|---|---|
| **P1** | [#46](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/46) Alerta de modelo stale | Detecta DAG fallando silenciosamente. Endpoint `/health/staleness` o tarea Airflow independiente. Origen: Clase 7, Caso 1 YarnIt. |
| **P1** | [#47](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/47) Validación de outputs (NaN / rangos físicos) | Defensivo. Evita devolver predicciones absurdas (negativas, NaN) al consumidor. Origen: Clase 7, slide 62. |
| ✅ **Cerrado** | [#14](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/14) Validación de schema CSV | Implementado en task `validate_dataset` (2026-05-27). Validación manual con pandas: columnas requeridas, campos críticos no vacíos, rango de años, mínimo de filas. Decisión #16 en README. |
| **P1** | [#42](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/42) Health check + graceful degradation | Endpoints `/health/live` y `/health/ready` + fallback al modelo cacheado en disco. Origen: Clase 6, slides 36 y 52. |
| **P1** | [#41](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/41) Autenticación API key | Hoy `/api/v1/*` están abiertos. Mínimo: `X-API-Key` validado contra `.env`. Origen: Clase 6, slide 52. |
| **P2** | [#40](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/40) Métricas de latencia P50/P90/P99 | Prerrequisito para definir SLA y para validar #27/#28. Origen: Clase 6, slides 7, 27, 52. |
| **P2** | [#44](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/44) Logs operacionales estructurados (JSON) | Distinto de #15: logs de sistema, no de predicciones. Base para debugging serio. Origen: Clase 6, slide 51. |
| **P2** | [#15](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/15) Prediction logging en API | CSV/SQLite con `(id_well, date, target, pred, features, timestamp)`. Habilita análisis de drift en producción. Origen: roadmap original. |
| **P2** | [#48](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/48) Bias y calibration en `monitor_model` | Completa la tríada de canary metrics. Hoy hay R² delta + drift_share, faltan bias y calibración por bucket. Origen: Clase 7, slide 11. |
| **P2** | [#43](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/43) Prometheus + Grafana | Stack estándar para observabilidad operativa. Depende de #40 (necesita endpoint `/metrics`). Origen: Clase 6, slide 51. |
| **P3** | [#9](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/9) Eval desagregada por `tipoextraccion` | Quick win en `evaluate_model`. Detecta disparate impact por subgrupo. Origen: roadmap original (ética IA). |
| **P3** | [#10](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/10) Feature importance en MLFlow | Quick win, mismo PR que #9. Una línea por feature por target. Origen: roadmap original (XAI). |
| **P3** | [#13](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/13) LabelEncoder como artefacto | Cierra training-serving skew latente si aparecen nuevas categorías de `tipoextraccion`. Origen: roadmap original. |
| **P3** | [#11](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/11) OpenAPI descriptions en endpoints | Mejora usabilidad del Swagger UI con descriptions y ejemplos por param. Origen: roadmap original. |
| **P4** | [#38](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/38) Refactor `split_data` y `prepare_offline_store` por chunks | El siguiente bottleneck post-XGBoost — Feast cargando todo en RAM. Habilita entrenar con 3+ años. Origen: bitácora #36. |
| **P4** | [#16](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/16) CI/CD básico con GitHub Actions | Tests + lint + validación de schema en cada PR. Lleva al Nivel 2 de MLOps. Origen: roadmap original. |
| **P4** | [#17](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/17) Point-in-Time correct con `entity_df` en Feast | Robustez del entrenamiento ante mutaciones retroactivas del dataset upstream. Origen: roadmap original. |
| **P4** | [#18](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/18) Segundo dataset del RFC | Metadata estructural por pozo (formación, cuenca, empresa). Complementa el modelo. Origen: roadmap original. |
| **P5** | [#27](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/27) Cache de inferencia con Redis | Optimización condicional. Justificada solo si #40 muestra que la CPU del API es bottleneck. Origen: Clase 6. |
| **P5** | [#28](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/28) Autoscaling dinámico de Ray Serve | Optimización condicional. Justificada solo si #40 muestra utilización desigual. Origen: Clase 6. |
| **P5** | [#26](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/26) Separación de deployments gas/pet | Justificada solo si #15 muestra asimetría sostenida de carga entre fluidos. Origen: Clase 6 / Decisión #10 README. |
| **P5** | [#39](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/39) Adaptive Isolation Forest | Exploratorio para `monitor_model`. No aplica directamente al caso de uso (no es clasificación de fraude). Origen: Clase 7. |

---

## Requerimientos obligatorios pendientes

Tras releer el RFC, los dos obligatorios (DEBE) están cerrados:

- ✅ **Arquitectura escalable de inferencia (Ray):** cubierto por #29 (Ray Serve) — mergeado.
- ✅ **Reporte de model decay / data drift con al menos dos métricas:** cubierto por #31 (Evidently AI) — mergeado en PR #35 (2026-05-01).

---

### 1. Model decay / data drift report

**Requisito:** El sistema DEBE dar un reporte de model decay, data drift / concept drift con al menos dos métricas que permitan observar cuándo la performance del modelo se aleja de la esperada.

**Estado:** ✅ Implementado en #31 (mergeado en PR #35, 2026-05-01). Ver bitácora más abajo para el detalle de decisiones e iteraciones. La sección a continuación es la propuesta original de diseño que guió la implementación.

**Propuesta de implementación original:**

Agregar una tarea `monitor_model` al final del DAG (después de `select_best_model`) que compare el modelo recién entrenado contra la versión anterior en producción y genere un reporte con estas métricas:

| Métrica | Qué mide | Cómo calcularlo |
|---|---|---|
| **R² delta** | Model decay: si el nuevo modelo performa peor que el anterior | Comparar `r2` del run actual contra el `r2` del modelo con alias `production` en MLFlow |
| **Feature distribution shift** | Data drift: si la distribución del input cambió respecto al ciclo anterior | Comparar media y desvío de cada feature numérico del parquet actual contra el snapshot del ciclo anterior; loguear como métricas en MLFlow |
| **PSI sobre features de ventana temporal** | Data drift en los features más sensibles al régimen de producción | Calcular Population Stability Index sobre `avg_prod_gas_10m` y `avg_prod_pet_10m` entre el ciclo actual y el anterior. PSI > 0.1 es alerta amarilla; PSI > 0.25 es alerta roja |
| **Prediction bias** | Concept drift: si el modelo empieza a sobreestimar o subestimar sistemáticamente | `mean(pred - actual)` sobre el test set. Un bias sostenido en un sentido es señal de drift antes de que R² caiga |
| **R² ventana reciente vs. R² global** | Concept drift temporal: si el modelo pierde precisión en el régimen actual | Calcular R² solo sobre los últimos 3 meses del test set y compararlo contra el R² global del mismo run. Si divergen, el modelo funciona mejor en datos históricos que en los más recientes |

El reporte puede ser un artefacto JSON o HTML logueado en MLFlow al final del run. Si el delta de R² supera un umbral configurable (ver ítem 11), el DAG emite una advertencia via log o Airflow alert.

**Criterio de selección del modelo a producción:**

El `select_best_model` actual promueve la versión con mayor R² global. Con el reporte de monitoreo, el criterio puede volverse más robusto usando filtrado secuencial:

1. **Filtro de umbral mínimo:** descartar versiones con `R² < 0.85` (modelo inutilizable)
2. **Filtro de bias:** descartar versiones con `|prediction_bias| > umbral_configurable` (modelo sistemáticamente sesgado)
3. **Selección por score compuesto** entre los modelos que pasan los filtros:

```
score = 0.5 × R² + 0.3 × (1 - RMSE_normalizado) + 0.2 × (1 - |bias_normalizado|)
```

La ponderación refleja prioridades del problema: R² es el indicador principal de capacidad predictiva (0.5), RMSE en unidades originales (m³) es lo que el operador interpreta directamente (0.3), y el bias penaliza subestimación/sobreestimación sistemática que tiene impacto económico real (0.2). Los pesos son configurables vía variables de entorno junto al umbral del ítem 11.

---

### 2. Arquitectura escalable para inferencia (Ray)

**Requisito:** El sistema DEBE implementar una arquitectura escalable para responder la inferencia de la API (ej: Ray).

**Estado:** ✅ Implementado en #29 (mergeado 2026-04-30) con Ray Serve + FastAPI, `num_replicas=2` unificado. Ver Decisión #10 del README para el detalle arquitectónico. La sección a continuación es la propuesta original que guió la implementación.

**Propuesta de implementación original:**

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

**Estado en esta entrega:** No se implementa. El issue [#12](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/12) quedó cerrado apoyándose en el supuesto de inmutabilidad del dataset histórico del gobierno (documentado en el README, aceptado por el docente).

**Pendiente para producción a escala:** Si el dataset upstream pasara a mutar entre snapshots, o si se quisiera reproducir versiones antiguas con garantía fuerte de linaje de datos, habría que calcular el MD5 del CSV descargado en `download_dataset` y loguearlo como parámetro en el run de MLFlow (`mlflow.log_param("dataset_hash", md5)`). Así cada versión del modelo quedaría vinculada a un hash concreto del dataset, cerrando el pilar Datos de la triada de reproducibilidad.

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

---

### 13. Integrar el segundo dataset del RFC (listado de pozos)

**Origen:** RFC — "Datasets a Utilizar" especifica dos fuentes; solo se usa una actualmente.

**Problema:** El dataset de metadata de pozos (empresa operadora, formación geológica, cuenca, coordenadas) no está integrado. Esto tiene dos consecuencias: (1) incumplimiento parcial del RFC, y (2) el modelo no puede diferenciar pozos por sus características estructurales, lo que contribuye a la convergencia a la media documentada en inferencia futura.

**Implementación:** En `download_dataset`, descargar también el segundo CSV (`energia_cbfa4d79-ffb3-4096-bab5-eb0dde9a8385`). En `prepare_offline_store`, hacer un join por `idpozo` para agregar columnas de metadata estática — candidatas: `formprod` (formación productiva), `cuenca`, `empresa`. Estas columnas pasan al feature store como features estáticos del pozo y quedan disponibles tanto en el offline store (entrenamiento) como en el online store (inferencia futura).

---

## Bitácora de implementación

Registro cronológico por feature: errores encontrados, cómo se resolvieron y decisiones tomadas que no están explícitas en el código. El README documenta el **qué** (decisión final); esta bitácora documenta el **cómo** (proceso, errores, alternativas descartadas).

---

### Histórico — features ya mergeadas

#### #5 + metadata MLflow Model Registry (mergeado 2026-04-14)

Se documentó cada versión del Model Registry con tags (`run_name`, `n_estimators`, `max_depth`, `features`, `r2`) y descripción legible. **Decisión clave:** la descripción a nivel del modelo registrado se sobrescribe en cada run pero el contenido es estático — esto evita reescribir lógica de versionado pero genera una pequeña ineficiencia. Se aceptó porque la descripción del modelo es metadata del concepto, no del run.

#### #19 — `select_best_model` filtra por run_id del DAG actual (mergeado 2026-04-14)

**Problema detectado:** la versión original de `select_best_model` comparaba todas las versiones históricas del modelo en MLflow para decidir cuál promover. **Por qué fallaba:** dos runs entrenados con datasets distintos producen R² no comparables (un R² calculado sobre el test set de 2023 y otro sobre el test set de 2021 evalúan distribuciones distintas). **Decisión:** filtrar por `run_id` del DAG actual para que el resultado sea determinístico. Diferentes colaboradores corriendo el mismo DAG con los mismos datos llegan al mismo ganador.

#### #21 — Decisiones de diseño documentadas en el README (mergeado 2026-04-14)

Se agregaron 9 decisiones explícitas: split temporal en lugar de aleatorio, modelos independientes para gas y petróleo, alias `production` en lugar de stages deprecados, predicción autoregresiva descartada en la API, etc. **Por qué importa:** sin esta documentación, futuras modificaciones podrían revertir decisiones que ya se tomaron por buenas razones (ej. alguien que vea el split aleatorio "más simple" sin saber el data leakage que introduce).

#### #22 — Cleanup del README + asimetría training-inference (mergeado 2026-04-16)

**Decisión clave documentada:** el modelo se entrena con features de mes T para predecir target de mes T (T→T), pero en inferencia se usan features del último mes conocido para predecir el mes siguiente (T→T+1). Esto significa que el modelo **nunca aprendió explícitamente la relación T→T+1** — asume que el estado del mes más reciente es un proxy razonable para el siguiente. Es una limitación conocida del pipeline, registrada como pendiente para futuras iteraciones.

#### #24 — Cierre de #12 (CSV hash) (mergeado 2026-04-30)

**Decisión:** **no implementar** hashing del CSV descargado. El supuesto es que los datos históricos del Ministerio de Energía son inmutables una vez publicados (aceptado por el docente). Si el supuesto se viola en producción a escala, el plan documentado es agregar `mlflow.log_param("dataset_hash", md5)` en `download_dataset`. **Por qué se cerró sin implementar:** agregar hashing introduce complejidad sin valor mientras el supuesto se mantenga. Decisión costo/beneficio.

#### #29 — Ray Serve para inferencia escalable (mergeado 2026-04-30)

Cubre el obligatorio del RFC sobre arquitectura escalable. **Decisión clave:** deployment unificado con `num_replicas=2` (gas + pet en el mismo deployment) en lugar de deployments separados. **Razonamiento:** balancear memoria (320 MB total con 2 réplicas) contra fault tolerance (si una réplica muere, la otra atiende mientras Ray la recrea). Tres issues abiertas para optimizaciones data-driven: #26 (separación gas/pet si la carga lo justifica), #27 (cache Redis), #28 (autoscaling).

#### #25 — Exclusión automática de años atípicos (mergeado 2026-04-30)

Reemplaza el filtro manual por un param `exclude_years=[2020]` por default. **Decisión clave:** filtrar 2020 específicamente, no `date_from=2021-01-01`. **Razón:** descartar datos pre-COVID válidos sería arbitrario; lo que queremos excluir es el régimen anómalo, no un cutoff temporal. **Efecto colateral documentado:** la fila futura del online store puede desplazarse a un año anterior al excluido (un pozo con data hasta junio 2020 termina con fila futura en enero 2020 cuando se filtra 2020). Es coherente con la semántica del filtro.

---

### #31 — Reporte de model decay con Evidently AI (mergeado 2026-05-01)

**Rama:** `feature/model-decay-monitor`
**Inicio:** 2026-04-30
**Estado:** ✅ Mergeado en PR #35 (2026-05-01). Cierra obligatorio del RFC.
**Cubre obligatorio del RFC:** "El sistema DEBE dar un reporte de model decay / data drift con al menos dos métricas que permitan observar cuándo la performance del modelo se aleja de la esperada."
**Consolida issues:** #7 (model decay report), #8 (threshold configurable), motivación de #30 (alibi-detect, cerrado).

#### Decisiones tomadas durante la implementación

1. **Reference = train, Current = test** (en lugar de snapshots persistentes entre runs).
   - **Por qué:** la asimetría temporal del split del DAG (test = datos más recientes) ya provee la base para detectar drift sin requerir persistir parquets entre runs.
   - **Trade-off aceptado:** no es decay "puro" entre runs, sino una mezcla de drift train→test + calidad del modelo en datos no vistos.
   - **Mitigado con:** decay temporal explícito (siguiente decisión).

2. **Sumar decay temporal real (delta R² entre runs).**
   - **Evolución del razonamiento:** la primera versión del monitor tenía solo un threshold absoluto sobre R² (`R² < 0.85` → alerta de calidad). Al revisar el alcance, se identificó que ese threshold detecta "modelo malo" pero no "modelo que se degradó" — y el RFC pide específicamente *decay*, que es un concepto temporal: comparar performance actual contra performance esperada/anterior.
   - **Decisión:** agregar comparación `r2_actual - r2_anterior` leyendo del Model Registry, que ya persiste todas las versiones. No requiere persistencia adicional.
   - **Cómo identificar la versión "anterior":** filtrar versiones cuyo `run_id` no esté en `current_run_ids` (los del DAG actual) y tomar la más reciente.

3. **Tres thresholds independientes vía `.env`:**
   - `MODEL_QUALITY_R2_FLOOR=0.85` — piso absoluto de calidad sobre R² del modelo en test.
   - `MODEL_DECAY_DRIFT_SHARE_THRESHOLD=0.5` — % máximo aceptable de features con drift.
   - `MODEL_DECAY_R2_DELTA=0.05` — caída máxima aceptable de R² entre runs.
   - **Por qué tres y no uno:** cada uno cubre una pregunta distinta (calidad / drift de datos / decay temporal). Tener uno solo dejaría agujeros.

4. **Alertas blandas (warnings en log de Airflow, no abortan el DAG).**
   - **Por qué:** abortar dejaría el modelo viejo en producción de forma silenciosa. Peor que un nuevo modelo con alerta visible que un humano puede revisar.

5. **Evidently AI sobre alibi-detect (la librería vista en clase).**
   - **Por qué se descartó alibi-detect:** cubre solo drift, no regression performance ni reportes HTML. El RFC pide ambas dimensiones. Evidently los provee con dos presets predefinidos en una sola dependencia.
   - **Otras alternativas evaluadas:** NannyML (caso de uso distinto: estimación sin ground truth, no aplica acá), whylogs (orientado a profiling con cloud SaaS, fuera del scope local), implementación propia con sklearn + Jinja2 (mantenimiento alto).

6. **`evidently==0.6.7` pineado** (en lugar de 0.4.40 inicial o 0.7.x).
   - **Por qué se reemplazó 0.4.40:** rompió en runtime con `TypeError: got an unexpected keyword argument 'squared'` por incompatibilidad con sklearn 1.8 (ver error 3 abajo).
   - **Por qué 0.6.7 sobre 0.7.x:** la API de 0.7+ es nueva y rompe `Report` + `metric_preset`. La 0.6.x mantiene la API clásica más estable.

#### Errores encontrados y resoluciones

##### Error 1 — OOM en `download_dataset` (run 21:02:43, 21:08:35)

**Síntoma:**
```
critical: Process terminated by signal. Likely out of memory error (OOM).
signal=-9 (SIGKILL)
```

**Causa raíz:** la tarea hacía `pd.read_csv(url)` para descargar y `df.to_csv(save_path)` para guardar. El CSV completo del MINEM se cargaba en memoria como DataFrame (~200 MB con tipos inferidos) antes de escribirse. La tarea funcionaba con la huella de memoria anterior, pero al sumar Evidently a `_PIP_ADDITIONAL_REQUIREMENTS` se trajeron deps pesadas (matplotlib, plotly, dask, statsmodels, mlflow 3.11) que aumentaron la huella permanente del worker container, dejando menos margen para cargas puntuales.

**Solución:** descargar con streaming directo a disco usando `urllib.request.urlopen` + `shutil.copyfileobj`, sin pasar por pandas. Reduce el uso de memoria de la tarea de ~200 MB a casi cero.

```python
with urllib.request.urlopen(url) as response, open(save_path, 'wb') as out_file:
    shutil.copyfileobj(response, out_file)
```

##### Error 2 — OOM en `prepare_offline_store` (run 21:08:35)

**Síntoma:** mismo SIGKILL, ahora 1 segundo después de arrancar la tarea — síntoma de OOM al inicio del proceso, no durante el procesamiento.

**Causa raíz:** estructural. `docker stats` mostró que los 9 containers totalizan ~7.13 GB de los 7.65 GB asignados a Docker Desktop (93% de uso). El container `api-1` con Ray Serve a 2 réplicas consume 2.34 GB (el más grande de todos). Cuando `prepare_offline_store` arranca y necesita cargar el CSV + procesarlo en memoria, no entra.

**Solución (workaround):** parar `api-1` durante el DAG run. La API de inferencia no se necesita para entrenar. Libera 2.3 GB.

```bash
docker compose stop api
# trigger DAG
docker compose start api  # cuando termine
```

**Soluciones permanentes (no aplicadas, registradas como issues futuras):**
- Subir RAM de Docker Desktop a 10-12 GB (depende del entorno local de cada usuario).
- Bajar `num_replicas=2 → 1` en Ray Serve (cambia decisión #10 del README, requiere PR aparte).

##### Error 3 — `TypeError: got an unexpected keyword argument 'squared'` (run 21:13:42)

**Síntoma:**
```
File ".../evidently/metrics/regression_performance/regression_quality.py:119"
TypeError: got an unexpected keyword argument 'squared'
```

**Causa raíz:** incompatibilidad entre `evidently==0.4.40` y `scikit-learn==1.8.0` (la versión que arrastró mlflow 3.11). Evidently 0.4.x llamaba internamente `mean_squared_error(y_true, y_pred, squared=False)` para calcular RMSE. El parámetro `squared` fue **removido en sklearn 1.6+**; la nueva forma es `root_mean_squared_error()`.

**Solución:** actualizar a `evidently==0.6.7`, que ya no usa `squared=False` y mantiene la API clásica (`Report` + `metric_preset`). La 0.7+ se descartó porque cambió la API por completo y aún está en evolución.

**Cambio en `.env`:**
```diff
- evidently==0.4.40
+ evidently==0.6.7
```

##### Error 4 — `drift_share=0.0` en ambos targets (falso negativo del default de Evidently)

**Síntoma:** la primera corrida exitosa de `monitor_model` (run 21:39:44) reportó:
```
[MODEL_DECAY] oil_gas_prod_gas R²=0.872, drift_share=0.0
[MODEL_DECAY] oil_gas_prod_pet R²=0.910, drift_share=0.0
```

`drift_share=0.0` significa "ninguna feature con drift". Sospechoso — `n_readings` tiene drift por construcción (en train acumula menos lecturas que en test).

**Causa raíz:** Evidently usa por default **Wasserstein distance con threshold 0.5** para features numéricas. Inspeccionando el HTML del reporte, los drift_scores reales eran:

| Feature | Wasserstein score |
|---|---|
| tef | 0.085 |
| n_readings | 0.053 |
| target | 0.046 |
| prediction | 0.033 |
| prod_agua | 0.027 |
| avg_prod_gas_10m | 0.019 |
| last_prod_gas | 0.018 |
| tipoextraccion | 0.016 |
| profundidad | 0.015 |

Todos están **muy por debajo de 0.5**. Por lo tanto: 0/9 features con drift detectado → `share_of_drifted_columns = 0`. El parsing del dict estaba bien; el threshold default era el problema. Para el dominio del proyecto, scores entre 0.05 y 0.10 ya son señales relevantes que el default oculta.

**Solución (primera iteración, falló):** cambiar el stattest default por tests de hipótesis con threshold interpretable:
- `num_stattest='ks'` (Kolmogorov-Smirnov) con `num_stattest_threshold=0.05` (p-value).
- `cat_stattest='chisquare'` con `cat_stattest_threshold=0.05`.

Adicionalmente se agregó al monitor el logueo de `monitor_drift_<feature>` por cada feature individual en MLFlow, para tener visibilidad granular sin abrir el HTML del reporte.

##### Error 5 — `drift_share=1.0` en ambos targets (falso positivo de KS con muestra grande)

**Síntoma:** la corrida con KS + chi-square (run 23:50:48) reportó:
```
[MODEL_DECAY] oil_gas_prod_gas drift share (100%) > threshold (50%)
[MODEL_DECAY] oil_gas_prod_pet drift share (100%) > threshold (50%)
```

9/9 features marcadas como con drift. Saltó del extremo opuesto al original.

**Causa raíz:** **tests de hipótesis (KS, chi-square) son ultra-sensibles al tamaño de muestra.** Con miles de filas en train y test, el p-value se hace minúsculo aunque la diferencia entre distribuciones sea minúscula. Inspeccionando el HTML, los p-values reales fueron:

| Feature | Stattest | p-value |
|---|---|---|
| target | KS | 1.2e-05 |
| prediction | KS | 1e-06 |
| tef | KS | 0.0 |
| n_readings | KS | 0.0 |
| profundidad | KS | 0.0 |
| last_prod_gas | KS | 0.000325 |
| tipoextraccion | chi-square | 0.000723 |
| avg_prod_gas_10m | KS | 0.0325 |
| prod_agua | KS | 0.0397 |

Todos por debajo de 0.05 → 9/9 con drift. Las features con p-value 0.0 (tef, n_readings, profundidad) sí tienen cambios reales, pero los otros (avg_prod_gas_10m con p=0.03) están en el filo y probablemente sean diferencias mínimas detectadas como significantes solo por el N grande.

**Solución (definitiva):** cambiar a métricas que miden **magnitud del cambio**, no significancia estadística:
- `num_stattest='psi'` (Population Stability Index) con `num_stattest_threshold=0.1`.
- `cat_stattest='jensenshannon'` (Jensen-Shannon distance) con `cat_stattest_threshold=0.1`.

PSI es el threshold estándar de la industria (PSI < 0.1 sin drift, 0.1-0.25 moderado, ≥ 0.25 severo) y **no depende del tamaño de muestra**. Coincide además con la propuesta original del item 1 del roadmap del proyecto.

**Lección general:** para data drift en pipelines automáticos donde el tamaño de muestra varía de un run a otro, **preferir métricas de magnitud sobre tests de hipótesis**. Los p-values son útiles para decisiones puntuales con muestras chicas; las métricas de magnitud son más robustas en producción.

#### Cómo se cerró #31

1. ✅ Validada la corrida end-to-end después del fix de Evidently 0.6.7.
2. ✅ Verificados artefactos: HTML del reporte en MLFlow + métricas `monitor_r2`, `monitor_drift_share`, `monitor_r2_delta`.
3. ✅ PR abierto, revisado y mergeado como PR #35.
4. ✅ Setup completo restaurado tras el DAG run.

---

### 🔄 En review — #36 Incremental learning con XGBoost

**Rama:** `feature/incremental-learning-xgboost`
**Inicio:** 2026-05-02
**Estado:** validación end-to-end completa ✅, PR #37 abierto y en review.
**Resuelve:** OOM estructural de `RandomForestRegressor.fit()` al entrenar con histórico extenso.

#### Cambio conceptual

Random Forest es un *bagging ensemble*: cada árbol se entrena sobre un bootstrap sample del dataset completo. La limitación de memoria es **algorítmica, no de implementación**: necesita acceso simultáneo a todo el dataset al armar cada árbol y por eso no tiene `partial_fit`. Con eso, entrenar localmente con histórico extenso (~2019 en adelante) era inviable.

XGBoost es un *gradient boosting ensemble* construido secuencialmente: cada árbol nuevo aprende del error residual del modelo anterior. Esa propiedad permite *continuation*: cargar un modelo previo y agregarle árboles nuevos entrenando solo con un chunk de datos. La memoria queda acotada por el tamaño del chunk + el booster (chico, < 10 MB), no por el dataset total.

**Aclaración:** la mejora no viene de cambiar el modelo solo. XGBoost con `xgb.fit(dataset_completo)` reproduce el mismo OOM. Son **dos cambios juntos**: (1) algoritmo que soporte continuación, y (2) bucle que itere el dataset por chunks.

#### Decisiones tomadas durante la implementación

1. **XGBoost sobre SGDRegressor.** SGDRegressor mantiene memoria estrictamente constante via `partial_fit`, pero es lineal — pierde la capacidad no-lineal valiosa para los features de ventana (`avg_prod_*_10m`, `last_prod_*`) y estáticos (`profundidad`). El tradeoff de memoria estrictamente constante no compensa la pérdida predictiva.
2. **Chunks mensuales en orden temporal.** El split de Feast ya provee `event_timestamp`; iterar por mes es natural. El orden cronológico es deliberado: los meses recientes pesan más en el modelo final, lo cual es coherente con el caso de uso (predecir el mes siguiente).
3. **`n_estimators_per_chunk` en lugar de `n_estimators`.** En XGBoost continuation, el parámetro pasa a ser "árboles a agregar por chunk", no "total". Con dataset de ~10 meses de train, `n_estimators_per_chunk=10` produce ~100 árboles totales. Renombrar el parámetro evita confusión.
4. **`learning_rate=0.1`** (default de XGB es 0.3). Más conservador para que ningún chunk individual domine al modelo final.
5. **Formato `.ubj` en MLFlow** (no pickle). `mlflow.xgboost.log_model(model_format="ubj")` preserva metadata específica de XGBoost y es portable entre versiones.
6. **`event_timestamp` en X_train/X_test.** Para que `train_model` pueda iterar por mes. En las tasks que entrenan/predicen se filtra a `[features]` antes de pasar al modelo, así que nunca llega como feature.
7. **`LabelEncoder` se mantiene** para `tipoextraccion`. XGBoost lo trata como ordinal numérico (subóptimo pero funciona). Migrar a `enable_categorical=True` queda para [issue #13](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/13).

#### Validación end-to-end (run `manual__2026-05-07T22:22:24`)

- **DAG completó exitosamente** las 27 tareas (download, prepare, online_store, split, 10 × train+evaluate, select_best_model, monitor_model). 0 fallos.
- **Training:** ejemplo del primer experimento (prod_pet, 5 árboles/chunk): 31.940 samples procesados en **9 chunks mensuales**, 45 árboles totales en el modelo final.
- **Memoria:** training en chunks no provocó picos de RAM perceptibles, a diferencia del RF que generaba presión cerca del límite.

#### Validación cuantitativa con 3 escenarios de rango temporal

Después de la validación end-to-end inicial, se diseñó un experimento más riguroso para cuantificar el tradeoff RF vs XGBoost. La idea: correr el DAG con tres rangos temporales crecientes (A: 2 años, B: 3 años, C: 4 años efectivos) en ambos modelos y medir tiempo, RAM y performance.

**Tabla 1 — Resumen ejecutivo**

| Escenario | Modelo | Estado | Tiempo total | Tiempo train_model (suma 10 exp) | RAM peak worker |
|---|---|---|---|---|---|
| A (2022-2023) | XGBoost | ✅ success | **244s** | **45s** | **2.99 GiB** |
| A (2022-2023) | RandomForest | ✅ success | 469s | 211s | 3.53 GiB |
| B (2020-2023) | XGBoost | ❌ failed en `split_data` | 98s | (no llegó) | 3.67 GiB |
| B (2020-2023) | RandomForest | (saltado, fallaría idéntico) | — | — | — |
| C (2019-2023) | XGBoost | (saltado, fallaría idéntico) | — | — | — |
| C (2019-2023) | RandomForest | (saltado, fallaría idéntico) | — | — | — |

**Tabla 2 — Setup**

| Escenario | Modelo | Filas X_train | Árboles totales | Hiperparámetros clave |
|---|---|---|---|---|
| A | XGBoost | 63.424 | 95 (5 × 19 chunks) | est_pc=5, depth=6, lr=0.1 |
| A | RandomForest | 63.424 | 100 | est=100, depth=10 |

**Tabla 3 — Performance (escenario A, único comparable)**

| Modelo | R² gas | R² pet | RMSE gas (m³) | RMSE pet (m³) | MAE gas (m³) | MAE pet (m³) |
|---|---|---|---|---|---|---|
| XGBoost | 0.871 | 0.863 | 668,9 | 393,8 | 193,3 | 122,4 |
| RandomForest | **0.923** | **0.902** | **517,4** | **334,4** | **141,7** | **93,7** |
| Δ (RF − XGB) | +5,2 pp | +3,9 pp | −151,5 (−23%) | −59,4 (−15%) | −51,6 (−27%) | −28,7 (−24%) |

**Lectura del experimento:**

- **XGBoost gana en eficiencia operativa**: ~2× más rápido total, ~5× más rápido específicamente en `train_model`, 15% menos RAM peak. En producción mensual estos ahorros son reales.
- **RandomForest gana en precisión** en este rango chico: R² entre 4 y 5 pp mejor, RMSE 15-23% menor.
- **B y C revelan el siguiente cuello de botella**: con 3+ años, ambos modelos fallan en `split_data` por OOM. La razón no es el modelo: es Feast cargando todo en memoria al hacer `get_historical_features`, antes incluso de empezar a entrenar.

#### Tuning fallido — la config conservadora era el techo

Tras ver la pérdida de performance, se intentó tunear XGBoost con configuraciones más agresivas (max_depth=8-10, learning_rate=0.1-0.2, est_pc=10-15) bajo la hipótesis de que la config original era subóptima por árboles muy chicos y learning_rate muy bajo.

**Resultado: todos los experimentos empeoraron.**

| Configuración | R² gas | R² pet | Diagnóstico |
|---|---|---|---|
| depth=6, lr=0.1, est_pc=5 (original) | 0.871 | 0.863 | Baseline. |
| depth=10, lr=0.1, est_pc=10 | 0.661 | 0.673 | −0,21 pp |
| depth=10, lr=0.2, est_pc=10 | −0.04 | −0.24 | Catastrófico. |
| depth=8, lr=0.1, est_pc=15 | 0.458 | 0.484 | −0,41 pp |
| depth=8, lr=0.2, est_pc=10 | −0.08 | 0.217 | Catastrófico. |
| depth=10, lr=0.1, reduced features | −0.46 | −0.07 | Catastrófico. |

**Diagnóstico — overfitting estructural por incremental + árboles profundos:**

Con `max_depth=10` cada árbol es muy expresivo. Con 19 chunks × 10 árboles = 190 árboles muy expresivos acumulados. Cada chunk se ajusta al residuo de su mes específico y memoriza patrones locales. El test set es contiguous a los meses finales del train (split temporal 80/20 sobre fechas), entonces los chunks finales overfittean justo donde se va a evaluar. `learning_rate=0.2` acelera el efecto.

**Aprendizaje: la pérdida de R² ~5 pp vs RandomForest no es por subóptima configuración — es el costo intrínseco del incremental learning con árboles en este dataset.** Subir capacidad expresiva (depth) o agresividad (lr) empeora porque ya estábamos en el sweet spot.

Se vuelve a la configuración original (depth=6, lr=0.1, est_pc=5) y se acepta el tradeoff documentado.

#### Lo que NO resuelve esta feature (próximos cuellos de botella)

1. **`split_data` y `prepare_offline_store` siguen cargando todo en memoria.** Los escenarios B y C fallaron acá, antes del training. **Es el siguiente bottleneck prioritario** — issue separado a abrir: refactorear estas tareas a procesamiento por chunks o migrar de pandas in-memory a Dask/Polars. **Resolver esto desbloquea dos mejoras independientes que pueden compensar la pérdida de performance:**
   - Entrenar con histórico completo (B/C escenarios). XGBoost con más datos típicamente mejora; el escalado a 4+ años puede acercar la performance a RF e incluso superarla en datos más recientes.
   - Sumar el segundo dataset del RFC ([#18](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/18)) con metadata estructural por pozo (formación geológica, cuenca, empresa). Esos features tienen capacidad explicativa que hoy no está en el modelo y podrían cerrar la brecha vs RandomForest.
2. **Presión de RAM por containers simultáneos.** Sigue siendo necesario parar `api-1` durante el DAG run o subir RAM de Docker Desktop.

---

### Próximos features e issues a abrir

#### Issues abiertas a tomar en orden

1. **Incremental learning con memoria acotada — migrar RandomForest → XGBoost ([#36](https://github.com/fedehofmann/oil_and_gas_mlops_pipeline/issues/36)).**

    **Problema actual:** `RandomForestRegressor.fit()` carga el dataset completo en RAM. Esto limita el rango de fechas con el que se puede entrenar localmente a aproximadamente 1 año. Para histórico completo (~2019 en adelante) rompe por OOM. La limitación es **estructural al algoritmo**, no de implementación: Random Forest es un *bagging ensemble* donde cada árbol se entrena sobre un bootstrap sample del dataset completo, por lo que necesita acceso simultáneo a toda la data al armar cada árbol. No tiene `partial_fit` y no puede tenerlo.

    **Por qué el cambio resuelve el OOM:** XGBoost es un *gradient boosting ensemble* construido secuencialmente. Cada árbol nuevo aprende del **error residual** del modelo anterior, no del dataset original. Esa propiedad permite agregar árboles al final de un modelo previo entrenando solo con un chunk nuevo de datos: el batch actual + el modelo previo (chico) son todo lo que necesita en RAM en cada paso.

    **Cuenta de memoria:**

    | Esquema | Memoria peak | Limitante |
    |---|---|---|
    | RF + dataset completo (actual) | `sizeof(dataset) + sizeof(modelo) ≈ 5 GB + 50 MB` | Si el dataset > RAM → OOM, sin escape posible. |
    | XGBoost incremental por chunks | `sizeof(chunk) + sizeof(modelo_creciente) ≈ 500 MB + ~10 MB` | Acotada por `chunk_size`, independiente del dataset total. |

    **Aclaración importante:** la mejora de memoria viene de **dos cambios juntos**, no del modelo solo. Cambiar a XGBoost sin iterar el dataset (`xgb.fit(dataset_completo)`) reproduciría la misma OOM. Lo que reduce memoria es la combinación de:

    1. Un algoritmo que soporta continuación de entrenamiento (XGBoost via `xgb_model` parameter, o SGDRegressor via `partial_fit`).
    2. Un loop de entrenamiento que itera el dataset por batches en lugar de pasarlo entero.

    **Por qué XGBoost sobre SGDRegressor:** SGDRegressor también soporta `partial_fit` y mantiene memoria estrictamente constante. Pero es un modelo lineal — pierde la capacidad de capturar relaciones no-lineales entre features (rolling means, profundidad, tipoextraccion). El proyecto perdería capacidad predictiva. XGBoost preserva la capacidad no-lineal de los árboles y eso es más valioso que la diferencia marginal de memoria entre los dos esquemas (en ambos casos la memoria queda acotada por chunk).

    **Trade-offs aceptados:**
    - El modelo crece linealmente con la cantidad de chunks (cada chunk agrega árboles), pero el crecimiento está acotado: con `max_depth=6` y 100 árboles totales el booster pesa < 10 MB.
    - Hiperparámetros distintos. `n_estimators` en XGBoost incremental significa "árboles a agregar por chunk", no el total — pitfall a tener mapeado.
    - La API de inferencia (`api/main.py`) tiene que migrar de `mlflow.sklearn.load_model` a `mlflow.xgboost.load_model`.

    **Lo que NO resuelve este issue:**
    - El OOM de `prepare_offline_store` (carga del CSV completo + groupby + rolling). Eso es un cuello de botella separado, antes del training. Issue futura: refactorear ese task a procesamiento por chunks o usar Dask/Polars.
    - El OOM de presión total de containers (Ray Serve + entrenamientos + Evidently). Ese se resuelve con más RAM en Docker Desktop o bajando `num_replicas` de Ray Serve.

    **Decisiones técnicas tomadas en la planificación** (todas en el issue #36):
    - Versión: `xgboost==3.2.0`, sklearn-style API (`XGBRegressor.fit(X, y, xgb_model=prev_path)`).
    - Logueo en MLFlow: `mlflow.xgboost.log_model(model_format="ubj")` — formato nativo, no pickle (más portable, preserva metadata categórica).
    - `learning_rate = 0.1` (default 0.3 es agresivo para incremental).
    - Chunks **mensuales** y orden **temporal** (no shuffle): coherente con el split temporal del DAG y con el caso de uso de inferencia (predecir mes siguiente, los meses recientes pesan más).
    - **Mantener `LabelEncoder`** para `tipoextraccion` por ahora; migrar a `enable_categorical=True` queda para un PR aparte.
    - **Issue #13 (LabelEncoder como artefacto)** queda separado: ya tiene su propio scope.
2. **#18 Segundo dataset del RFC** (información complementaria, no obligatorio).
3. **#9 + #10** Quick wins de evaluación desagregada y feature importance (mismo PR, toca `evaluate_model`).
4. **#16 CI/CD básico** con GitHub Actions: tests + lint + validación de schema de `features.py`.
5. **#11 OpenAPI descriptions** en `main.py` (Swagger más usable).
6. **#13 LabelEncoder como artefacto** (cierra training-serving skew latente).
7. **#14 Validación de schema** del CSV con pandera.
8. **#15 Prediction logging** en la API.
9. **#17 Point-in-Time correct** vía `entity_df` con `event_timestamp` en Feast.

#### Issues nuevas detectadas durante #31

- **Snapshot del run anterior como `reference` de Evidently** (en lugar de train). Sería decay "puro" pero requiere persistir el parquet de cada run en el repo de Feast o en S3-equivalente.
- **Score compuesto en `select_best_model`** (R² + RMSE + bias). Más robusto que solo R² para promoción a producción. El roadmap del proyecto lo describe en el ítem 1; queda como issue separada.
- **Evaluar bajar `num_replicas=2 → 1` en Ray Serve** o subir RAM de Docker Desktop. El estado actual obliga a parar `api-1` para correr el DAG, lo cual es operativamente inviable en producción.
- **Cleanup automático de versiones viejas en MLFlow.** El Model Registry acumula todas las versiones entrenadas; con runs mensuales esto crece linealmente. Una política de retención (ej. mantener las últimas 10 + la actual production) evitaría que la búsqueda de "versión anterior" en `monitor_model` se vuelva lenta.
