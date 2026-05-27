from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from sklearn.preprocessing import LabelEncoder
import pandas as pd # Lo usamos en varias tasks asi que tiene sentido importarlo fuera del dag
import os # Lo usamos en varias tasks asi que tiene sentido importarlo fuera del dag

# -------------------- EXPERIMENTOS --------------------

ALL_FEATURES_PET = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua', 'avg_prod_pet_10m', 'last_prod_pet', 'n_readings']
REDUCED_FEATURES_PET = ['tipoextraccion', 'tef', 'profundidad']

ALL_FEATURES_GAS = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua', 'avg_prod_gas_10m', 'last_prod_gas', 'n_readings']
REDUCED_FEATURES_GAS = ['tipoextraccion', 'tef', 'profundidad']

EXPERIMENTS = [
    # XGBoost incremental: n_estimators_per_chunk = árboles agregados por mes (chunk).
    # Total de árboles del modelo final = n_estimators_per_chunk * cantidad de meses en train.
    # Ej: con dataset de ~10 meses de train, n_estimators_per_chunk=10 → ~100 árboles totales.
    # learning_rate=0.1 (más conservador que el default 0.3, ningún chunk individual domina).
    #
    # Grilla validada experimentalmente: configuraciones más agresivas (max_depth=10,
    # learning_rate=0.2) generan overfitting estructural por la combinación incremental
    # + árboles profundos (cada chunk se ajusta al residuo local y memoriza patrones del
    # mes; con muchos chunks se sobreajusta justo a los meses contiguos al test set).
    # Detalle en la bitácora: "Tuning fallido — la config conservadora era el techo".

    # prod_pet: variando n_estimators_per_chunk
    {'target': 'prod_pet', 'model_params': {'n_estimators_per_chunk': 5,  'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    {'target': 'prod_pet', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    # prod_pet: variando max_depth
    {'target': 'prod_pet', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 4, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    {'target': 'prod_pet', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 8, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    # prod_pet: features reducidas (baseline de ablación)
    {'target': 'prod_pet', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': REDUCED_FEATURES_PET},

    # prod_gas: variando n_estimators_per_chunk
    {'target': 'prod_gas', 'model_params': {'n_estimators_per_chunk': 5,  'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    {'target': 'prod_gas', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    # prod_gas: variando max_depth
    {'target': 'prod_gas', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 4, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    {'target': 'prod_gas', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 8, 'learning_rate': 0.1, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    # prod_gas: features reducidas
    {'target': 'prod_gas', 'model_params': {'n_estimators_per_chunk': 10, 'max_depth': 6, 'learning_rate': 0.1, 'random_state': 42}, 'features': REDUCED_FEATURES_GAS},
]

# -------------------- DAG --------------------

@dag(
    dag_id = 'ml_pipeline_oil_and_gas',
    description = 'Pipeline de Machine Learning con Airflow, MLFlow y Feature Store - Trabajo Práctico Final',
    schedule = '0 0 1 * *', # Corre el primer día de cada mes a medianoche
    params = { # Los params permiten configurar el DAG desde la UI de Airflow sin tocar el código
        'date_from': Param(default = None, type = ['null', 'string'], description = 'Fecha inicio (YYYY-MM-DD). Si es None, usa todos los datos disponibles.'),
        'date_to': Param(default = None, type = ['null', 'string'], description = 'Fecha fin (YYYY-MM-DD). Si es None, usa todos los datos disponibles.'),
        'exclude_years': Param(default = [2020], type = ['null', 'array'], items = {'type': 'integer'}, description = 'Años a excluir del entrenamiento por ser atípicos. Default [2020]: excluye el año distorsionado por COVID-19. Para excluir varios años, listarlos separados por coma (ej. [2020, 2022] excluye 2020 y 2022, no el rango entre ambos). Dejar vacío para no excluir ninguno. Ver Decisiones de diseño en README.'),
    }
)
def ml_pipeline():
  start = EmptyOperator(
      task_id = 'start',
  )

  @task
  def download_dataset(url, save_path):
    """
    Descarga un dataset CSV desde una URL y lo guarda en disco.

    Usa streaming directo a disco (urllib + chunks) en lugar de pd.read_csv(url) +
    df.to_csv() para evitar cargar el CSV completo en memoria. Es importante en este
    entorno: el worker container ya tiene huella alta por las deps de Evidently
    (matplotlib, plotly, dask, statsmodels) y mantener el dataset en RAM puede
    desbordar el límite de Docker Desktop con OOM.

    Args: url (str) - URL de descarga, save_path (str) - ruta local del archivo.
    Retorna: la ruta del archivo guardado.
    """
    import urllib.request
    import shutil

    os.makedirs(os.path.dirname(save_path), exist_ok = True)

    # Streaming: copia chunks directo de la respuesta HTTP al archivo en disco
    with urllib.request.urlopen(url) as response, open(save_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

    return save_path

  @task
  def prepare_offline_store(read_csv_path, feature_store_repo, **context):
    """
    Lee el CSV, calcula los features de ventana para cada pozo y mes, y genera el offline store.
    Opcionalmente filtra por rango de fechas usando los params del DAG.
    Además agrega una fila futura por pozo (sin target) para el online store.
    Al finalizar ejecuta feast apply para registrar las definiciones en el registry.

    Args: read_csv_path (str) - ruta al CSV descargado por download_dataset.
        feature_store_repo (str) - path al repositorio de Feast.
        **context - contexto de Airflow, usado para acceder a los params date_from y date_to.
    Retorna: None. Guarda el parquet en feature_store_repo/data/well_features.parquet.
    """

    # Importamos librerías
    import subprocess

    params = context['params']
    date_from = params.get('date_from')
    date_to = params.get('date_to')
    exclude_years = params.get('exclude_years') or []

    df = pd.read_csv(read_csv_path)
    
    # En primer lugar, creamos el campo fecha
    df['fecha'] = pd.to_datetime(df['anio'].astype(str) + '-' + df['mes'].astype(str).str.zfill(2) + '-01')

    # En segundo lugar seleccionamos las columnas que serán parte de nuestro offline store
    columns = ['idpozo', 'fecha', 'prod_pet', 'prod_gas', 'tipoextraccion', 'profundidad', 'tef', 'prod_agua']

    # En tercer lugar filtramos las columnas, dropeamos nulos y ordenamos ascendentemente para calcular las nuevas variables explicativas
    # El filtro de fechas se aplica DESPUÉS del cálculo de features para preservar el contexto histórico del rolling
    df = (df[columns]
          .dropna()
          .sort_values(['idpozo', 'fecha'])
          .reset_index(drop = True)
          )

    # En cuarto lugar codificamos las variables categóricas, en nuestro caso únicamente 'tipoextraccion'
    le = LabelEncoder()
    df['tipoextraccion'] = le.fit_transform(df['tipoextraccion'].astype(str))

    # En quinto lugar, agregamos las nuevas variables calculadas usando method chaining y Pandas (vectorizado)
    df = df.assign(
        # shift(1) evita data leakage: cuando estamos en marzo, miro febrero hacia atrás
        avg_prod_gas_10m = lambda x: x.groupby('idpozo')['prod_gas'].transform(lambda x: x.shift(1).rolling(10, min_periods = 1).mean()),
        avg_prod_pet_10m = lambda x: x.groupby('idpozo')['prod_pet'].transform(lambda x: x.shift(1).rolling(10, min_periods = 1).mean()),

        # Último valor conocido antes de este mes
        last_prod_gas = lambda x: x.groupby('idpozo')['prod_gas'].shift(1),
        last_prod_pet = lambda x: x.groupby('idpozo')['prod_pet'].shift(1),

        # Cuántas lecturas lleva el pozo hasta este momento
        n_readings = lambda x: x.groupby('idpozo').cumcount() + 1
    )

    # Aplicamos el filtro de fechas DESPUÉS de calcular los features
    # Así el rolling de la primera fila del rango usa toda la historia disponible, no solo desde date_from
    if date_from:
        df = df[df['fecha'] >= date_from].reset_index(drop = True)
    if date_to:
        df = df[df['fecha'] <= date_to].reset_index(drop = True)
    if exclude_years:
        df = df[~df['fecha'].dt.year.isin(exclude_years)].reset_index(drop = True)

    # Generamos la fila futura por pozo para el online store (sin target)
    future = (df.groupby('idpozo').tail(1).copy()
        .assign(
            fecha = lambda x: x['fecha'] + pd.DateOffset(months = 1),
            prod_gas = None,
            prod_pet = None,
        )
    )

    # Unimos el histórico con las filas futuras
    offline_feat_df = pd.concat([df, future]).sort_values(['idpozo', 'fecha']).reset_index(drop = True)

    # Definimos el path del parquet y creamos la carpeta si no existe
    offline_parquet_path = os.path.join(feature_store_repo, 'data/well_features.parquet')
    os.makedirs(os.path.dirname(offline_parquet_path), exist_ok = True)

    # Guardamos el DataFrame en formato parquet
    offline_feat_df.to_parquet(offline_parquet_path, index = False)

    # Feast no sobreescribe entradas con timestamp más viejo ni recrea tablas si el registry ya existe
    # Borramos ambos para forzar una instalación limpia en cada corrida
    for path in [
        os.path.join(feature_store_repo, 'online_store/online.db'),
        os.path.join(feature_store_repo, 'registry/registry.db'),
    ]:
        if os.path.exists(path):
            os.remove(path)

    # Registramos las definiciones de features.py en el registry de Feast y apuntamos al parquet recién generado
    subprocess.run(['feast', 'apply'], cwd = feature_store_repo, check = True)

  @task
  def populate_online_store(feature_store_repo):
    """
    Lee el parquet del offline store, toma la última fila de cada pozo
    (la fila futura sin target) y la materializa en el online store (SQLite).

    feast apply recrea las tablas del SQLite antes de escribir, garantizando
    consistencia con el parquet actual (el borrado del SQLite ocurre en
    prepare_offline_store antes del feast apply de esa task).
    """
    # Importamos librería
    from feast import FeatureStore

    # Construimos el path del parquet a partir del repo de Feast
    offline_parquet_path = os.path.join(feature_store_repo, 'data/well_features.parquet')

    # Cargamos el DataFrame presente en el offline store
    offline_feat_df = pd.read_parquet(offline_parquet_path)

    # Nos quedamos con la última fila de cada pozo (la fila futura, sin target)
    latest_df = offline_feat_df.sort_values('fecha').groupby('idpozo').tail(1)

    # Inicializamos el Feature Store y materializamos
    store = FeatureStore(repo_path = feature_store_repo)
    store.write_to_online_store(
        feature_view_name = 'well_stats',
        df = latest_df,
    )
  @task
  def split_data(feature_store_repo):
    """
    Obtiene los features históricos del offline store via Feast usando get_historical_features,
    que hace un point-in-time lookup: para cada par (idpozo, fecha), devuelve los features
    tal como eran en esa fecha, garantizando que no haya data leakage.

    Después descarta las filas futuras (sin target) y divide en train/test con un split
    temporal (train = pasado, test = futuro).

    Args: feature_store_repo (str) - path al repositorio de Feast.
    Retorna: dict con los paths a los archivos parquet de cada subconjunto (X_train, X_test, y_train, y_test).
    """
    from feast import FeatureStore

    # Leemos el parquet para obtener las llaves de entidades
    offline_parquet_path = os.path.join(feature_store_repo, 'data/well_features.parquet')
    raw_df = pd.read_parquet(offline_parquet_path)

    # Feast necesita idpozo y event_timestamp para el point-in-time lookup
    entity_df = raw_df[['idpozo', 'fecha', 'prod_gas', 'prod_pet']].copy()
    entity_df['fecha'] = pd.to_datetime(entity_df['fecha'])
    entity_df = entity_df.rename(columns = {'fecha': 'event_timestamp'})

    # Nos conectamos al feature store y obtenemos los features históricos
    store = FeatureStore(repo_path = feature_store_repo)
    training_df = store.get_historical_features(
        entity_df = entity_df,
        features = [
            'well_stats:tipoextraccion',
            'well_stats:avg_prod_gas_10m',
            'well_stats:avg_prod_pet_10m',
            'well_stats:last_prod_gas',
            'well_stats:last_prod_pet',
            'well_stats:n_readings',
            'well_stats:profundidad',
            'well_stats:tef',
            'well_stats:prod_agua',
        ],
    ).to_df()

    # Sacamos los prefijos "well_stats__" que agrega Feast
    training_df.columns = [c.split('__')[-1] for c in training_df.columns]

    # Dropeamos las filas futuras (prod_gas = None) que agregamos para el online store
    training_df = training_df.dropna(subset = ['prod_gas', 'prod_pet'])

    # Split temporal: el 80% más antiguo es train, el 20% más reciente es test
    # Para series de tiempo esto es correcto: el modelo nunca ve datos futuros durante el entrenamiento
    training_df = training_df.sort_values('event_timestamp').reset_index(drop = True)
    unique_dates = sorted(training_df['event_timestamp'].unique())
    cutoff_date = unique_dates[int(len(unique_dates) * 0.8)]

    train_df = training_df[training_df['event_timestamp'] <  cutoff_date]
    test_df  = training_df[training_df['event_timestamp'] >= cutoff_date]

    # X es igual para ambos targets: mismas filas, mismas columnas.
    # IMPORTANTE: mantenemos event_timestamp en X para que train_model pueda iterar por mes
    # (chunks mensuales para incremental learning con XGBoost). En train_model y evaluate_model
    # se filtra X a `[features]` antes de pasarlo al modelo, así que event_timestamp queda
    # disponible para particionar el dataset pero nunca llega como feature al modelo.
    non_features = ['idpozo', 'prod_pet', 'prod_gas']
    X_train = train_df.drop(columns = non_features)
    X_test  = test_df.drop(columns = non_features)

    # Creamos la carpeta si no existe y guardamos los splits en disco
    os.makedirs('/opt/airflow/data/splits', exist_ok = True)

    X_train.to_parquet('/opt/airflow/data/splits/X_train.parquet')
    X_test.to_parquet('/opt/airflow/data/splits/X_test.parquet')

    train_df[['prod_pet']].to_parquet('/opt/airflow/data/splits/y_pet_train.parquet')
    test_df[['prod_pet']].to_parquet('/opt/airflow/data/splits/y_pet_test.parquet')
    train_df[['prod_gas']].to_parquet('/opt/airflow/data/splits/y_gas_train.parquet')
    test_df[['prod_gas']].to_parquet('/opt/airflow/data/splits/y_gas_test.parquet')

    return {
        'prod_pet': {
            'X_train': '/opt/airflow/data/splits/X_train.parquet',
            'X_test': '/opt/airflow/data/splits/X_test.parquet',
            'y_train': '/opt/airflow/data/splits/y_pet_train.parquet',
            'y_test': '/opt/airflow/data/splits/y_pet_test.parquet',
        },
        'prod_gas': {
            'X_train': '/opt/airflow/data/splits/X_train.parquet',
            'X_test': '/opt/airflow/data/splits/X_test.parquet',
            'y_train': '/opt/airflow/data/splits/y_gas_train.parquet',
            'y_test': '/opt/airflow/data/splits/y_gas_test.parquet',
        }
    }
  @task
  def train_model(splits, config):
      """
      Entrena un XGBRegressor en chunks mensuales (incremental learning) según la
      configuración del experimento, guarda el booster final en disco y retorna metadata.

      A diferencia de RandomForest, donde cada `fit` requiere todo el dataset en RAM,
      XGBoost permite continuar el entrenamiento sobre un modelo previo: cada chunk
      agrega árboles nuevos al final del booster sin tocar los anteriores. Esto acota
      el uso de memoria a `chunk_actual + booster_creciente` (~MB) en lugar de
      `dataset_completo + modelo` (~GB con histórico extenso).

      Iteración por mes en orden temporal: el split de Feast ya viene con
      `event_timestamp`. Se agrupa por `(año, mes)` y se hace `XGBRegressor.fit`
      sobre cada chunk pasando `xgb_model=booster_path` para continuar el modelo
      previo. El orden temporal es deliberado: los meses recientes pesan más en el
      modelo final, lo cual es coherente con el caso de uso de inferencia
      (predecir el mes siguiente).

      Args: splits (dict) - paths a los subconjuntos train/test (output de split_data).
            config (dict) - configuración del experimento. `model_params` debe incluir
            `n_estimators_per_chunk` (árboles a agregar por mes), `max_depth`,
            `learning_rate`, `random_state`.
      Retorna: dict con metadata del modelo (target, features, model_path, n_samples,
            n_chunks, model_params con n_estimators_total calculado).
      """
      import xgboost as xgb

      # Extraemos la configuración del experimento
      target = config['target']
      features = config['features']
      base_params = config['model_params']
      n_estimators_per_chunk = base_params['n_estimators_per_chunk']

      # Convertimos a hiperparámetros aceptados por XGBRegressor
      # (sacamos n_estimators_per_chunk y agregamos n_estimators con su valor)
      xgb_params = {k: v for k, v in base_params.items() if k != 'n_estimators_per_chunk'}
      xgb_params['n_estimators'] = n_estimators_per_chunk

      # Path del modelo en formato .ubj (nativo de XGBoost — más portable que pickle entre versiones)
      model_path = f'/opt/airflow/models/model_{target}_est{n_estimators_per_chunk}_depth{xgb_params.get("max_depth", "default")}_feat{"all" if len(features) > 3 else "reduced"}.ubj'
      os.makedirs(os.path.dirname(model_path), exist_ok = True)

      # Leemos X_train completo (con event_timestamp) y y_train
      X_train = pd.read_parquet(splits.get(target).get('X_train'))
      y_train = pd.read_parquet(splits.get(target).get('y_train')).squeeze()

      # Aseguramos que event_timestamp es datetime para poder agrupar por mes
      X_train['event_timestamp'] = pd.to_datetime(X_train['event_timestamp'])

      # Iteramos por mes en orden temporal (sort=True por default en groupby)
      booster_path = None
      n_chunks = 0
      n_samples_total = 0

      for periodo, X_chunk in X_train.groupby(X_train['event_timestamp'].dt.to_period('M')):
          # Aislamos las features que el modelo usa (event_timestamp, idpozo, etc. no entran)
          X_chunk_features = X_chunk[features]
          y_chunk = y_train.loc[X_chunk.index]

          # Creamos un nuevo XGBRegressor por chunk con los mismos hiperparámetros estructurales.
          # Pasar xgb_model=booster_path le dice a XGBoost que continúe el modelo previo
          # (sin xgb_model, .fit() reentrena desde cero silenciosamente — pitfall conocido).
          model = xgb.XGBRegressor(**xgb_params)
          if booster_path is not None:
              model.fit(X_chunk_features, y_chunk, xgb_model = booster_path)
          else:
              model.fit(X_chunk_features, y_chunk)

          # Guardamos el booster en formato nativo .ubj (sobreescribe la versión anterior)
          model.save_model(model_path)
          booster_path = model_path
          n_chunks += 1
          n_samples_total += len(X_chunk)

      # Devolvemos metadata. n_estimators_total refleja la cantidad real de árboles
      # del modelo final (cada chunk agregó n_estimators_per_chunk árboles).
      return {
          'target': target,
          'features': features,
          'model_path': model_path,
          'n_samples': n_samples_total,
          'n_chunks': n_chunks,
          'model_params': {
              **xgb_params,
              'n_estimators_per_chunk': n_estimators_per_chunk,
              'n_estimators_total': n_estimators_per_chunk * n_chunks,
          }
      }
  
  @task
  def evaluate_model(results, splits):
    """
    Carga el modelo XGBoost entrenado en chunks, evalúa sus métricas sobre el conjunto
    de test, las loguea en MLflow y registra el modelo en el Model Registry para que
    la API pueda cargarlo en inferencia.

    Args: results (dict) - metadata del modelo incluyendo target, features y model_path
          (booster en formato .ubj generado por train_model).
          splits (dict) - paths a los subconjuntos train/test.
    Retorna: dict con target y run_id (consumido por select_best_model).
    """
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    import xgboost as xgb
    import mlflow
    import mlflow.xgboost
    from mlflow.tracking import MlflowClient

    features = results['features']

    # Leemos test set filtrado a las features del experimento (X_test puede traer
    # event_timestamp por el cambio en split_data, así que lo filtramos explícitamente)
    X_test = pd.read_parquet(splits.get(results['target']).get('X_test'))[features]
    y_test = pd.read_parquet(splits.get(results['target']).get('y_test')).squeeze()

    # Cargamos el booster desde el path .ubj que generó train_model
    loaded_model = xgb.XGBRegressor()
    loaded_model.load_model(results['model_path'])
    y_pred = loaded_model.predict(X_test)

    # Métricas en testing
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    r2 = r2_score(y_test, y_pred)

    # Nombre descriptivo del run para identificarlo en la UI de MLflow
    # Ej: "prod_gas_est10pc_depth6_featall" — pc = per chunk
    n_estimators_pc = results['model_params']['n_estimators_per_chunk']
    n_estimators_total = results['model_params']['n_estimators_total']
    depth_label = str(results['model_params'].get('max_depth', 'default'))
    feat_label = 'all' if len(results['features']) > 3 else 'reduced'
    run_name = f"{results['target']}_est{n_estimators_pc}pc_depth{depth_label}_feat{feat_label}"

    # Seleccionamos el experimento donde se van a agrupar todos los runs
    mlflow.set_experiment('ml_pipeline_oil_and_gas')

    with mlflow.start_run(run_name = run_name):
        mlflow.log_param('target', results['target'])
        mlflow.log_param('n_chunks', results['n_chunks'])
        mlflow.log_params(results['model_params'])  # incluye n_estimators_per_chunk y _total
        mlflow.log_metric('mae', mae)
        mlflow.log_metric('mse', mse)
        mlflow.log_metric('rmse', rmse)
        mlflow.log_metric('r2', r2)
        # Registramos el modelo con formato .ubj nativo (más portable que pickle entre versiones).
        # mlflow.xgboost guarda el booster en formato JSON/UBJ y preserva metadata específica
        # de XGBoost (incluyendo soporte para enable_categorical=True si se migra a ese esquema).
        mlflow.xgboost.log_model(
            loaded_model,
            name = "model",
            model_format = "ubj",
            registered_model_name = f"oil_gas_{results['target']}",
        )

        # Tags y descripción en el Model Registry para que sea legible sin abrir cada run
        client = MlflowClient()
        run_id = mlflow.active_run().info.run_id
        versions = client.search_model_versions(f"run_id='{run_id}'")
        if versions:
            model_name = f"oil_gas_{results['target']}"
            version = versions[0].version

            client.set_model_version_tag(model_name, version, 'run_name', run_name)
            client.set_model_version_tag(model_name, version, 'n_estimators_per_chunk', str(n_estimators_pc))
            client.set_model_version_tag(model_name, version, 'n_estimators_total', str(n_estimators_total))
            client.set_model_version_tag(model_name, version, 'max_depth', depth_label)
            client.set_model_version_tag(model_name, version, 'learning_rate', str(results['model_params'].get('learning_rate', 'default')))
            client.set_model_version_tag(model_name, version, 'features', feat_label)
            client.set_model_version_tag(model_name, version, 'r2', f"{r2:.4f}")

            client.update_model_version(
                name = model_name,
                version = version,
                description = f"XGBoost incremental | {n_estimators_pc} árboles/chunk × {results['n_chunks']} chunks = {n_estimators_total} árboles | max_depth={depth_label} | features={feat_label} | R²={r2:.4f} | RMSE={rmse:.2f}"
            )

            # Descripción a nivel del modelo registrado (estática)
            fluid = 'petróleo' if results['target'] == 'prod_pet' else 'gas'
            feat_list = ', '.join(results['features'])
            client.update_registered_model(
                name = model_name,
                description = f"Predice producción mensual de {fluid} (m³) por pozo. Entrenado con XGBoost incremental por chunks mensuales sobre datos del MINEM (producción no convencional). Features: {feat_list}."
            )

        return {"target": results['target'], "run_id": run_id}

  @task
  def select_best_model(eval_results):
    """
    Recibe los run_ids de los experimentos del DAG actual, compara solo esas versiones
    por target (prod_gas y prod_pet) y promueve a Production la de mejor r2.

    Filtra por run_id para que el resultado sea determinístico e independiente del
    historial acumulado en MLflow: dos instancias distintas (ej: distintos colaboradores)
    llegan al mismo ganador si corrieron el DAG con los mismos datos.
    """
    import mlflow
    from mlflow.tracking import MlflowClient

    client = MlflowClient()

    for target in ['prod_gas', 'prod_pet']:
        model_name = f"oil_gas_{target}"

        # Filtramos solo los run_ids del DAG actual para este target
        current_run_ids = {r['run_id'] for r in eval_results if r['target'] == target}

        # Obtenemos todas las versiones del modelo registrado y filtramos por corrida actual
        versions = client.search_model_versions(f"name = '{model_name}'")
        versions = [v for v in versions if v.run_id in current_run_ids]

        if not versions:
            print(f"No hay versiones registradas para {model_name}")
            continue

        # Buscamos el run con mejor r2 para este modelo
        best_version = None
        best_r2 = float('-inf')

        for v in versions:
            run = mlflow.get_run(v.run_id)
            r2 = run.data.metrics.get('r2', float('-inf'))
            if r2 > best_r2:
                best_r2 = r2
                best_version = v.version

        # Seteamos el alias "production" en la mejor versión
        client.set_registered_model_alias(
            name = model_name,
            alias = "production",
            version = best_version,
        )
        print(f"Modelo {model_name} v{best_version} promovido a Production (r2 = {best_r2:.4f})")

  @task
  def monitor_model(eval_results, splits):
    """
    Genera un reporte de Evidently AI por cada modelo promovido a producción y compara
    su R² contra el del modelo que estaba en producción antes de este run del DAG.

    Para cada target (prod_gas, prod_pet):
      - Carga el modelo con alias `production` desde MLFlow.
      - Construye reference = dataset de train y current = dataset de test, ambos con
        predicciones del modelo, y corre RegressionPreset + DataDriftPreset.
      - Loguea métricas clave en el run del modelo en producción:
          * monitor_r2 (R² en current)
          * monitor_drift_share (% de features con drift entre train y test)
          * monitor_r2_delta (R² actual menos R² del último production anterior, si existe)
      - Emite warning en el log de Airflow si:
          * R² < MODEL_QUALITY_R2_FLOOR (calidad mínima del modelo en datos recientes)
          * drift_share > MODEL_DECAY_DRIFT_SHARE_THRESHOLD (data drift severo)
          * monitor_r2_delta < -MODEL_DECAY_R2_DELTA (decay temporal vs. run anterior)
        Las alertas son blandas: no abortan el DAG, solo dejan trazas visibles.

    Args: eval_results (list) - run_ids de los experimentos del DAG actual, usado para
                                identificar la versión "anterior" en el Model Registry.
          splits (dict) - paths a los parquets de cada subconjunto train/test.
    """
    import logging
    import mlflow
    import mlflow.xgboost
    from mlflow.tracking import MlflowClient
    from evidently.report import Report
    from evidently.metric_preset import RegressionPreset, DataDriftPreset
    from evidently.pipeline.column_mapping import ColumnMapping

    log = logging.getLogger(__name__)
    r2_floor = float(os.getenv('MODEL_QUALITY_R2_FLOOR', '0.85'))
    drift_threshold = float(os.getenv('MODEL_DECAY_DRIFT_SHARE_THRESHOLD', '0.5'))
    r2_delta_threshold = float(os.getenv('MODEL_DECAY_R2_DELTA', '0.05'))
    client = MlflowClient()

    # Run_ids del DAG actual: las versiones del Model Registry creadas en este DAG
    # llevan estos run_ids. Para encontrar la versión "anterior" filtramos por las que NO los tienen.
    current_run_ids = {r['run_id'] for r in eval_results}

    report_dir = '/opt/airflow/data/monitoring'
    os.makedirs(report_dir, exist_ok = True)

    for target in ['prod_gas', 'prod_pet']:
        model_name = f"oil_gas_{target}"

        # Cargamos el modelo promovido a producción y leemos sus features.
        # XGBRegressor (sklearn API) expone feature_names_in_ vía property que delega
        # al booster — funciona igual que con sklearn estimators.
        loaded_model = mlflow.xgboost.load_model(f"models:/{model_name}@production")
        features = list(loaded_model.feature_names_in_)

        # Leemos splits restringidos a las features del modelo
        target_splits = splits[target]
        X_train = pd.read_parquet(target_splits['X_train'])[features]
        X_test = pd.read_parquet(target_splits['X_test'])[features]
        y_train = pd.read_parquet(target_splits['y_train']).squeeze()
        y_test = pd.read_parquet(target_splits['y_test']).squeeze()

        # Construimos reference y current con target + prediction (formato esperado por Evidently)
        ref_df = X_train.assign(target = y_train.values, prediction = loaded_model.predict(X_train))
        cur_df = X_test.assign(target = y_test.values, prediction = loaded_model.predict(X_test))

        # Column mapping: tipoextraccion es la única categórica (encodada con LabelEncoder)
        column_mapping = ColumnMapping(
            target = 'target',
            prediction = 'prediction',
            numerical_features = [c for c in features if c != 'tipoextraccion'],
            categorical_features = [c for c in features if c == 'tipoextraccion'],
        )

        # Generamos el reporte combinado: regression performance + data drift.
        # Stattest elegidos: tests basados en magnitud, no en significancia estadística.
        # Razón: tests de hipótesis (KS, chi-square con p-value) son ultra-sensibles al
        # tamaño de muestra — con miles de filas detectan cualquier diferencia minúscula
        # como "drift". Tests de magnitud (PSI, Jensen-Shannon) miden cuánto cambió la
        # distribución, no si el cambio es estadísticamente significativo, y por eso
        # no escalan mal con el tamaño del sample.
        #   - Numéricas: PSI (Population Stability Index) con threshold 0.1.
        #     Threshold de la industria: PSI < 0.1 sin drift, 0.1-0.25 moderado, > 0.25 severo.
        #     Mismo enfoque propuesto originalmente en el roadmap del proyecto.
        #   - Categóricas: Jensen-Shannon distance con threshold 0.1.
        #     Análogo a PSI pero para variables categóricas; threshold consistente.
        report = Report(metrics = [
            RegressionPreset(),
            DataDriftPreset(
                num_stattest = 'psi', num_stattest_threshold = 0.1,
                cat_stattest = 'jensenshannon', cat_stattest_threshold = 0.1,
            ),
        ])
        report.run(reference_data = ref_df, current_data = cur_df, column_mapping = column_mapping)

        report_path = os.path.join(report_dir, f'monitor_{target}.html')
        report.save_html(report_path)

        # Extraemos métricas del reporte. Tres niveles:
        #   - r2: regression performance global (current).
        #   - drift_share: % agregado de features con drift (alerta global).
        #   - column_drift_scores: drift_score por feature individual, para visibilidad granular
        #     y poder identificar cuál feature genera el drift sin abrir el HTML.
        r2, drift_share = None, None
        column_drift_scores = {}
        for m in report.as_dict().get('metrics', []):
            metric_type = m.get('metric')
            result = m.get('result', {})
            if metric_type == 'RegressionQualityMetric':
                r2 = result.get('current', {}).get('r2_score')
            elif metric_type == 'DatasetDriftMetric':
                drift_share = result.get('share_of_drifted_columns')
            elif metric_type == 'DataDriftTable':
                for col, info in result.get('drift_by_columns', {}).items():
                    score = info.get('drift_score')
                    if score is not None:
                        column_drift_scores[col] = score

        # Decay temporal: comparar R² actual contra el último production anterior a este DAG run
        # MLFlow no guarda historia de aliases, así que aproximamos tomando la versión más reciente
        # del modelo cuyo run_id NO pertenece al DAG actual. Es la versión que estaba en el Registry
        # antes de que arrancara este DAG, candidata más razonable a "production anterior".
        previous_r2 = None
        all_versions = client.search_model_versions(f"name = '{model_name}'")
        all_versions.sort(key = lambda v: int(v.creation_timestamp), reverse = True)
        previous_versions = [v for v in all_versions if v.run_id not in current_run_ids]
        if previous_versions:
            previous_r2 = mlflow.get_run(previous_versions[0].run_id).data.metrics.get('r2')

        delta_r2 = None
        if r2 is not None and previous_r2 is not None:
            delta_r2 = r2 - previous_r2

        # Asociamos el reporte y métricas al run del modelo en producción para tener todo en un solo lugar
        production_version = client.get_model_version_by_alias(model_name, "production")
        with mlflow.start_run(run_id = production_version.run_id):
            mlflow.log_artifact(report_path, artifact_path = 'monitoring')
            if r2 is not None:
                mlflow.log_metric('monitor_r2', r2)
            if drift_share is not None:
                mlflow.log_metric('monitor_drift_share', drift_share)
            if delta_r2 is not None:
                mlflow.log_metric('monitor_r2_delta', delta_r2)
            # Drift score por feature: permite ver qué feature en particular tiene el drift
            # más alto run-a-run sin abrir el HTML de Evidently.
            for col, score in column_drift_scores.items():
                mlflow.log_metric(f'monitor_drift_{col}', score)

        # Alertas blandas: el DAG no aborta, solo deja warnings visibles en el log
        msg_prefix = f"[MODEL_DECAY] {model_name}"
        if r2 is not None and r2 < r2_floor:
            log.warning(f"{msg_prefix} R² ({r2:.4f}) < floor ({r2_floor})")
        if drift_share is not None and drift_share > drift_threshold:
            log.warning(f"{msg_prefix} drift share ({drift_share:.0%}) > threshold ({drift_threshold:.0%})")
        if delta_r2 is not None and delta_r2 < -r2_delta_threshold:
            log.warning(f"{msg_prefix} R² delta ({delta_r2:+.4f}) < -{r2_delta_threshold} (decay vs. run anterior, R² previo = {previous_r2:.4f})")
        elif previous_r2 is None:
            log.info(f"{msg_prefix} sin run anterior para comparar decay (primera corrida del modelo)")
        log.info(f"{msg_prefix} reporte: {report_path} (R²={r2}, drift_share={drift_share}, delta_r2={delta_r2})")

  # -------------------- SECUENCIAS --------------------

  FEATURE_STORE_REPO = '/opt/airflow/feature_store'
  dataset_download_url = 'http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv'

  # Descargamos el dataset
  csv_path = download_dataset(
      url = dataset_download_url,
      save_path = '/opt/airflow/data/pozos.csv'
  )

  # Preparamos el offline store y registramos en Feast
  offline_store = prepare_offline_store(
      read_csv_path = csv_path,
      feature_store_repo = FEATURE_STORE_REPO
  )

  # Materializamos el online store
  online_store = populate_online_store(
      feature_store_repo = FEATURE_STORE_REPO
  )

  # Spliteamos el dataset consumiendo del feature store
  splits = split_data(
      feature_store_repo = FEATURE_STORE_REPO
  )

  # Definimos dependencias iniciales
  start >> csv_path >> offline_store >> online_store >> splits

  # Loop sobre los experimentos encadenados en serie
  prev_task = splits
  eval_results = []
  for exp in EXPERIMENTS:
      result = train_model(splits = splits, config = exp) # Entrena el modelo con la config del experimento
      eval_result = evaluate_model(result, splits) # Evalúa y loguea métricas en MLflow
      prev_task >> result >> eval_result # Encadena en serie: el anterior termina antes de que arranque el siguiente
      prev_task = eval_result # El próximo experimento arranca cuando este termina
      eval_results.append(eval_result) # Acumulamos el run_id de cada experimento

  # Una vez terminados todos los experimentos, seleccionamos el mejor modelo
  # Pasamos los run_ids del DAG actual para que compare solo versiones de esta corrida
  best_model = select_best_model(eval_results)
  prev_task >> best_model

  # Generamos el reporte de monitoreo del modelo en producción
  monitor = monitor_model(eval_results = eval_results, splits = splits)
  best_model >> monitor

ml_pipeline()