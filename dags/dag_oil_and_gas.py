from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from sklearn.preprocessing import LabelEncoder
import pandas as pd # Lo usamos en varias tasks asi que tiene sentido importarlo fuera del dag
import os # Lo usamos en varias tasks asi que tiene sentido importarlo fuera del dag
import pickle # Lo usamos en varias tasks asi que tiene sentido importarlo fuera del dag

# -------------------- EXPERIMENTOS --------------------

ALL_FEATURES_PET = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua', 'avg_prod_pet_10m', 'last_prod_pet', 'n_readings']
REDUCED_FEATURES_PET = ['tipoextraccion', 'tef', 'profundidad']

ALL_FEATURES_GAS = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua', 'avg_prod_gas_10m', 'last_prod_gas', 'n_readings']
REDUCED_FEATURES_GAS = ['tipoextraccion', 'tef', 'profundidad']

EXPERIMENTS = [
    # prod_pet: variando n_estimators
    {'target': 'prod_pet', 'model_params': {'n_estimators': 50, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    {'target': 'prod_pet', 'model_params': {'n_estimators': 100, 'random_state': 42}, 'features': ALL_FEATURES_PET},
    # prod_pet: limitando profundidad del árbol
    {'target': 'prod_pet', 'model_params': {'n_estimators': 100, 'random_state': 42, 'max_depth': 5}, 'features': ALL_FEATURES_PET},
    {'target': 'prod_pet', 'model_params': {'n_estimators': 100, 'random_state': 42, 'max_depth': 10}, 'features': ALL_FEATURES_PET},
    # prod_pet: features reducidas
    {'target': 'prod_pet', 'model_params': {'n_estimators': 100, 'random_state': 42}, 'features': REDUCED_FEATURES_PET},
    # prod_gas: variando n_estimators
    {'target': 'prod_gas', 'model_params': {'n_estimators': 50, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    {'target': 'prod_gas', 'model_params': {'n_estimators': 100, 'random_state': 42}, 'features': ALL_FEATURES_GAS},
    # prod_gas: limitando profundidad
    {'target': 'prod_gas', 'model_params': {'n_estimators': 100, 'random_state': 42, 'max_depth': 5}, 'features': ALL_FEATURES_GAS},
    {'target': 'prod_gas', 'model_params': {'n_estimators': 100, 'random_state': 42, 'max_depth': 10}, 'features': ALL_FEATURES_GAS},
    # prod_gas: features reducidas
    {'target': 'prod_gas', 'model_params': {'n_estimators': 100, 'random_state': 42}, 'features': REDUCED_FEATURES_GAS},
]

# -------------------- DAG --------------------

@dag(
    dag_id = 'ml_pipeline_oil_and_gas',
    description = 'Pipeline de Machine Learning con Airflow, MLFlow y Feature Store - Trabajo Práctico Final',
    schedule = '0 0 1 * *', # Corre el primer día de cada mes a medianoche. El dataset es mensual
                             # (producción por pozo por mes), por lo que el reentrenamiento mensual
                             # es la frecuencia natural. En producción con un backend distribuido
                             # para Feast (BigQuery, Spark), correría sobre el rango completo.
                             # En entornos locales se recomienda triggear manualmente con date_from
                             # y date_to para acotar el dataset y evitar OOM.
    params = { # Los params permiten configurar el DAG desde la UI de Airflow sin tocar el código
        'date_from': Param(default = None, type = ['null', 'string'], description = 'Fecha inicio (YYYY-MM-DD). Si es None, usa todos los datos disponibles.'),
        'date_to': Param(default = None, type = ['null', 'string'], description = 'Fecha fin (YYYY-MM-DD). Si es None, usa todos los datos disponibles.'),
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

    Args: url (str) - URL de descarga, save_path (str) - ruta local del archivo.
    Retorna: la ruta del archivo guardado.
    """
    # Creamos la carpeta en disco (Docker)
    os.makedirs(os.path.dirname(save_path), exist_ok = True)

    df = pd.read_csv(url)
    df.to_csv(save_path, index = False)
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

    # Borramos el registry y el SQLite antes de feast apply para garantizar consistencia
    # con el parquet actual. Feast no sobreescribe entradas del online store cuyo timestamp
    # sea más reciente que el nuevo dato. Además, feast apply solo crea las tablas del
    # online store si detecta cambios en el registry — si el registry existe y dice que
    # la infraestructura ya está creada, no recrea las tablas aunque el SQLite no exista.
    # Borrando ambos forzamos una instalación limpia en cada corrida del DAG.
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

    # X es igual para ambos targets: mismas filas, mismas columnas (sin ID, timestamp ni targets)
    non_features = ['idpozo', 'event_timestamp', 'prod_pet', 'prod_gas']
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
      Entrena un RandomForestRegressor según la configuración del experimento,
      guarda el modelo en disco y retorna metadata para que evaluate_model lo evalúe.

      Args: splits (dict) - diccionario con paths a cada uno de los subconjuntos de train y test,
            config (dict) - configuración del experimento con target, features y model_params.
      Retorna: dict con metadata del modelo (target, features, ruta, n_samples, model_params).
      """
      from sklearn.ensemble import RandomForestRegressor

      # Extraemos la configuración del experimento
      target = config['target']
      features = config['features']
      model_params = config['model_params']

      # Construimos el path del modelo en base al target y los hiperparámetros
      model_path = f'/opt/airflow/models/model_{target}_est{model_params["n_estimators"]}_depth{model_params.get("max_depth", "none")}.pkl'

      # Leemos los subconjuntos de train filtrando solo las features del experimento
      X_train = pd.read_parquet(splits.get(target).get('X_train'))[features]
      y_train = pd.read_parquet(splits.get(target).get('y_train')).squeeze()

      # Creamos el modelo con los hiperparámetros del experimento
      # Random Forest construye N árboles de decisión, cada uno entrenado con una muestra aleatoria distinta de los datos y un subconjunto aleatorio de features
      # Para predecir, promedia los resultados de todos los árboles
      model = RandomForestRegressor(**model_params)

      # Lo entrenamos
      model.fit(X_train, y_train)

      # Creamos la carpeta si no existe (pickle falla si la carpeta no existe)
      os.makedirs(os.path.dirname(model_path), exist_ok = True)

      # Guardamos el modelo en disco en formato binario
      # Lo que guardamos es el objeto modelo completo: todos los árboles con sus reglas de split
      with open(model_path, 'wb') as f:
          pickle.dump(model, f)

      return {
          'target': target,
          'features': features,
          'model_path': model_path,
          'n_samples': len(X_train),
          'model_params': model_params
      }
  
  @task
  def evaluate_model(results, splits):
    """
    Carga el modelo entrenado, evalúa sus métricas sobre el conjunto de test,
    las loguea en MLflow y registra el modelo con un nombre para poder cargarlo desde la API.

    Args: results (dict) - metadata del modelo incluyendo target, features y model_path,
          splits (dict) - diccionario con paths a cada uno de los subconjuntos de train y test.
    Retorna: None.
    """
    # Importamos métricas de Sickit Learn
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    # Importamos MLFlow
    import mlflow
    import mlflow.sklearn

    # Definimos las variables que fueron seleccionadas para entrenar (tienen que ser las mismas para no romper)
    features = results['features']

    # Leemos los subconjuntos de train y test
    X_test = pd.read_parquet(splits.get(results['target']).get('X_test'))[features]
    y_test = pd.read_parquet(splits.get(results['target']).get('y_test')).squeeze()

    # Cargamos el modelo
    loaded_model = pickle.load(open(results['model_path'], 'rb'))
    y_pred = loaded_model.predict(X_test)

    # Calculamos métricas en testing
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    r2 = r2_score(y_test, y_pred)

    # Nombre descriptivo del run para identificarlo en la UI de MLflow
    # Ej: "prod_gas_est100_depth5_featall"
    run_name = f"{results['target']}_est{results['model_params']['n_estimators']}_depth{results['model_params'].get('max_depth', 'none')}_feat{'all' if len(results['features']) > 2 else 'reduced'}"

    # Seleccionamos el experimento donde se van a agrupar todos los runs
    mlflow.set_experiment('ml_pipeline_oil_and_gas')

    with mlflow.start_run(run_name = run_name): # Abrimos un nuevo run con ese nombre
        mlflow.log_param('target', results['target']) # Logueamos manualmente el target para filtrar en la UI
        mlflow.log_params(results['model_params']) # Logueamos los hiperparámetros del experimento
        mlflow.log_metric('mae', mae) # Métricas de evaluación
        mlflow.log_metric('mse', mse)
        mlflow.log_metric('rmse', rmse)
        mlflow.log_metric('r2', r2)
        # Registramos el modelo con un nombre para poder cargarlo después desde la API
        # Cada run crea una nueva versión del modelo registrado
        mlflow.sklearn.log_model(loaded_model, "model", registered_model_name = f"oil_gas_{results['target']}")

  @task
  def select_best_model():
    """
    Consulta MLflow, compara todos los modelos registrados por target (prod_gas y prod_pet)
    y promueve a Production el que tenga mejor r2 en cada caso.
    """
    import mlflow
    from mlflow.tracking import MlflowClient

    client = MlflowClient()

    for target in ['prod_gas', 'prod_pet']:
        model_name = f"oil_gas_{target}"

        # Obtenemos todas las versiones del modelo registrado
        versions = client.search_model_versions(f"name = '{model_name}'")

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
  for exp in EXPERIMENTS:
      result = train_model(splits = splits, config = exp) # Entrena el modelo con la config del experimento
      eval_result = evaluate_model(result, splits) # Evalúa y loguea métricas en MLflow
      prev_task >> result >> eval_result # Encadena en serie: el anterior termina antes de que arranque el siguiente
      prev_task = eval_result # El próximo experimento arranca cuando este termina

  # Una vez terminados todos los experimentos, seleccionamos el mejor modelo
  best_model = select_best_model()
  prev_task >> best_model

ml_pipeline()