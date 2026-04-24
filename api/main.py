from fastapi import FastAPI, HTTPException
from feast import FeatureStore
from ray import serve
import mlflow.sklearn
import pandas as pd
import os

# -------------------- CONFIGURACIÓN --------------------

# Paths al feature store y al parquet dentro del contenedor Docker
FEATURE_STORE_REPO = '/opt/airflow/feature_store'
PARQUET_PATH = os.path.join(FEATURE_STORE_REPO, 'data/well_features.parquet')

# FastAPI genera documentación Swagger automáticamente en /docs
app = FastAPI(
    title = "Oil & Gas Forecast API",
    version = "1.0.0",
    description = "API simple para consultar el listado de pozos y sus pronósticos de producción."
)

# -------------------- DEPLOYMENT --------------------

@serve.deployment(
    num_replicas = 2,            # Dos réplicas permiten paralelizar inferencia sin explotar memoria local
    max_queued_requests = 30,    # Límite de cola: bajo saturación devuelve 503 en vez de degradar latencia
)
@serve.ingress(app)
class APIDeployment:
    """
    Deployment unificado con ambos modelos (gas y petróleo) cargados en memoria.
    Los modelos y el feature store se inicializan una vez por réplica,
    eliminando el overhead de cargarlos en cada request.

    Ver "Decisiones de diseño → Arquitectura de serving" en el README.
    """

    def __init__(self):
        # Cargamos ambos modelos desde MLFlow una sola vez por réplica
        self.model_gas = mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production")
        self.model_pet = mlflow.sklearn.load_model("models:/oil_gas_prod_pet@production")

        # Feature store client también reutilizable entre requests
        self.store = FeatureStore(repo_path = FEATURE_STORE_REPO)

    # -------------------- ENDPOINTS --------------------

    @app.get("/api/v1/wells")
    def get_wells(self, date_query: str):
        """
        Devuelve el listado de pozos que tienen registros para la fecha dada.
        La fecha debe ser el primer día del mes (ej: 2022-01-01).
        """
        try:
            # Leemos el parquet del offline store (una fila por pozo por mes)
            df = pd.read_parquet(PARQUET_PATH)
            df['fecha'] = pd.to_datetime(df['fecha'])
            date = pd.to_datetime(date_query)

            # Filtramos por fecha exacta y devolvemos los IDs únicos
            pozos = df[df['fecha'] == date]['idpozo'].unique().tolist()

            if not pozos:
                raise HTTPException(status_code = 404, detail = f"No se encontraron pozos para la fecha {date_query}")

            return [{"id_well": str(p)} for p in pozos]

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code = 500, detail = str(e))

    @app.get("/api/v1/forecast")
    def get_forecast(self, id_well: str, date_start: str, date_end: str, target: str = "gas"):
        """
        Devuelve el pronóstico de producción de un pozo para cada mes entre date_start y date_end.

            - Para fechas históricas con datos reales usa los features del offline store (parquet).

            - Para fechas futuras o la fila futura del parquet (target nulo) usa el online store.

        No se hace actualización autoregresiva para evitar distribution shift en avg_prod_10m.
        """
        try:
            if target not in ("gas", "pet"):
                raise HTTPException(status_code = 400, detail = "target debe ser 'gas' o 'pet'")

            # Definimos la columna target y seleccionamos modelo y features según el fluido a predecir
            target_col = 'prod_gas' if target == 'gas' else 'prod_pet'

            if target == "gas": # Cada target usa sus propios features de ventana porque gas y petróleo no siempre están correlacionados en pozos no convencionales
                feature_cols = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                                'avg_prod_gas_10m', 'last_prod_gas', 'n_readings']
                model = self.model_gas
            else:
                feature_cols = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                                'avg_prod_pet_10m', 'last_prod_pet', 'n_readings']
                model = self.model_pet

            # Generamos el rango de fechas (primer día de cada mes)
            dates = pd.date_range(start = date_start, end = date_end, freq = 'MS')
            if len(dates) == 0:
                raise HTTPException(status_code = 400, detail = "date_start debe ser anterior a date_end")

            # Cargamos el parquet filtrado por el pozo e indexado por fecha
            df = pd.read_parquet(PARQUET_PATH)
            df['fecha'] = pd.to_datetime(df['fecha'])
            well_df = df[df['idpozo'] == int(id_well)].set_index('fecha') # Filtramos pozo e index de fecha

            if well_df.empty:
                raise HTTPException(status_code = 404, detail = f"No se encontraron datos para el pozo {id_well}")

            min_date = well_df.index.min()
            max_date = well_df.index.max()

            # Validación temporal: no permitir fechas anteriores al histórico
            if any(date < min_date for date in dates):
                raise HTTPException(
                    status_code = 400,
                    detail = f"No se permiten predicciones anteriores al histórico disponible ({min_date.date()} - período de entrenamiento)"
                )

            # Se inicializa lazy: el online store solo se consulta si hay fechas futuras en el rango
            online_X = None

            results = [] # Por cada mes del rango hacemos results.append(...) con la fecha y la predicción

            for date in dates: # Iteramos todos los meses del input

                # Es histórica solo si está en el parquet Y el target no es nulo
                is_historical = (
                    date in well_df.index and
                    pd.notna(well_df.loc[date, target_col]) # La fila futura que agrega el DAG tiene target = None — se trata como futura
                )

                if is_historical:
                    # Usamos los features reales de esa fecha del offline store
                    X = well_df.loc[[date], feature_cols] # avg_prod_10m en T = promedio de T-10 a T-1 (shift(1) — sin leakage)
                else:
                    # Usamos el online store: una fila por pozo con el estado más reciente
                    # Se reutiliza para todos los meses futuros del rango (lazy load)
                    if online_X is None:
                        online_features = self.store.get_online_features(
                            features=[
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
                            entity_rows = [{"idpozo": int(id_well)}]
                        ).to_df()

                        if online_features.isnull().all(axis = 1).any():
                            raise HTTPException(status_code = 404, detail = f"No se encontraron features para el pozo {id_well}")

                        online_X = online_features[feature_cols]

                    X = online_X

                # La producción no puede ser negativa, aplicamos floor en 0
                pred = max(0.0, float(model.predict(X)[0]))
                results.append({"date": date.strftime("%Y-%m-%d"), "prod": round(pred, 2)})

            return {
                "id_well": id_well,
                "data": results
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code = 500, detail = str(e))


# Entry point para `serve run api.main:app_deployment`
app_deployment = APIDeployment.bind()
