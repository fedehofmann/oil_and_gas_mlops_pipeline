from fastapi import FastAPI, HTTPException
from feast import FeatureStore
import mlflow.sklearn
import pandas as pd
import os

# -------------------- CONFIGURACIÓN --------------------

# Paths al feature store y al parquet histórico dentro del contenedor Docker
FEATURE_STORE_REPO = '/opt/airflow/feature_store'
PARQUET_PATH = os.path.join(FEATURE_STORE_REPO, 'data/well_features.parquet')

app = FastAPI(
    title = "Oil & Gas Forecast API",
    version = "1.0.0",
    description = "API simple para consultar el listado de pozos y sus pronósticos de producción."
)

# -------------------- ENDPOINTS --------------------
@app.get("/api/v1/wells")
def get_wells(date_query: str):
    """
    Devuelve el listado de pozos que tienen registros para la fecha dada.
    Args: date_query (str) - fecha en formato YYYY-MM-DD.
    """
    try:
        df = pd.read_parquet(PARQUET_PATH)
        df['fecha'] = pd.to_datetime(df['fecha'])
        date = pd.to_datetime(date_query)

        # Filtramos por fecha exacta y devolvemos los IDs únicos
        pozos = df[df['fecha'] == date]['idpozo'].unique().tolist()

        if not pozos:
            raise HTTPException(status_code = 404, detail = f"No se encontraron pozos para la fecha {date_query}")

        return [{"id_well": str(p)} for p in pozos]

    except HTTPException:
        raise # Re-lanzamos para que FastAPI devuelva el código HTTP correcto
    except Exception as e:
        raise HTTPException(status_code = 500, detail = str(e))


@app.get("/api/v1/forecast")
def get_forecast(id_well: str, date_start: str, date_end: str, target: str = "gas"):
    """
    Devuelve el pronóstico de producción de un pozo dado.
    Para la entrega parcial se devuelve el próximo mes disponible
    independientemente del rango solicitado.
    Args:
        id_well (str) - identificador del pozo.
        date_start (str) - fecha de inicio en formato YYYY-MM-DD.
        date_end (str) - fecha de fin en formato YYYY-MM-DD.
        target (str) - tipo de producción a predecir: 'gas' (default) o 'pet'.
    """
    try:
        if target not in ("gas", "pet"):
            raise HTTPException(status_code = 400, detail = "target debe ser 'gas' o 'pet'")

        # Obtenemos los features más recientes del pozo desde el online store
        store = FeatureStore(repo_path = FEATURE_STORE_REPO)
        online_features = store.get_online_features(
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

        # Verificamos que el pozo existe en el online store
        if online_features.isnull().all(axis = 1).any():
            raise HTTPException(status_code = 404, detail = f"No se encontraron features para el pozo {id_well}")

        # Seleccionamos features y modelo según el target
        # Cada target usa sus propios features de ventana para evitar introducir ruido entre variables que no siempre están correlacionadas
        if target == "gas":
            X = online_features[[
                'tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                'avg_prod_gas_10m', 'last_prod_gas', 'n_readings'
            ]]
            model = mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production") # alias "production"
            # Este alias fue asignado por select_best_model al modelo con mejor r2 entre los 5 experimentos entrenados para gas
        else:
            X = online_features[[
                'tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                'avg_prod_pet_10m', 'last_prod_pet', 'n_readings'
            ]]
            model = mlflow.sklearn.load_model("models:/oil_gas_prod_pet@production") # alias "production"
            # Este alias fue asignado por select_best_model al modelo con mejor r2 entre los 5 experimentos entrenados para gas

        # Predecimos
        prediction = float(model.predict(X)[0])

        # Calculamos la fecha del próximo mes disponible
        df = pd.read_parquet(PARQUET_PATH)
        df['fecha'] = pd.to_datetime(df['fecha'])
        last_date = df[df['idpozo'] == int(id_well)]['fecha'].max()
        next_date = last_date + pd.DateOffset(months = 1)

        return {
            "id_well": id_well,
            "target": target,
            "data": [
                {
                    "date": next_date.strftime("%Y-%m-%d"),
                    "prod": prediction
                }
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code = 500, detail = str(e))