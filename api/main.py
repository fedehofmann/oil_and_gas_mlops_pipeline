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
    Devuelve el pronóstico de producción de un pozo para cada mes entre date_start y date_end.

    Estrategia de inferencia según el tipo de fecha:
    - Fechas dentro del parquet (históricas): se usan los features reales del offline store.
      avg_prod_10m fue calculado con shift(1) en prepare_offline_store, así que el valor
      en la fila de fecha T contiene producción de T-10 a T-1 (sin leakage).
    - Fechas futuras (posteriores al último dato del parquet): se usan los features del online
      store (estado más reciente del pozo). El modelo predice un solo paso; se repite la misma
      predicción para todos los meses futuros del rango. No se hace actualización autoregresiva
      porque alimentar el modelo con sus propias predicciones cambia la distribución de
      avg_prod_10m respecto al entrenamiento (distribution shift).

    Args:
        id_well (str) - identificador del pozo.
        date_start (str) - fecha de inicio en formato YYYY-MM-DD.
        date_end (str) - fecha de fin en formato YYYY-MM-DD.
        target (str) - tipo de producción a predecir: 'gas' (default) o 'pet'.
    """
    try:
        if target not in ("gas", "pet"):
            raise HTTPException(status_code = 400, detail = "target debe ser 'gas' o 'pet'")

        # Columnas de features y modelo según el target
        # Cada target usa sus propios features de ventana para evitar ruido entre variables
        # que no siempre están correlacionadas (un pozo puede ser gasífero o petrolífero)
        if target == "gas":
            feature_cols = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                            'avg_prod_gas_10m', 'last_prod_gas', 'n_readings']
            model = mlflow.sklearn.load_model("models:/oil_gas_prod_gas@production")
        else:
            feature_cols = ['tipoextraccion', 'tef', 'profundidad', 'prod_agua',
                            'avg_prod_pet_10m', 'last_prod_pet', 'n_readings']
            model = mlflow.sklearn.load_model("models:/oil_gas_prod_pet@production")

        # Rango mensual (primer día de cada mes)
        dates = pd.date_range(start = date_start, end = date_end, freq = 'MS')
        if len(dates) == 0:
            raise HTTPException(status_code = 400, detail = "date_start debe ser anterior a date_end")

        # Cargamos el offline store indexado por fecha para el pozo dado
        df = pd.read_parquet(PARQUET_PATH)
        df['fecha'] = pd.to_datetime(df['fecha'])
        well_df = df[df['idpozo'] == int(id_well)].set_index('fecha')

        if well_df.empty:
            raise HTTPException(status_code = 404, detail = f"No se encontraron datos para el pozo {id_well}")

        # Features del online store: se cargan solo si el rango incluye fechas futuras
        online_X = None

        results = []
        for date in dates:
            if date in well_df.index:
                # Fecha histórica: features reales del offline store.
                # avg_prod_10m en la fila T = media de prod de T-10 a T-1 (shift(1) aplicado
                # en prepare_offline_store). No hay leakage.
                X = well_df.loc[[date], feature_cols]
            else:
                # Fecha futura: features del online store (estado más reciente del pozo).
                # Se reutiliza online_X para todos los meses futuros del rango.
                if online_X is None:
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

                    if online_features.isnull().all(axis = 1).any():
                        raise HTTPException(status_code = 404, detail = f"No se encontraron features para el pozo {id_well}")

                    online_X = online_features[feature_cols]

                X = online_X

            pred = max(0.0, float(model.predict(X)[0]))  # floor en 0: producción no puede ser negativa
            results.append({"date": date.strftime("%Y-%m-%d"), "prod": round(pred, 2)})

        return {
            "id_well": id_well,
            "data": results
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code = 500, detail = str(e))
