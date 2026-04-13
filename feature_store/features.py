from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32

# Entidad: el "sujeto" de los features. 
# Feast necesita saber quién es el protagonista para poder buscar features por ID. 
# En este caso, cada pozo es una entidad.

pozo = Entity(
  name = "idpozo",
  description = "Identificador único del pozo de extracción",
)

# Fuente de datos (offline store): le dice a Feast dónde está el parquet con los features históricos. 
# timestamp_field le indica qué columna usar para el point-in-time lookup (buscar los features de un pozo tal como eran en una fecha específica).

well_stats_source = FileSource(
    path = "/opt/airflow/feature_store/data/well_features.parquet",
    timestamp_field = "fecha",
)

# Feature View: une todo. Define qué features están disponibles para cada pozo, desde qué fuente leerlos, y con qué esquema (nombre y tipo de cada columna).
# Los nombres tienen que coincidir exactamente con las columnas del parquet.

well_stats = FeatureView(
  name = "well_stats",
  entities = [pozo],
  schema = [
    # Features del dataset original
    Field(name = "prod_gas", dtype = Float32), # Target
    Field(name = "prod_pet", dtype = Float32), # Target
    Field(name = "prod_agua", dtype = Float32),
    Field(name = "tef", dtype = Float32),
    Field(name = "profundidad", dtype = Float32),
    Field(name = "tipoextraccion", dtype = Int32),

    # Features de ventana (últimas 10 lecturas por pozo)
    Field(name = "avg_prod_gas_10m", dtype = Float32),
    Field(name = "avg_prod_pet_10m", dtype = Float32),
    Field(name = "last_prod_gas", dtype = Float32),
    Field(name = "last_prod_pet", dtype = Float32),
    Field(name = "n_readings", dtype = Int32),
  ],
  source = well_stats_source,
) 