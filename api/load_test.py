"""
Load test del endpoint /api/v1/forecast servido con Ray Serve.

Envía `RATE` requests por segundo durante `TEST_DURATION_S` segundos y reporta
throughput, distribución de latencia (avg, p50, p95, p99, max), y tasa de error
(5xx + excepciones). La medición de latencia se hace solo sobre requests exitosos (200).

Decisiones de diseño del script:

    - Variables de entorno para configurar RATE, TEST_DURATION_S, URL, etc.:
      permite correr distintos escenarios (valle, pico, pico sostenido) sin editar el archivo.
    - Seed fija de `random`: los pozos y targets elegidos son los mismos en cada corrida
      con la misma configuración, de forma que dos ejecuciones del test son directamente
      comparables (la variabilidad restante viene del sistema, no del test).
    - Pozos e inputs tomados del parquet del feature store: cada request ejerce inferencia
      real contra modelos y online store, no un payload sintético.
    - Validación temprana del rango de fechas: si el parquet actual no tiene datos en
      `[DATE_START, DATE_END]`, el script aborta antes de disparar requests y explica cómo
      resolver (en vez de reportar 404 masivo).

Uso:
    python api/load_test.py

Configuración por variables de entorno (opcionales):
    RATE=50                    # requests por segundo
    TEST_DURATION_S=5          # duración del test en segundos
    URL=http://localhost:8000/api/v1/forecast
    DATE_START=2019-07-01
    DATE_END=2019-09-01
    PARQUET_PATH=feature_store/data/well_features.parquet
"""
import asyncio
import os
import random
import statistics
import time
from collections import Counter

import aiohttp
import numpy as np
import pandas as pd

RATE = int(os.getenv("RATE", "50"))
TEST_DURATION_S = int(os.getenv("TEST_DURATION_S", "5"))
REQUESTS = int(RATE * TEST_DURATION_S)
URL = os.getenv("URL", "http://localhost:8000/api/v1/forecast")
DATE_START = os.getenv("DATE_START", "2019-07-01")
DATE_END = os.getenv("DATE_END", "2019-09-01")
PARQUET_PATH = os.getenv("PARQUET_PATH", "feature_store/data/well_features.parquet")

# Seed fija: la misma corrida sobre el mismo parquet produce inputs idénticos request a request
random.seed(204)

# Cargamos pozos disponibles desde el parquet actual
df = pd.read_parquet(PARQUET_PATH)
WELL_IDS = df["idpozo"].unique().tolist()
TARGETS = ["gas", "pet"]

# Validación temprana: si el parquet no cubre [DATE_START, DATE_END], el test daría 404 masivo
fechas = pd.to_datetime(df["fecha"])
if not ((fechas >= DATE_START) & (fechas <= DATE_END)).any():
    raise SystemExit(
        f"El parquet actual no tiene datos entre {DATE_START} y {DATE_END}.\n"
        f"Rango disponible en el parquet: {fechas.min().date()} → {fechas.max().date()}.\n"
        f"Re-ejecutar el DAG con un rango compatible, o pasar DATE_START / DATE_END por env var."
    )


def params():
    return {
        "id_well": str(random.choice(WELL_IDS)),
        "date_start": DATE_START,
        "date_end": DATE_END,
        "target": random.choice(TARGETS),
    }


async def send(session, ok, fail):
    t0 = time.perf_counter()
    try:
        async with session.get(URL, params = params()) as r:
            await r.read()
            ok.append((r.status, (time.perf_counter() - t0) * 1000))
    except Exception:
        fail.append((time.perf_counter() - t0) * 1000)


async def main():
    ok, fail = [], []
    t0 = time.perf_counter()
    async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(limit = 500)) as s:
        tasks = []
        for _ in range(REQUESTS):
            tasks.append(asyncio.create_task(send(s, ok, fail)))
            await asyncio.sleep(1 / RATE)
        await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0

    # Latencias solo para requests exitosos (200)
    lat_ok = [ms for status, ms in ok if status == 200]
    codes = Counter(c for c, _ in ok)

    print("=" * 55)
    print(f"Enviando {RATE} req/s por {TEST_DURATION_S} segundos.")
    print("=" * 55)
    print(f"Throughput (exitosos): {len(lat_ok) / elapsed:.1f} req/s")
    if lat_ok:
        print(f"avg={statistics.mean(lat_ok):.0f}ms")
        print(f"p50={np.percentile(lat_ok, 50):.0f}ms")
        print(f"p95={np.percentile(lat_ok, 95):.0f}ms")
        print(f"p99={np.percentile(lat_ok, 99):.0f}ms")
        print(f"max={max(lat_ok):.0f}ms")
    else:
        print("-> (Ningún request exitoso para medir latencia)")

    errors_5xx = sum(v for k, v in codes.items() if int(k) >= 500)
    error_pct = 100 * (len(fail) + errors_5xx) / REQUESTS if REQUESTS else 0
    print(f"HTTP: {dict(codes)}  ( error rate: {error_pct:.1f}% )")


if __name__ == "__main__":
    asyncio.run(main())
