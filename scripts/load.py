from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

TRUSTED_PATH = Path(
    r"C:\airflow\challenge\data\trusted"
)

import os

engine = create_engine(
    (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
)

for arquivo in TRUSTED_PATH.glob("*.parquet"):

    print(f"Carregando {arquivo.name}")

    df = pd.read_parquet(arquivo)

    df.to_sql(
        "taxi_trips",
        engine,
        if_exists="append",
        index=False,
        chunksize=10000
    )

print("Carga concluída.")