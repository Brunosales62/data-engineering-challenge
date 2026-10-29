"""
Pipeline de transformação dos dados NYC Taxi.

Fluxo:
Raw -> Trusted

Regras aplicadas:
- Remove duplicidades
- Converte datas
- Calcula duração da corrida
- Cria colunas derivadas para análise
- Remove registros inválidos
- Salva um arquivo transformado para cada arquivo de entrada
"""

from pathlib import Path

import pandas as pd


# Diretórios
RAW_PATH = Path(r"C:\airflow\challenge\data\raw")
TRUSTED_PATH = Path(r"C:\airflow\challenge\data\trusted")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as transformações de negócio.
    """

    # Remover registros duplicados
    df = df.drop_duplicates()

    # Garantir tipo datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"]
    )

    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"]
    )

    # Duração da corrida em minutos
    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Colunas derivadas para análise
    df["pickup_date"] = (
        df["tpep_pickup_datetime"].dt.date
    )

    df["pickup_hour"] = (
        df["tpep_pickup_datetime"].dt.hour
    )

    df["pickup_month"] = (
        df["tpep_pickup_datetime"].dt.month
    )

    df["pickup_day_of_week"] = (
        df["tpep_pickup_datetime"].dt.day_name()
    )

    # Receita da corrida
    df["trip_revenue"] = (
        df["fare_amount"]
        + df["tip_amount"]
        + df["tolls_amount"]
    )

    # Velocidade média (milhas por hora)
    df["average_speed_mph"] = (
        df["trip_distance"]
        / (df["trip_duration_minutes"] / 60)
    )

    # Remover registros inconsistentes
    df = df[
        (df["trip_distance"] > 0)
        & (df["fare_amount"] >= 0)
        & (df["trip_duration_minutes"] > 0)
    ].copy()

    return df


def process_file(file_path: Path) -> None:
    """
    Processa um único arquivo.
    """

    print(f"Processando: {file_path.name}")

    df = pd.read_parquet(file_path)

    rows_before = len(df)

    df = transform_data(df)

    rows_after = len(df)

    output_file = (
        TRUSTED_PATH
        / file_path.name.replace(".gz", "")
    )

    TRUSTED_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Linhas: {rows_before:,} -> {rows_after:,}"
    )
    print(
        f"Arquivo salvo: {output_file.name}"
    )
    print("-" * 50)


def main() -> None:
    """
    Processa todos os arquivos da camada raw.
    """

    files = sorted(
        RAW_PATH.glob("*.parquet.gz")
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em: {RAW_PATH}"
        )

    print(
        f"Arquivos encontrados: {len(files)}"
    )

    for file in files:
        process_file(file)

    print("Processamento concluído.")


if __name__ == "__main__":
    main()