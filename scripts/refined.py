from pathlib import Path

import pandas as pd

TRUSTED_PATH = Path(
    r"C:\airflow\challenge\data\trusted"
)

REFINED_PATH = Path(
    r"C:\airflow\challenge\data\refined"
)

REFINED_PATH.mkdir(
    parents=True,
    exist_ok=True
)

# =========================
# Variáveis auxiliares
# =========================

total_records = 0
total_trips_17_june = 0

max_distance = -1
max_distance_row = None

distances = []

target_date = pd.Timestamp(
    "2022-06-17"
).date()

# =========================
# Processamento
# =========================

for file in sorted(
    TRUSTED_PATH.glob("*.parquet")
):

    print(
        f"Processando {file.name}"
    )

    df = pd.read_parquet(file)

    # Questão 1
    total_records += len(df)

    # Questão 2
    filtro = (
        (
            df["tpep_pickup_datetime"]
            .dt.date
            == target_date
        )
        &
        (
            df["tpep_dropoff_datetime"]
            .dt.date
            == target_date
        )
    )

    total_trips_17_june += filtro.sum()

    # Questão 3
    current_idx = df[
        "trip_distance"
    ].idxmax()

    current_distance = df.loc[
        current_idx,
        "trip_distance"
    ]

    if current_distance > max_distance:

        max_distance = current_distance

        max_distance_row = (
            df.loc[[current_idx]]
            .copy()
        )

    # Questão 4
    distances.append(
        df["trip_distance"]
    )

# =========================
# Q1
# =========================

q1 = pd.DataFrame(
    {
        "total_records": [
            total_records
        ]
    }
)

q1.to_parquet(
    REFINED_PATH /
    "question_1_total_records.parquet",
    index=False
)

# =========================
# Q2
# =========================

q2 = pd.DataFrame(
    {
        "date": [
            "2022-06-17"
        ],
        "total_trips": [
            total_trips_17_june
        ]
    }
)

q2.to_parquet(
    REFINED_PATH /
    "question_2_trips_17_june.parquet",
    index=False
)

# =========================
# Q3
# =========================

max_distance_row.to_parquet(
    REFINED_PATH /
    "question_3_longest_trip.parquet",
    index=False
)

# =========================
# Q4
# =========================

print(
    "Calculando estatísticas..."
)

distance_series = pd.concat(
    distances,
    ignore_index=True
)

q4 = pd.DataFrame(
    {
        "metric": [
            "mean",
            "std",
            "min",
            "q1",
            "median",
            "q3",
            "max"
        ],
        "value": [
            distance_series.mean(),
            distance_series.std(),
            distance_series.min(),
            distance_series.quantile(
                0.25
            ),
            distance_series.quantile(
                0.50
            ),
            distance_series.quantile(
                0.75
            ),
            distance_series.max()
        ]
    }
)

q4.to_parquet(
    REFINED_PATH /
    "question_4_trip_distance_stats.parquet",
    index=False
)

print(
    "Camada Refined criada com sucesso."
)