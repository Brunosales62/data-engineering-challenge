from pathlib import Path
import requests

BASE_DIR = Path(__file__).resolve().parent.parent


def source():

    destino = BASE_DIR / "data" / "source"

    destino.mkdir(
        parents=True,
        exist_ok=True
    )

    for mes in range(1, 13):

        arquivo = f"yellow_tripdata_2022-{mes:02d}.parquet.gz"

        caminho_arquivo = destino / arquivo

        # Se já existe, não baixa de novo
        if caminho_arquivo.exists():
            print(f"Já existe: {arquivo}")
            continue

        url = (
            "https://raw.githubusercontent.com/"
            "datarisk-io/data-engineering-challenge/master/"
            f"nyc-tlc-data/{arquivo}"
        )

        print(f"Baixando: {arquivo}")

        response = requests.get(url)

        if response.status_code == 200:

            with open(caminho_arquivo, "wb") as f:
                f.write(response.content)

            print(f"Concluído: {arquivo}")

        else:

            print(
                f"Erro ao baixar {arquivo} "
                f"(status {response.status_code})"
            )


if __name__ == "__main__":
    source()