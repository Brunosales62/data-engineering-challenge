from pathlib import Path
import shutil

BASE_DIR = Path(__file__).resolve().parent.parent

def extract():

    origem = BASE_DIR / "data" / "source"
    destino = BASE_DIR / "data" / "raw"

    destino.mkdir(parents=True, exist_ok=True)

    for arquivo in origem.glob("*.parquet.gz"):
        shutil.copy2(
            arquivo,
            destino / arquivo.name
        )

    print("Arquivos copiados")