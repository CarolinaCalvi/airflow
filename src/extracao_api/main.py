import json
import math
import os
from io import BytesIO

import pandas as pd
import requests
from google.cloud import storage

URL = "https://employability-portal.gupy.io/api/v1/jobs?"
PARAMS = {"jobName": "dados"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}


def buscar_pagina(offset: int | None = None) -> dict:
    params = PARAMS.copy()
    if offset is not None:
        params["offset"] = offset

    response = requests.get(URL, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def extrair_paginas() -> list[dict]:
    primeira_pagina = buscar_pagina()
    pagination = primeira_pagina.get("pagination", {})

    total = pagination.get("total", 0)
    limit = pagination.get("limit", 0)

    if not limit:
        return [primeira_pagina]

    total_paginas = math.ceil(total / limit)
    paginas = []

    for offset_atual in range(0, total_paginas):
        paginas.append(buscar_pagina(offset_atual))

    return paginas


def salvar_json_bruto_no_gcs(client: storage.Client, paginas: list[dict]) -> None:
    bucket_name = os.environ["GCS_BUCKET"]
    raw_object = os.environ["GCS_RAW_OBJECT"]
    conteudo = json.dumps(paginas, ensure_ascii=False, indent=4)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(raw_object)
    blob.upload_from_string(conteudo, content_type="application/json")

    total_vagas = sum(len(pagina.get("data", [])) for pagina in paginas)
    print(f"JSON bruto enviado para gs://{bucket_name}/{raw_object}")
    print(f"Total de paginas extraidas: {len(paginas)}")
    print(f"Total de vagas extraidas: {total_vagas}")


def transformar_json_gcs_para_parquet_gcs(client: storage.Client) -> None:
    bucket_name = os.environ["GCS_BUCKET"]
    raw_object = os.environ["GCS_RAW_OBJECT"]
    parquet_object = os.environ["GCS_PARQUET_OBJECT"]

    bucket = client.bucket(bucket_name)
    raw_blob = bucket.blob(raw_object)
    paginas = json.loads(raw_blob.download_as_bytes().decode("utf-8"))

    vagas = []
    for pagina in paginas:
        vagas.extend(pagina.get("data", []))

    dataframe = pd.json_normalize(vagas)
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_blob = bucket.blob(parquet_object)
    parquet_blob.upload_from_file(
        parquet_buffer,
        content_type="application/octet-stream",
    )

    print(f"Parquet enviado para gs://{bucket_name}/{parquet_object}")
    print(f"Total de vagas transformadas: {len(dataframe)}")


def executar_pipeline() -> None:
    client = storage.Client()
    paginas = extrair_paginas()

    salvar_json_bruto_no_gcs(client, paginas)
    transformar_json_gcs_para_parquet_gcs(client)


if __name__ == "__main__":
    executar_pipeline()
