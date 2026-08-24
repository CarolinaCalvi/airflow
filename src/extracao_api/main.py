import json
import math
import os

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


def extrair_vagas() -> list[dict]:
    primeira_pagina = buscar_pagina()
    pagination = primeira_pagina.get("pagination", {})

    total = pagination.get("total", 0)
    limit = pagination.get("limit", 0)

    if not limit:
        return primeira_pagina.get("data", [])

    total_paginas = math.ceil(total / limit)
    vagas = []

    for offset_atual in range(0, total_paginas):
        pagina = buscar_pagina(offset_atual)
        vagas.extend(pagina.get("data", []))

    return vagas


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


def salvar_json_bruto_no_gcs() -> None:
    bucket_name = os.environ["GCS_BUCKET"]
    object_name = os.environ["GCS_RAW_OBJECT"]

    paginas = extrair_paginas()
    conteudo = json.dumps(paginas, ensure_ascii=False, indent=4)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(conteudo, content_type="application/json")

    total_vagas = sum(len(pagina.get("data", [])) for pagina in paginas)
    print(f"JSON bruto enviado para gs://{bucket_name}/{object_name}")
    print(f"Total de paginas extraidas: {len(paginas)}")
    print(f"Total de vagas extraidas: {total_vagas}")


if __name__ == "__main__":
    salvar_json_bruto_no_gcs()
