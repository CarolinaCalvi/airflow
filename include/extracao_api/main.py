import requests
import json
import math

url = "https://employability-portal.gupy.io/api/v1/jobs?"

params = {"jobName": "dados"}

def extrair_dados():
    response = requests.get(url, params = params)

    data = response.json()

    # print("data:", data)

    pagination = data.get("pagination", {})

    total = pagination.get("total", 0)
    limit = pagination.get("limit", 0)
    offset = pagination.get("offset", 0)

    total_posicoes = total / limit
    total_offset = math.ceil(total_posicoes)

    for offset_atual in range(0, total_offset):
        params["offset"] = offset_atual

        response = requests.get(url, params=params)
        data_offset = response.json()

        yield data_offset



with open("/usr/local/airflow/include/extracao_api/resultado_api.json", "w", encoding="utf-8") as arquivo:
    arquivo.write("[\n")

    for i, data_offset in enumerate(extrair_dados()):
        if i > 0:
            arquivo.write(",\n")

        json.dump(data_offset, arquivo, ensure_ascii=False, indent=4)

    arquivo.write("\n]")