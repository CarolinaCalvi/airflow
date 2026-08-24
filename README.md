# astronomer_estudo

Projeto de estudo com Astronomer/Airflow para orquestrar uma extracao da API da Gupy usando Docker e Google Cloud Storage.

## Objetivo

A DAG `extracao_api` executa uma imagem Docker propria. O container faz todo o pipeline:

1. consulta a API publica da Gupy buscando vagas relacionadas a `dados`;
2. salva o JSON bruto diretamente no Cloud Storage;
3. le o JSON bruto do Cloud Storage;
4. transforma os dados para Parquet dentro do proprio container;
5. salva o Parquet diretamente no Cloud Storage.

O Airflow apenas orquestra a execucao do container. A transformacao JSON para Parquet nao roda no processo do Airflow.

## Estrutura

```text
.
|-- airflow/
|   |-- dags/
|   |   |-- dag_extracao_api.py       # DAG que executa a imagem Docker
|   |   |-- dag_hello_world.py
|   |   `-- exampledag.py
|   |-- keys/
|   |   `-- airflow-projeto-4cddd5ef2c66.json
|   |-- airflow_settings.yaml         # Conexao google_cloud_default
|   |-- docker-compose.override.yml   # Monta docker.sock e keys no scheduler
|   |-- Dockerfile                    # Imagem base do Astro Runtime
|   |-- packages.txt
|   `-- requirements.txt              # Dependencias do Airflow
|-- src/
|   `-- extracao_api/
|       |-- Dockerfile                # Imagem extracao-api:latest
|       |-- main.py                   # Extrai, salva JSON no GCS, transforma e salva Parquet no GCS
|       `-- requirements.txt          # Dependencias da imagem de extracao
`-- README.md
```

## Fluxo Da DAG

A DAG tem uma task principal:

```python
extrair_api_docker
```

Essa task usa `DockerOperator` para executar a imagem:

```text
extracao-api:latest
```

O comando executado dentro do container e:

```bash
python /src/main.py
```

A DAG passa para o container os caminhos de destino no Cloud Storage:

```text
raw/gupy/jobs/dt={{ ds }}/resultado_api.json
staging/gupy/jobs/dt={{ ds }}/resultado_api.parquet
```

Assim, para uma execucao com `ds=2026-08-24`, os arquivos ficam em:

```text
gs://data-lake-estudo/raw/gupy/jobs/dt=2026-08-24/resultado_api.json
gs://data-lake-estudo/staging/gupy/jobs/dt=2026-08-24/resultado_api.parquet
```

## Como O Docker Acessa O GCS

O container recebe a chave de service account por volume:

```text
airflow/keys -> /keys
```

E usa a variavel:

```text
GOOGLE_APPLICATION_CREDENTIALS=/keys/airflow-projeto-4cddd5ef2c66.json
```

## Como O Airflow Acessa O Docker

O arquivo `airflow/docker-compose.override.yml` monta o socket do Docker no scheduler:

```yaml
services:
  scheduler:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./keys:/usr/local/airflow/keys:ro
```

Isso permite que o `DockerOperator` crie containers usando o Docker da maquina local.

## Executando Localmente

Entre na pasta do projeto Astro:

```bash
cd C:\Users\pcnot\Documents\vscode\estudos\astronomer_estudo\airflow
```

Suba ou reinicie o Airflow:

```bash
astro dev start
```

Se o ambiente ja estiver rodando:

```bash
astro dev restart
```

Acesse:

```text
http://localhost:8080
```

Credenciais padrao:

```text
Usuario: admin
Senha: admin
```

## Build Da Imagem De Extracao

Sempre que alterar `src/extracao_api/main.py`, `Dockerfile` ou `requirements.txt`, reconstrua a imagem:

```bash
docker build -t extracao-api:latest C:\Users\pcnot\Documents\vscode\estudos\astronomer_estudo\src\extracao_api
```

## Teste Manual Da Imagem

Para testar a imagem fora do Airflow:

```bash
docker run --rm ^
  -v "C:\Users\pcnot\Documents\vscode\estudos\astronomer_estudo\src\extracao_api:/src" ^
  -v "C:\Users\pcnot\Documents\vscode\estudos\astronomer_estudo\airflow\keys:/keys:ro" ^
  -e GOOGLE_APPLICATION_CREDENTIALS="/keys/airflow-projeto-4cddd5ef2c66.json" ^
  -e GCS_BUCKET="data-lake-estudo" ^
  -e GCS_RAW_OBJECT="raw/gupy/jobs/dt=manual/resultado_api.json" ^
  -e GCS_PARQUET_OBJECT="staging/gupy/jobs/dt=manual/resultado_api.parquet" ^
  extracao-api:latest ^
  python /src/main.py
```

## Dependencias

Imagem de extracao:

```text
requests
google-cloud-storage
pandas
pyarrow
```

Airflow:

```text
apache-airflow-providers-google
apache-airflow-providers-docker
requests
pandas
pyarrow
```
