# airflow

Projeto de estudo com Astronomer/Airflow para criar, executar e testar DAGs localmente.

O ambiente segue a estrutura padrao de um projeto criado com Astro CLI.

## Objetivo

Este repositorio reune exemplos simples de DAGs e uma primeira rotina de extracao de dados via API.

## Estrutura do projeto

```text
.
|-- dags/
|   |-- dag_extracao_api.py      # DAG que executa o script de extracao da API
|   |-- dag_hello_world.py       # DAG simples de Hello World
|   `-- exampledag.py            # DAG exemplo do Astronomer
|-- include/
|   `-- extracao_api/
|       `-- main.py              # Script Python que consulta a API da Gupy
|       `-- resultado_api.json   # Arquivo gerado com o resultado da extracao
|-- plugins/                     # Plugins customizados do Airflow
|-- tests/                       # Testes com pytest para validacao das DAGs
|-- airflow_settings.yaml        # Connections, pools e variables para uso local
|-- Dockerfile                   # Imagem base do Astro Runtime
|-- packages.txt                 # Pacotes de sistema adicionais
`-- requirements.txt             # Dependencias Python adicionais
```

## DAGs criadas

### `hello_world`

DAG simples para validar a criacao de uma tarefa Python no Airflow.

### `extracao_api`

Executa diariamente uma imagem Docker propria para extrair dados da API e transformar o resultado em Parquet.

O container consulta a API publica da Gupy procurando vagas relacionadas a `dados`, pagina os resultados e grava a resposta no arquivo `resultado_api.json`.
Depois, transforma os dados em `resultado_api.parquet`.

Os arquivos gerados ficam em um volume Docker nomeado, compartilhado entre o container executado pelo `DockerOperator` e o container do Airflow Scheduler.
Depois disso, a DAG envia o JSON e o Parquet para o Google Cloud Storage.

## Como executar localmente

Pre-requisitos:

- Docker;
- Astro CLI instalado.

Entre na pasta do projeto:

```bash
cd airflow
```

Suba o ambiente local:

```bash
astro dev start
```

Crie a imagem Docker usada pelo `DockerOperator`:

```bash
docker build -t extracao-api:latest include/extracao_api
```

Depois que os containers iniciarem, acesse a interface do Airflow em:

```text
http://localhost:8080
```

Credenciais padrao do ambiente local:

```text
Usuario: admin
Senha: admin
```

Para parar o ambiente:

```bash
astro dev stop
```

Se alterar o `docker-compose.override.yml`, reinicie o ambiente:

```bash
astro dev restart
```

## Executando o script manualmente

Tambem e possivel rodar o script de extracao diretamente com Python:

```bash
python include/extracao_api/main.py
```

Ao final da execucao, o arquivo `resultado_api.json` sera sobrescrito com os dados retornados pela API.
