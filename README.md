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

Executa diariamente o script `include/extracao_api/main.py`.

O script consulta a API publica da Gupy procurando vagas relacionadas a `dados`, pagina os resultados e grava a resposta no arquivo resultado_api.json

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

## Executando o script manualmente

Tambem e possivel rodar o script de extracao diretamente com Python:

```bash
python include/extracao_api/main.py
```

Ao final da execucao, o arquivo `resultado_api.json` sera sobrescrito com os dados retornados pela API.
