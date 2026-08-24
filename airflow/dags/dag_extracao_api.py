import json
from io import BytesIO

import pandas as pd
from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage
from docker.types import Mount


BUCKET = "data-lake-estudo"
GCP_CONN_ID = "google_cloud_default"

# Caminhos usados pelo Docker daemon do Docker Desktop ao criar o container da task.
DOCKER_SRC_DIR = "/run/desktop/mnt/host/c/Users/pcnot/Documents/vscode/estudos/astronomer_estudo/src/extracao_api"
DOCKER_KEYS_DIR = "/run/desktop/mnt/host/c/Users/pcnot/Documents/vscode/estudos/astronomer_estudo/airflow/keys"


def transformar_json_gcs_para_parquet_gcs(ds: str) -> None:
    raw_object = f"raw/gupy/jobs/dt={ds}/resultado_api.json"
    parquet_object = f"staging/gupy/jobs/dt={ds}/resultado_api.parquet"

    credentials = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID).get_credentials()
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET)

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

    print(f"Parquet enviado para gs://{BUCKET}/{parquet_object}")
    print(f"Total de vagas transformadas: {len(dataframe)}")

# Antes, a DAG lia os arquivos a partir de:
# OUTPUT_DIR = "/usr/local/airflow/include/extracao_api/output"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="5 9 * * *",
    catchup=False,
)
def extracao_api():

    extrair = DockerOperator(
        task_id="extrair_api_docker",
        image="extracao-api:latest",
        command="python /src/main.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        auto_remove="success",
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/keys/airflow-projeto-4cddd5ef2c66.json",
            "GCS_BUCKET": BUCKET,
            "GCS_RAW_OBJECT": "raw/gupy/jobs/dt={{ ds }}/resultado_api.json",
        },
        mounts=[
            Mount(
                source=DOCKER_SRC_DIR,
                target="/src",
                type="bind",
            ),
            Mount(
                source=DOCKER_KEYS_DIR,
                target="/keys",
                type="bind",
            )
        ],
    )

    # Antes, o container executava a extracao e a transformacao dentro da imagem:
    #
    # extrair_transformar = DockerOperator(
    #     task_id="extrair_transformar_docker",
    #     image="extracao-api:latest",
    #     command="sh -c 'python main.py && python transform_to_parquet.py'",
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="bridge",
    #     mount_tmp_dir=False,
    #     auto_remove="success",
    #     environment={
    #         "EXTRACAO_API_OUTPUT_DIR": "/data",
    #     },
    #     mounts=[
    #         Mount(
    #             source="extracao_api_data",
    #             target="/data",
    #             type="volume",
    #         )
    #     ],
    # )

    transformar_para_parquet = PythonOperator(
        task_id="transformar_json_gcs_para_parquet_gcs",
        python_callable=transformar_json_gcs_para_parquet_gcs,
    )

    # Antes, a DAG subia o JSON bruto gerado pelo main.py:
    #
    # upload_raw_gcs = LocalFilesystemToGCSOperator(
    #     task_id="upload_raw_resultado_api_gcs",
    #     src=f"{AIRFLOW_DATA_DIR}/resultado_api_docker.json",
    #     dst="raw/gupy/jobs/dt={{ ds }}/resultado_api_docker.json",
    #     bucket="data-lake-estudo",
    #     gcp_conn_id="google_cloud_default",
    #     mime_type="application/json",
    # )

    # Antes, a DAG tambem subia o parquet gerado pelo transform_to_parquet.py:
    #
    # upload_staging_gcs = LocalFilesystemToGCSOperator(
    #     task_id="upload_staging_resultado_api_gcs",
    #     src=f"{OUTPUT_DIR}/resultado_api.parquet",
    #     dst="staging/gupy/jobs/dt={{ ds }}/resultado_api.parquet",
    #     bucket="data-lake-estudo",
    #     gcp_conn_id="google_cloud_default",
    #     mime_type="application/octet-stream",
    # )

    # load_staging_to_bigquery = GCSToBigQueryOperator(
    #     task_id="load_staging_parquet_to_bigquery",
    #     bucket="data-lake-estudo",
    #     source_objects=[
    #         "staging/gupy/jobs/dt={{ ds }}/resultado_api.parquet"
    #     ],
    #     destination_project_dataset_table="airflow-projeto.bronze.extracao_api",
    #     source_format="PARQUET",
    #     write_disposition="WRITE_TRUNCATE",
    #     autodetect=True,
    #     gcp_conn_id="google_cloud_default",
    # )

    extrair >> transformar_para_parquet

    # Cadeia anterior:
    # extrair_transformar >> upload_raw_gcs >> upload_staging_gcs
    # >> load_staging_to_bigquery

    # run_script()

dag = extracao_api()
