from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


BUCKET = "data-lake-estudo"

DOCKER_SRC_DIR = "/run/desktop/mnt/host/c/Users/pcnot/Documents/vscode/estudos/astronomer_estudo/src/extracao_api"
DOCKER_KEYS_DIR = "/run/desktop/mnt/host/c/Users/pcnot/Documents/vscode/estudos/astronomer_estudo/airflow/keys"


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
            "GCS_PARQUET_OBJECT": "staging/gupy/jobs/dt={{ ds }}/resultado_api.parquet",
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

dag = extracao_api()
