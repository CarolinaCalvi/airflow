from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import subprocess

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="5 9 * * *",
    catchup=False,
)
def extracao_api():

    @task
    def run_script():
        subprocess.run(
            ["python", "/usr/local/airflow/include/extracao_api/main.py"],
            check=True
        )

    upload_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_resultado_api_gcs",
        src="/usr/local/airflow/include/extracao_api/resultado_api.json",
        dst="raw/gupy/jobs/dt={{ ds }}/resultado_api.json",
        bucket="data-lake-estudo",
        gcp_conn_id="google_cloud_default",
        mime_type="application/json",
    )

    run_script() >> upload_gcs

    # run_script()

dag = extracao_api()
