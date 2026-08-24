from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="5 9 * * *",
    catchup=False
)
def hello_world():

    @task
    def hello():
        print("Hello World 🚀")

    hello()

hello_world()