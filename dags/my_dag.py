from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime

def pick_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'train_model_A',
        'train_model_B',
        'train_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


def train_model():
    return randint(0, 100)

with DAG("my_dag", start_date=datetime(2023, 1, 1),
    schedule_interval="@once", catchup=False) as dag:

        train_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=train_model
        )

        train_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=train_model
        )

        train_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=train_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=pick_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
        )

        [train_model_A, train_model_B, train_model_C] >> choose_best_model >> [accurate, inaccurate]