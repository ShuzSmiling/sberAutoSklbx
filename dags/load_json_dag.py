import datetime
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/sberAutoSklbx')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.load_files import load_files

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 4, 24),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
    dag_id='load_json_file',
    schedule_interval='50 21 * * *',
    default_args=args,
) as dag:
    # Загружаем JSON файлы в БД
    load_json = PythonOperator(
        task_id='load_json',
        python_callable=load_files
    )

    load_json