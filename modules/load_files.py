from sqlalchemy import create_engine

import pandas as pd
import json
import os

PATH = os.environ.get('PROJECT_PATH', '.')


def list_files(path: str) -> list:
    """
    Принимает на вход путь к папке и возращает два списка с файлами
    :param path:
    :return:
    """
    return (
        [i for i in os.listdir(path) if 'ga_hits_new' in i],
        [i for i in os.listdir(path) if 'ga_sessions_new' in i]
    )


def load(df: pd.DataFrame, table: str) -> None:
    """
    Скрипт подключения к БД и функции загрузки
    :param df:
    :param table:
    :return:
    """
    engine = create_engine(f'mysql+pymysql://airflow:123456@db:3306/user_metrics')
    df.to_sql(
        table,
        engine,
        index=True,
        chunksize=100,
        if_exists='append'
    )


def check_load_files(files: list, table: str)->None:
    """
    Обрабатываем данные и загружаем с помощью функции load
    :param files:
    :param table:
    :return:
    """
    for i in files:
        with open(f'{PATH}/data/test_json/{i}') as file:
            data = json.load(file)

        key = list(data.keys())[0]
        if len(data[key]):
            df = pd.DataFrame.from_dict(data[key])
            load(df, table)


def load_files():
    """
    Объединяем запуск в одну функцию для DAG файла
    :return:
    """
    hits_files, sess_files = list_files(f'{PATH}/data/test_json/')
    table_di = [(hits_files, 'user_hits'), (sess_files, 'user_session')]
    print(f'Начинаем загружать данные из JSON в БД')

    for i in table_di:
        check_load_files(i[0], i[1])


if __name__ == '__main__':
    load_files()
