from sqlalchemy import create_engine

import pandas as pd
import os

PATH = os.environ.get('PROJECT_PATH', '.')

def load(data_file, table, database_port):
    """
    Скрипт загружает первоначальные данные в пустую БД MariaDB
    
    :param data_file: - файл для чтения в pandas
    :param table: - наименование таблицы
    :param database_port: - данные для подключения к БД, вида login:password@IP_address:port/database_name
    :return: 
    """
    engine = create_engine(f'mysql+pymysql://{database_port}')
    file_path, file_extension = os.path.splitext(data_file)

    if file_extension == '.csv':
        df = pd.read_csv(data_file)
    elif file_extension == '.json':
        df = pd.read_json(data_file)

    try:
        df.to_sql(
            table,
            engine,
            index=True,
            chunksize=100,
            if_exists='replace'
        )
    except Exception as e:
        print(f'Ошибка: \n{e}')


if __name__ == '__main__':
    file_lists = {f'{PATH}/data/csv_files/ga_sessions.csv': 'user_session',
                  f'{PATH}/data/csv_files/ga_hits.csv': 'user_hits'}
    flag = 0

    for i, k in file_lists.items():
        flag += 1
        print(f'Загружаем данные в таблицу {k}')
        load(data_file=i, table=k, database_port='admin:NoPass123@db:3306/user_metrics')
        print(f'Загрузка завершена - {flag}/{len(file_lists)}')





