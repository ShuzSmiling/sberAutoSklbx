# sberAutoSklbx


## Описание

| GA Sessions (ga_sessions.pkl) | Описание                         | GA Hits (ga_hits.pkl) | Описание                                  |
|-------------------------------|----------------------------------|-----------------------|-------------------------------------------|
| session_id                    | ID визита;                       | session_id            | ID визита;                                |
| client_id                     | ID посетителя;                   | hit_type              | тип события;                              |
| visit_date                    | дата визита;                     | hit_date              | дата события;                             |
| visit_time                    | время визита;                    | hit_time              | время события;                            |
| visit_number                  | порядковый номер визита клиента; | hit_number            | порядковый номер события в рамках сессии; |
| utm_source                    | канал привлечения;               | hit_referer           | источник события;                         |
| utm_medium                    | тип привлечения;                 | hit_page_path         | страница события;                         |
| utm_campaign                  | рекламная кампания;              | event_category        | тип действия;                             |
| utm_keyword                   | ключевое слово;                  | event_action          | действие;                                 |
| device_category               | тип устройства;                  | event_label           | тег действия;                             |
| device_os                     | ОС устройства;                   | event_value           | значение результата действияv             |
| device_brand                  | марка устройства;                |                       |                                           |
| device_model                  | модель устройства;               |                       |                                           |
| device_screen_resolution      | разрешение экрана;               |                       |                                           |
| device_browser                | браузер;                         |                       |                                           |
| geo_country                   | страна;                          |                       |                                           |
| geo_city                      | город                            |                       |                                           |

## Развертывание

1. В папке с проектом запустить airflow-init
    ```
    docker-compose up airflow-init
    ```
2. Далее разворачиваем проект
   ```
   docker compose up
	```
3. Далее скопируем файлы проекта в docker-container
   ```
	docker cp ~/airflow_hw worker_id:/home/airflow/airflow_hw  
	docker cp ~/airflow_hw scheduler_id:/home/airflow/airflow_hw
	```
	Узнать id контейнеров можно:
	```
	docker ps | grep worker  
	docker ps | grep scheduler
	```
4. Коннектимся к airflow
	```
	docker exec -it -u root worker_id bash
	docker exec -it scheduler_id bash
	```
5. Устанавливаем софт и навешиваем права 
   ```
   chmod -R 777 /home/airflow/sberAutoSklbx/data /home/airflow/sberAutoSklbx/dags /home/airflow/sberAutoSklbx/modules
   
   su airflow
   pip install pymysql
	```
6. Загружаем данные основные данные в БД
   ```
   cd /home/airflow/sberAutoSklbx
   python3 modules/load.py
	```
	Загрузка первоначальных сырых данных занимает примерно 30 минут
7. Проект развернут, данные загружены в БД. Можно играться с airflow. Данные для подключения:
   - **airflow**:
	   - login - **airflow**
	   - password - **airflow**
	   - address - **localhost:8080**
   - **adminer**:
	   - login - **airflow**
	   - password - **123456**
	   - address - **localhost:33380**
## Запуск DAG'а
Переходим по адресу выше в airflow -> запускаем DAG. В админере смотрим загрузку новых данных в БД

## Структура проекта
```
├── adminer
│   └── theme
│       └── adminer.css
├── config
├── dags
│   ├── __init__.py
│   ├── load_json_dag.py
├── data
│   ├── csv_files
│   └── test_json
├── docker-compose.yaml
├── init
│   └── db.sql
├── logs
├── modules
│   ├── __init__.py
│   ├── load_files.py
│   ├── load.py
├── plugins
├── README.md
└── requirements.txt

```

* **adminer** - директория с конфигами adminer
* **config** - директория необходимая airflow
* **dags** - директория с дагами, необходимая для airflow
* **data** - директория для хранения data файлов, *необходима airflow*
	* csv_files - исходные данные в формате csv
	* test_json - новые данные для обработки
* **init** - содержит sql файл, который создает БД в MariaDB
* **logs** - *необходим airflow*, содержит логи
* **modules** - *необходим airflow*, содержит скрипты
* **plugins** - *необходим airflow*