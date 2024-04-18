# sberAutoSklbx
Finally work DE Skillbox

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

## Шаги:
1. Очистить данные в блокноте
2. Перенести код из блокнота в pipeline 
3. Поднять БД
4. Написать код, который отправляет данные в БД
5. Скомпоновать это все в airflow