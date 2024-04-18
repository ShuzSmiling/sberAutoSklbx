import pandas as pd
import os

path = os.environ.get('PROJECT_PATH', '.')


def del_col(df, *args):
    # Удаление колонок
    del_col = []
    for i in args:
        del_col.append(i)

    return df.drop(del_col, axis=1)


def et_data_sess(df):
    # Удаляем пустые колонки
    df = del_col(df, 'device_model', 'utm_keyword', 'device_os')
    # Заполняем пропуски
    df.device_brand = df.device_brand.fillna('other')
    df.utm_source = df.utm_source.fillna(df.utm_source.mode()[0])
    df.utm_campaign = df.utm_campaign.fillna(df.utm_campaign.mode()[0])
    df.utm_adcontent = df.utm_adcontent.fillna(df.utm_adcontent.mode()[0])
    # Преобразуем дату и время
    df['visit_date'] = pd.to_datetime(df.visit_date)
    df['visit_time'] = pd.to_datetime(df['visit_time'], format='%H:%M:%S').dt.time

    return df


def et_data_hits(df):
    df = del_col(df, 'event_label', 'hit_referer', 'hit_time', 'event_value')
    df['hit_date'] = pd.to_datetime(df.hit_date)

    return df


def main(path):
    head = '''
CREATE DATABASE /*!32312 IF NOT EXISTS*/`user_metrics` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `user_metrics`;

CREATE USER 'client1'@'%' IDENTIFIED BY 'client1';
GRANT SELECT ON 'user_metrics'.* TO 'client1'@'%';
ALTER USER 'root'@'%' IDENTIFIED BY 'qwerty123';
FLUSH PRIVILEGES;

FLUSH PRIVILEGES;

DROP TABLE IF EXISTS `user_metrics`;

CREATE TABLE `user_sessions` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `session_id` VARCHAR(255) NOT NULL,
    `client_id` VARCHAR(255) NOT NULL,
    `visit_date` DATETIME NOT NULL,
    `visit_number` INT NOT NULL,
    `utm_source` VARCHAR(255) NOT NULL,
    `utm_medium` VARCHAR(255) NOT NULL,
    `utm_campaign` VARCHAR(255) NOT NULL,
    `utm_adcontent` VARCHAR(255) NOT NULL,
    `device_category` VARCHAR(255) NOT NULL,
    `device_brand` VARCHAR(255) NOT NULL,
    `device_screen_resolution` VARCHAR(255) NOT NULL,
    `geo_country` VARCHAR(255) NOT NULL,
    `geo_city` VARCHAR(255) NOT NULL,
) ENGINE=InnoDB;

CREATE TABLE `user_hits` (
   `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
   `session_id` VARCHAR(255) NOT NULL,
   `hit_date` DATETIME NOT NULL,
   `hit_number` INT NOT NULL,
   `hit_type` VARCHAR(255) NOT NULL,
   `hit_page_path` VARCHAR(255) NOT NULL,
   `event_category` VARCHAR(255) NOT NULL,
   `event_action` VARCHAR(255) NOT NULL,
) ENGINE=InnoDB;
'''
    # df_sess = pd.read_csv('../data/models/ga_sessions.csv')
    # df_sess = et_data_sess(df_sess)
    # df_hits = pd.read_csv('../data/models/ga_hits.csv')
    # df_hits = et_data_hits(df_hits)

    with open(path, 'w') as out:
        out.write(head)


if __name__ == '__main__':
    main('../data/sql_gen/green.sql')
