
-- Создание базы данных
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `user_metrics` /*!40100 DEFAULT CHARACTER SET latin1 */;

-- Подключение к базе данных
USE user_metrics;

CREATE USER 'airflow'@'%' IDENTIFIED BY '123456';
GRANT SELECT, INSERT, DELETE ON user_metrics.* TO 'airflow'@'%';
ALTER USER 'root'@'%' IDENTIFIED BY 'p76Se7BoVbrn';
FLUSH PRIVILEGES;

FLUSH PRIVILEGES ;

DROP TABLE IF EXISTS user_metrics;

-- Создание первой таблицы
CREATE TABLE user_session (
 id INT PRIMARY KEY AUTO_INCREMENT,
 session_id VARCHAR(100) NOT NULL,
 client_id VARCHAR(100) NOT NULL,
 visit_date VARCHAR(50) NOT NULL,
 visit_time VARCHAR(50) NOT NULL,
 visit_number INT NOT NULL,
 utm_source VARCHAR(50),
 utm_medium VARCHAR(50),
 utm_campaign VARCHAR(50),
 utm_adcontent VARCHAR(50),
 utm_keyword VARCHAR(50),
 device_category VARCHAR(50),
 device_os VARCHAR(20),
 device_brand VARCHAR(20),
 device_model VARCHAR(20),
 device_screen_resolution VARCHAR(50),
 device_browser VARCHAR(20),
 geo_country VARCHAR(20),
 geo_city VARCHAR(20)
) ENGINE=InnoDB;

-- Создание второй таблицы
CREATE TABLE user_hits (
 id INT PRIMARY KEY AUTO_INCREMENT,
 session_id VARCHAR(100) NOT NULL,
 hit_date VARCHAR(50) NOT NULL,
 hit_time VARCHAR(50),
 hit_number INT NOT NULL,
 hit_type VARCHAR(50),
 hit_referer VARCHAR(50),
 hit_page_path VARCHAR(50),
 event_category VARCHAR(50),
 event_action VARCHAR(50),
 event_label VARCHAR(50),
 event_value VARCHAR(50)
) ENGINE=InnoDB;
