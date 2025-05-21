## Необходимо скачать
*   Скачанные JDBC-драйверы в папке `./jars/`:
    *   `postgresql-42.7.5.jar`
    *   `clickhouse-jdbc-0.4.6-shaded.jar`
    *   `если возникнет проблема с версиями, тогда еще antlr4-runtime-4.13.2.jar`

1.  **Клонировать репозиторий:**
    ```bash
    git clone <https://github.com/willr4in/BigDataSpark>
    cd BigDataSpark
    ```

2.  **Запустить сервисы (PostgreSQL, Spark, ClickHouse):**
    Из корневой директории проекта:
    ```bash
    docker-compose up --build -d
    ```
    *PostgreSQL будет инициализирован: таблица `mock_data` будет создана и заполнена из CSV*

3. **Инициализация clickhouse базы данных:**
   ```bash
   docker exec -it clickhouse clickhouse-client < /app/clickhouse_init.sql 
   ```

4.  **Преобразование в снежинку:**
    ```bash
    docker exec -it spark bash
    spark-submit --jars /app/jars/postgresql-42.7.5.jar /app/spark_scripts/snowflake_model.py
    ```

5.  **Загрузка данных в Clickhouse и работа отчетов**
    ```bash
    spark-submit \
      --jars /app/jars/postgresql-42.7.5.jar,/app/jars/clickhouse-jdbc-0.4.6-shaded.jar \
      /app/spark_scripts/reports.py
    ```

## Результаты

1.  **Проверить список таблиц-отчетов в ClickHouse:**
    ```bash
    docker exec -it clickhouse clickhouse-client --query "SHOW TABLES;"
    ```
    *Должен отобразиться список таблиц, таких как `top_products`, `revenue_by_category`, и т.д.*

## Остановка проекта

1.  **Остановить и удалить контейнеры:**
    ```bash
    docker-compose down -v (удаление томов)
    ```
---
