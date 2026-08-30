from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'processed_to_silver_with_constraints',
    default_args=default_args,
    description='Загрузка очищенных CSV-файлов в типизированный silver-слой',
    schedule_interval=None,
    catchup=False,
    tags=['car_rental', 'etl', 'silver_layer']
)


def load_processed_to_silver():

    logger = logging.getLogger(__name__)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    processed_data_path = os.path.join(parent_dir, 'data', 'processed')

    if not os.path.isdir(processed_data_path):
        logger.error(f"Папка {processed_data_path} не существует!")
        raise FileNotFoundError(f"Папка {processed_data_path} не существует!")

    logger.info(f"Загрузка данных из {processed_data_path}")

    pg = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        conn.commit()

        tables = [
            'locations',
            'cars',
            'customers',
            'employees',
            'tariffs',
            'rentals',
            'payments',
            'car_condition'
        ]


        logger.info("Очистка старых таблиц в silver...")
        with conn.cursor() as cur:
            for table in reversed(tables):
                cur.execute(f"DROP TABLE IF EXISTS silver.{table} CASCADE;")
        conn.commit()


        def load_table(table_name):
            csv_file = os.path.join(processed_data_path, f"{table_name}.csv")

            if not os.path.exists(csv_file):
                logger.warning(f"Файл {csv_file} не найден!")
                return 0

            logger.info(f"Загрузка {table_name}...")


            with open(csv_file, 'r', encoding='utf-8') as f:
                headers_line = f.readline().strip()
                headers = [h.strip('"') for h in headers_line.split(',')]


            with conn.cursor() as cur:
                columns = [f'"{h}" TEXT' for h in headers]
                create_sql = f"""
                    CREATE TABLE silver.{table_name} (
                        {', '.join(columns)}
                    )
                """
                cur.execute(create_sql)


            with open(csv_file, 'r', encoding='utf-8') as f:
                with conn.cursor() as cur:
                    next(f)  # Пропускаем заголовок
                    cur.copy_expert(
                        sql=f"COPY silver.{table_name} FROM STDIN WITH CSV NULL ''",
                        file=f
                    )


            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM silver.{table_name}")
                count = cur.fetchone()[0]

            logger.info(f"  Загружено: {count} записей")
            return count


        total_records = 0
        for table in tables:
            try:
                count = load_table(table)
                total_records += count
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка при загрузке {table}: {str(e)}")
                conn.rollback()
                raise

        logger.info(f"Всего загружено {total_records} записей")


        logger.info("Преобразование типов данных...")
        transform_data_types(conn, logger)
        conn.commit()


        logger.info("Добавление ограничений...")
        add_constraints(conn, logger)
        conn.commit()


        logger.info("\n" + "=" * 60)
        logger.info("ИТОГОВАЯ СТАТИСТИКА SILVER СЛОЯ:")
        logger.info("=" * 60)

        with conn.cursor() as cur:
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM silver.{table}")
                    count = cur.fetchone()[0]


                    cur.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(DISTINCT {get_primary_key(table)}) as unique_keys
                        FROM silver.{table}
                    """)
                    stats = cur.fetchone()

                    logger.info(f"{table:20} : {count:6} записей, {stats[1]}/{stats[0]} уникальных ключей")
                except Exception as e:
                    logger.info(f"{table:20} : ошибка статистики: {e}")

        logger.info("=" * 60)
        logger.info(" ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ В SILVER С ОГРАНИЧЕНИЯМИ")

    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
        raise
    finally:
        conn.close()


def transform_data_types(conn, logger):

    logger.info("  Преобразование locations...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.locations 
            ALTER COLUMN location_id TYPE INTEGER USING location_id::integer,
            ALTER COLUMN is_active TYPE BOOLEAN USING 
                CASE 
                    WHEN LOWER(is_active) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(is_active) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                    ELSE NULL
                END,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)

    logger.info("  Преобразование cars...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.cars 
            ALTER COLUMN car_id TYPE INTEGER USING car_id::integer,
            ALTER COLUMN year TYPE INTEGER USING year::integer,
            ALTER COLUMN current_location_id TYPE INTEGER USING current_location_id::integer,
            ALTER COLUMN purchase_date TYPE DATE USING purchase_date::date,
            ALTER COLUMN purchase_price TYPE DECIMAL(10,2) USING purchase_price::decimal,
            ALTER COLUMN daily_rental_price TYPE DECIMAL(10,2) USING daily_rental_price::decimal,
            ALTER COLUMN seats TYPE INTEGER USING seats::integer,
            ALTER COLUMN mileage TYPE INTEGER USING mileage::integer,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование customers...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.customers 
            ALTER COLUMN customer_id TYPE INTEGER USING customer_id::integer,
            ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
            ALTER COLUMN license_issue_date TYPE DATE USING license_issue_date::date,
            ALTER COLUMN license_expiry_date TYPE DATE USING license_expiry_date::date,
            ALTER COLUMN registration_date TYPE DATE USING registration_date::date,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование employees...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.employees 
            ALTER COLUMN employee_id TYPE INTEGER USING employee_id::integer,
            ALTER COLUMN location_id TYPE INTEGER USING location_id::integer,
            ALTER COLUMN hire_date TYPE DATE USING hire_date::date,
            ALTER COLUMN salary TYPE DECIMAL(10,2) USING salary::decimal,
            ALTER COLUMN is_active TYPE BOOLEAN USING 
                CASE 
                    WHEN LOWER(is_active) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(is_active) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                    ELSE NULL
                END,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование tariffs...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.tariffs 
            ALTER COLUMN tariff_id TYPE INTEGER USING tariff_id::integer,
            ALTER COLUMN base_price_per_day TYPE DECIMAL(10,2) USING base_price_per_day::decimal,
            ALTER COLUMN km_included_per_day TYPE INTEGER USING km_included_per_day::integer,
            ALTER COLUMN additional_km_price TYPE DECIMAL(10,2) USING additional_km_price::decimal,
            ALTER COLUMN deposit_amount TYPE DECIMAL(10,2) USING deposit_amount::decimal,
            ALTER COLUMN min_rental_days TYPE INTEGER USING min_rental_days::integer,
            ALTER COLUMN max_rental_days TYPE INTEGER USING max_rental_days::integer,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование rentals...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.rentals 
            ALTER COLUMN rental_id TYPE INTEGER USING rental_id::integer,
            ALTER COLUMN car_id TYPE INTEGER USING car_id::integer,
            ALTER COLUMN customer_id TYPE INTEGER USING customer_id::integer,
            ALTER COLUMN employee_id TYPE INTEGER USING employee_id::integer,
            ALTER COLUMN tariff_id TYPE INTEGER USING tariff_id::integer,
            ALTER COLUMN pickup_location_id TYPE INTEGER USING pickup_location_id::integer,
            ALTER COLUMN return_location_id TYPE INTEGER USING return_location_id::integer,
            ALTER COLUMN rental_date TYPE DATE USING rental_date::date,
            ALTER COLUMN scheduled_return_date TYPE DATE USING scheduled_return_date::date,
            ALTER COLUMN actual_return_date TYPE DATE USING actual_return_date::date,
            ALTER COLUMN rental_days TYPE INTEGER USING rental_days::integer,
            ALTER COLUMN km_driven TYPE INTEGER USING km_driven::integer,
            ALTER COLUMN additional_km TYPE INTEGER USING additional_km::integer,
            ALTER COLUMN base_cost TYPE DECIMAL(10,2) USING base_cost::decimal,
            ALTER COLUMN additional_km_cost TYPE DECIMAL(10,2) USING additional_km_cost::decimal,
            ALTER COLUMN total_cost TYPE DECIMAL(10,2) USING total_cost::decimal,
            ALTER COLUMN deposit_amount TYPE DECIMAL(10,2) USING deposit_amount::decimal,
            ALTER COLUMN deposit_returned TYPE DECIMAL(10,2) USING deposit_returned::decimal,
            ALTER COLUMN late_days TYPE INTEGER USING late_days::integer,
            ALTER COLUMN is_late_return TYPE BOOLEAN USING 
                CASE 
                    WHEN LOWER(is_late_return) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(is_late_return) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                    ELSE FALSE
                END,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование payments...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.payments 
            ALTER COLUMN payment_id TYPE INTEGER USING payment_id::integer,
            ALTER COLUMN rental_id TYPE INTEGER USING rental_id::integer,
            ALTER COLUMN payment_date TYPE TIMESTAMP USING payment_date::timestamp,
            ALTER COLUMN amount TYPE DECIMAL(10,2) USING amount::decimal,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)


    logger.info("  Преобразование car_condition...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.car_condition 
            ALTER COLUMN condition_id TYPE INTEGER USING condition_id::integer,
            ALTER COLUMN car_id TYPE INTEGER USING car_id::integer,
            ALTER COLUMN entry_date TYPE TIMESTAMP USING entry_date::timestamp,
            ALTER COLUMN cost TYPE DECIMAL(10,2) USING cost::decimal,
            ALTER COLUMN is_repaired TYPE BOOLEAN USING 
                CASE 
                    WHEN LOWER(is_repaired) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(is_repaired) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                    ELSE NULL
                END,
            ALTER COLUMN repair_date TYPE DATE USING repair_date::date,
            ALTER COLUMN created_at TYPE TIMESTAMP USING created_at::timestamp,
            ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at::timestamp;
        """)

    logger.info(" Все типы данных преобразованы")


def get_primary_key(table_name):
    primary_keys = {
        'locations': 'location_id',
        'cars': 'car_id',
        'customers': 'customer_id',
        'employees': 'employee_id',
        'tariffs': 'tariff_id',
        'rentals': 'rental_id',
        'payments': 'payment_id',
        'car_condition': 'condition_id'
    }
    return primary_keys.get(table_name, 'id')


def add_constraints(conn, logger):

    logger.info("  Добавление первичных ключей...")

    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.locations 
            ADD CONSTRAINT pk_locations PRIMARY KEY (location_id);

            ALTER TABLE silver.cars 
            ADD CONSTRAINT pk_cars PRIMARY KEY (car_id);

            ALTER TABLE silver.customers 
            ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

            ALTER TABLE silver.employees 
            ADD CONSTRAINT pk_employees PRIMARY KEY (employee_id);

            ALTER TABLE silver.tariffs 
            ADD CONSTRAINT pk_tariffs PRIMARY KEY (tariff_id);

            ALTER TABLE silver.rentals 
            ADD CONSTRAINT pk_rentals PRIMARY KEY (rental_id);

            ALTER TABLE silver.payments 
            ADD CONSTRAINT pk_payments PRIMARY KEY (payment_id);

            ALTER TABLE silver.car_condition 
            ADD CONSTRAINT pk_car_condition PRIMARY KEY (condition_id);
        """)

    logger.info("  Добавление внешних ключей...")

    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE silver.cars 
            ADD CONSTRAINT fk_cars_location 
            FOREIGN KEY (current_location_id) 
            REFERENCES silver.locations(location_id);
        """)

        cur.execute("""
            ALTER TABLE silver.employees 
            ADD CONSTRAINT fk_employees_location 
            FOREIGN KEY (location_id) 
            REFERENCES silver.locations(location_id);
        """)

        cur.execute("""
            ALTER TABLE silver.rentals 
            ADD CONSTRAINT fk_rentals_car 
            FOREIGN KEY (car_id) 
            REFERENCES silver.cars(car_id),

            ADD CONSTRAINT fk_rentals_customer 
            FOREIGN KEY (customer_id) 
            REFERENCES silver.customers(customer_id),

            ADD CONSTRAINT fk_rentals_employee 
            FOREIGN KEY (employee_id) 
            REFERENCES silver.employees(employee_id),

            ADD CONSTRAINT fk_rentals_tariff 
            FOREIGN KEY (tariff_id) 
            REFERENCES silver.tariffs(tariff_id),

            ADD CONSTRAINT fk_rentals_pickup_location 
            FOREIGN KEY (pickup_location_id) 
            REFERENCES silver.locations(location_id),

            ADD CONSTRAINT fk_rentals_return_location 
            FOREIGN KEY (return_location_id) 
            REFERENCES silver.locations(location_id);
        """)


        cur.execute("""
            ALTER TABLE silver.payments 
            ADD CONSTRAINT fk_payments_rental 
            FOREIGN KEY (rental_id) 
            REFERENCES silver.rentals(rental_id);
        """)


        cur.execute("""
            ALTER TABLE silver.car_condition 
            ADD CONSTRAINT fk_car_condition_car 
            FOREIGN KEY (car_id) 
            REFERENCES silver.cars(car_id);
        """)

    logger.info(" Все ограничения добавлены")


load_task = PythonOperator(
    task_id='load_processed_to_silver_with_constraints',
    python_callable=load_processed_to_silver,
    dag=dag,
)
