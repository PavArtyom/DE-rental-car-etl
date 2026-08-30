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
    'csv_to_raw_layer',
    default_args=default_args,
    description='Загрузка исходных CSV-файлов в raw-слой DWH',
    schedule_interval=None,
    catchup=False,
    tags=['car_rental', 'etl', 'raw_layer']
)


def load_csv_to_postgres():
    """
    Оптимизированная загрузка CSV файлов в PostgreSQL
    """
    logger = logging.getLogger(__name__)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    raw_data_path = os.path.join(parent_dir, 'data', 'raw')

    if not os.path.exists(raw_data_path):
        logger.error(f"Папка {raw_data_path} не существует!")
        raise FileNotFoundError(f"Папка {raw_data_path} не существует!")

    logger.info(f"Загрузка данных из {raw_data_path}")

    pg = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
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

        create_queries = {
            'locations': """
                CREATE TABLE IF NOT EXISTS raw.locations (
                    location_id INTEGER,
                    city VARCHAR(100),
                    address TEXT,
                    phone VARCHAR(20),
                    opening_hours VARCHAR(50),
                    is_active VARCHAR(10)
                )
            """,
            'cars': """
                CREATE TABLE IF NOT EXISTS raw.cars (
                    car_id INTEGER,
                    brand VARCHAR(50),
                    model VARCHAR(50),
                    year INTEGER,
                    color VARCHAR(30),
                    license_plate VARCHAR(20),
                    vin VARCHAR(50),
                    current_location_id INTEGER,
                    purchase_date VARCHAR(20),
                    purchase_price DECIMAL(10, 2),
                    daily_rental_price DECIMAL(10, 2),
                    fuel_type VARCHAR(20),
                    transmission VARCHAR(20),
                    seats INTEGER,
                    status VARCHAR(20),
                    mileage INTEGER
                )
            """,
            'customers': """
                CREATE TABLE IF NOT EXISTS raw.customers (
                    customer_id INTEGER,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(150),
                    phone VARCHAR(20),
                    date_of_birth VARCHAR(20),
                    driving_license VARCHAR(50),
                    license_issue_date VARCHAR(20),
                    license_expiry_date VARCHAR(20),
                    address TEXT,
                    city VARCHAR(100),
                    registration_date VARCHAR(20),
                    loyalty_level VARCHAR(20)
                )
            """,
            'employees': """
                CREATE TABLE IF NOT EXISTS raw.employees (
                    employee_id INTEGER,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(150),
                    phone VARCHAR(20),
                    position VARCHAR(50),
                    hire_date VARCHAR(20),
                    salary DECIMAL(10, 2),
                    location_id INTEGER,
                    is_active VARCHAR(10)
                )
            """,
            'tariffs': """
                CREATE TABLE IF NOT EXISTS raw.tariffs (
                    tariff_id INTEGER,
                    tariff_name VARCHAR(50),
                    base_price_per_day DECIMAL(10, 2),
                    km_included_per_day INTEGER,
                    additional_km_price DECIMAL(10, 2),
                    deposit_amount DECIMAL(10, 2),
                    min_rental_days INTEGER,
                    max_rental_days INTEGER,
                    insurance_type VARCHAR(30),
                    cancellation_policy VARCHAR(30)
                )
            """,
            'rentals': """
                CREATE TABLE IF NOT EXISTS raw.rentals (
                    rental_id INTEGER,
                    car_id INTEGER,
                    customer_id INTEGER,
                    employee_id INTEGER,
                    tariff_id INTEGER,
                    pickup_location_id INTEGER,
                    return_location_id INTEGER,
                    rental_date VARCHAR(20),
                    scheduled_return_date VARCHAR(20),
                    actual_return_date VARCHAR(20),
                    rental_days INTEGER,
                    km_driven INTEGER,
                    additional_km INTEGER,
                    base_cost DECIMAL(10, 2),
                    additional_km_cost DECIMAL(10, 2),
                    total_cost DECIMAL(10, 2),
                    deposit_amount DECIMAL(10, 2),
                    deposit_returned DECIMAL(10, 2),
                    status VARCHAR(20),
                    notes TEXT
                )
            """,
            'payments': """
                CREATE TABLE IF NOT EXISTS raw.payments (
                    payment_id INTEGER,
                    rental_id INTEGER,
                    payment_date VARCHAR(20),
                    payment_method VARCHAR(30),
                    amount DECIMAL(10, 2),
                    payment_type VARCHAR(30),
                    status VARCHAR(20)
                )
            """,
            'car_condition': """
                CREATE TABLE IF NOT EXISTS raw.car_condition (
                    condition_id INTEGER,
                    car_id INTEGER,
                    entry_date VARCHAR(20),
                    entry_type VARCHAR(20),
                    description TEXT,
                    cost DECIMAL(10, 2),
                    reported_by VARCHAR(30),
                    is_repaired VARCHAR(10),
                    repair_date VARCHAR(20),
                    notes TEXT
                )
            """
        }

        for table in tables:
            logger.info(f"Создание/очистка таблицы raw.{table}...")
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS raw.{table};")
                cur.execute(create_queries[table])
        conn.commit()

        def load_table(table_name):
            csv_file = os.path.join(raw_data_path, f"{table_name}.csv")

            if not os.path.exists(csv_file):
                logger.warning(f"Файл {csv_file} не найден!")
                return 0

            logger.info(f"Начало загрузки {table_name}...")

            with open(csv_file, 'r', encoding='utf-8') as f:
                with conn.cursor() as cur:
                    next(f)
                    cur.copy_expert(
                        sql=f"COPY raw.{table_name} FROM STDIN WITH CSV NULL ''",
                        file=f
                    )

            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM raw.{table_name}")
                count = cur.fetchone()[0]

            logger.info(f"Загружено {count} записей в raw.{table_name}")
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

        logger.info("Итоговая статистика:")
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
                count = cur.fetchone()[0]
                logger.info(f"  raw.{table}: {count} записей")

    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
        raise
    finally:
        conn.close()


load_task = PythonOperator(
    task_id='load_csv_to_raw',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

