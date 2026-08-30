import pandas as pd
from datetime import datetime
import os
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_raw_data():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)

    raw_path = os.path.join(parent_dir, 'data', 'raw')
    processed_path = os.path.join(parent_dir, 'data', 'processed')

    logger.info(f"Текущая директория: {current_dir}")
    logger.info(f"Папка с исходными данными: {raw_path}")
    logger.info(f"Папка для обработанных данных: {processed_path}")


    if not os.path.exists(raw_path):
        logger.error(f"Папка {raw_path} не существует!")
        logger.error("Сначала сформируйте данные с помощью generate_source_data.py")
        raise FileNotFoundError(f"Папка с исходными данными не найдена: {raw_path}")

    os.makedirs(processed_path, exist_ok=True)

    logger.info("=" * 60)
    logger.info("НАЧАЛО ОБРАБОТКИ ДАННЫХ")
    logger.info("=" * 60)

    files = [
        'locations.csv',
        'cars.csv',
        'customers.csv',
        'employees.csv',
        'tariffs.csv',
        'rentals.csv',
        'payments.csv',
        'car_condition.csv'
    ]

    missing_files = []
    for file in files:
        if not os.path.exists(os.path.join(raw_path, file)):
            missing_files.append(file)

    if missing_files:
        logger.error(f"Отсутствуют файлы: {missing_files}")
        logger.error("Сначала сформируйте все данные с помощью generate_source_data.py")
        raise FileNotFoundError(f"Отсутствуют исходные файлы: {', '.join(missing_files)}")

    process_locations(raw_path, processed_path)
    process_cars(raw_path, processed_path)
    process_customers(raw_path, processed_path)
    process_employees(raw_path, processed_path)
    process_tariffs(raw_path, processed_path)
    process_rentals(raw_path, processed_path)
    process_payments(raw_path, processed_path)
    process_car_condition(raw_path, processed_path)

    logger.info("=" * 60)
    logger.info(" ВСЕ ДАННЫЕ ОБРАБОТАНЫ И СОХРАНЕНЫ")
    logger.info(f" Папка: {processed_path}")
    logger.info("=" * 60)

    logger.info("Созданные файлы:")
    for file in files:
        file_path = os.path.join(processed_path, file)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            logger.info(f"   {file}: {len(df)} записей")


def process_locations(raw_path, processed_path):
    logger.info(" Обработка locations...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'locations.csv'))

        df['is_active'] = (
            df['is_active']
            .map({'true': True, 'false': False})
            .astype('boolean')
            .fillna(True)
            .astype(bool)
        )

        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'locations.csv')
        df.to_csv(output_file, index=False)
        logger.info(f" Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  ✗ Ошибка при обработке locations: {e}")
        raise


def process_cars(raw_path, processed_path):
    logger.info(" Обработка cars...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'cars.csv'))

        df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(2020).astype(int)
        df['year'] = df['year'].clip(2010, 2024)


        df['purchase_price'] = pd.to_numeric(df['purchase_price'], errors='coerce').abs()
        df['daily_rental_price'] = pd.to_numeric(df['daily_rental_price'], errors='coerce').abs()

        df['purchase_price'] = df['purchase_price'].fillna(df['purchase_price'].median())
        df['daily_rental_price'] = df['daily_rental_price'].fillna(df['daily_rental_price'].median())


        df['purchase_price'] = df['purchase_price'].clip(10000, 200000)
        df['daily_rental_price'] = df['daily_rental_price'].clip(10, 500)


        df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce').fillna(0).astype(int)
        df['mileage'] = df['mileage'].clip(0, 500000)


        df['status'] = df['status'].fillna('Available')


        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'cars.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  ✗ Ошибка при обработке cars: {e}")
        raise


def process_customers(raw_path, processed_path):
    logger.info(" Обработка customers...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'customers.csv'))


        date_columns = ['date_of_birth', 'license_issue_date', 'license_expiry_date', 'registration_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')


        current_date = pd.Timestamp.now()
        max_birth_date = current_date - pd.Timedelta(days=365 * 18)


        mask = (df['date_of_birth'] > max_birth_date) | df['date_of_birth'].isna()
        df.loc[mask, 'date_of_birth'] = current_date - pd.Timedelta(days=365 * 25)  # Ставим 25 лет


        df['email'] = df['email'].str.lower().fillna('unknown@example.com')


        df['driving_license'] = df['driving_license'].fillna('')
        duplicates = df['driving_license'].duplicated(keep=False)
        df.loc[duplicates, 'driving_license'] = df.loc[duplicates, 'driving_license'] + '_' + df.loc[
            duplicates, 'customer_id'].astype(str)


        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'customers.csv')
        df.to_csv(output_file, index=False)
        logger.info(f" Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f" Ошибка при обработке customers: {e}")
        raise


def process_employees(raw_path, processed_path):
    logger.info("📌 Обработка employees...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'employees.csv'))


        df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
        df['is_active'] = (
            df['is_active']
            .map({'true': True, 'false': False})
            .astype('boolean')
            .fillna(True)
            .astype(bool)
        )


        df['salary'] = pd.to_numeric(df['salary'], errors='coerce').abs()
        df['salary'] = df['salary'].fillna(df['salary'].median())
        df['salary'] = df['salary'].clip(500, 5000)


        df['email'] = df['email'].str.lower().fillna('employee@rental.by')


        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'employees.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  ✓ Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  ✗ Ошибка при обработке employees: {e}")
        raise


def process_tariffs(raw_path, processed_path):
    logger.info(" Обработка tariffs...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'tariffs.csv'))

        numeric_cols = ['base_price_per_day', 'additional_km_price', 'deposit_amount']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').abs()
            df[col] = df[col].fillna(df[col].median())


        df['base_price_per_day'] = df['base_price_per_day'].clip(10, 500)
        df['additional_km_price'] = df['additional_km_price'].clip(0.1, 10)
        df['deposit_amount'] = df['deposit_amount'].clip(100, 3000)


        int_cols = ['km_included_per_day', 'min_rental_days', 'max_rental_days']
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(1).astype(int)


        df['max_rental_days'] = df.apply(
            lambda x: max(x['min_rental_days'], x['max_rental_days']),
            axis=1
        )


        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'tariffs.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f" Ошибка при обработке tariffs: {e}")
        raise


def process_rentals(raw_path, processed_path):
    logger.info(" Обработка rentals...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'rentals.csv'))

        date_columns = ['rental_date', 'scheduled_return_date', 'actual_return_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')


        mask = df['actual_return_date'] < df['rental_date']
        df.loc[mask, 'actual_return_date'] = df.loc[mask, 'rental_date'] + pd.Timedelta(days=1)

        mask = df['scheduled_return_date'] < df['rental_date']
        df.loc[mask, 'scheduled_return_date'] = df.loc[mask, 'rental_date'] + pd.Timedelta(days=1)

        numeric_cols = ['rental_days', 'km_driven', 'additional_km', 'base_cost',
                        'additional_km_cost', 'total_cost', 'deposit_amount', 'deposit_returned']

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

            if col in ['total_cost', 'base_cost', 'additional_km_cost']:
                df[col] = df[col].abs()
            elif col == 'deposit_returned':
                df[col] = df[col].clip(0, df['deposit_amount'])


        mask = df['rental_days'].isna() & df['rental_date'].notna() & df['scheduled_return_date'].notna()
        df.loc[mask, 'rental_days'] = (
            df.loc[mask, 'scheduled_return_date'] - df.loc[mask, 'rental_date']
        ).dt.days

        df['rental_days'] = df['rental_days'].fillna(1).clip(lower=1).astype(int)
        df['km_driven'] = df['km_driven'].fillna(100).astype(int)
        df['additional_km'] = df['additional_km'].fillna(0).astype(int)


        mask = df['total_cost'].isna() & df['base_cost'].notna() & df['additional_km_cost'].notna()
        df.loc[mask, 'total_cost'] = df.loc[mask, 'base_cost'] + df.loc[mask, 'additional_km_cost']


        df['status'] = df['status'].fillna('Completed')


        df['notes'] = df['notes'].fillna('')

        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        df['is_late_return'] = df['actual_return_date'] > df['scheduled_return_date']
        df['late_days'] = (df['actual_return_date'] - df['scheduled_return_date']).dt.days.clip(0, None)


        output_file = os.path.join(processed_path, 'rentals.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  ✓ Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  ✗ Ошибка при обработке rentals: {e}")
        raise


def process_payments(raw_path, processed_path):
    logger.info(" Обработка payments...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'payments.csv'))


        df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')

        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').abs()
        df['amount'] = df['amount'].fillna(df['amount'].median())
        df['amount'] = df['amount'].clip(0, 10000)  # Максимальный платеж 10,000 BYN


        df['status'] = df['status'].fillna('Completed')


        df['payment_method'] = df['payment_method'].fillna('Cash')
        df['payment_type'] = df['payment_type'].fillna('Rental')

        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time


        output_file = os.path.join(processed_path, 'payments.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  ✓ Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  ✗ Ошибка при обработке payments: {e}")
        raise


def process_car_condition(raw_path, processed_path):
    logger.info(" Обработка car_condition...")
    try:
        df = pd.read_csv(os.path.join(raw_path, 'car_condition.csv'))


        df['entry_date'] = pd.to_datetime(df['entry_date'], errors='coerce')
        df['repair_date'] = pd.to_datetime(df['repair_date'], errors='coerce')


        mask = df['repair_date'] < df['entry_date']
        df.loc[mask, 'repair_date'] = df.loc[mask, 'entry_date'] + pd.Timedelta(days=1)


        df['cost'] = pd.to_numeric(df['cost'], errors='coerce').abs()
        df['cost'] = df['cost'].fillna(df['cost'].median())
        df['cost'] = df['cost'].clip(0, 20000)


        df['is_repaired'] = (
            df['is_repaired']
            .map({'true': True, 'false': False})
            .astype('boolean')
            .fillna(False)
            .astype(bool)
        )


        df.loc[df['repair_date'].notna(), 'is_repaired'] = True


        df['entry_type'] = df['entry_type'].fillna('Maintenance')
        df['description'] = df['description'].fillna('Regular check')
        df['reported_by'] = df['reported_by'].fillna('Employee')
        df['notes'] = df['notes'].fillna('')


        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time

        output_file = os.path.join(processed_path, 'car_condition.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"  Сохранено {len(df)} записей в {output_file}")

    except Exception as e:
        logger.error(f"  Ошибка при обработке car_condition: {e}")
        raise


if __name__ == "__main__":
    process_raw_data()
