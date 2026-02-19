import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


np.random.seed(42)
random.seed(42)


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
data_dir = os.path.join(parent_dir, 'data', 'raw')
os.makedirs(data_dir, exist_ok=True)


N_RENTALS = 15000
N_CARS = 120
N_CUSTOMERS = 8000
N_LOCATIONS = 15
N_EMPLOYEES = 80
N_TARIFFS = 5


belarus_cities = [
    'Minsk', 'Gomel', 'Mogilev', 'Vitebsk', 'Grodno', 'Brest', 'Baranovichi',
    'Borisov', 'Pinsk', 'Orsha', 'Molodechno', 'Lida', 'Bobruisk', 'Slutsk',
    'Zhlobin', 'Svetlogorsk', 'Rechitsa', 'Polotsk', 'Novopolotsk'
]

car_brands_models = {
    'Volkswagen': ['Polo', 'Golf', 'Passat', 'Tiguan', 'Jetta'],
    'Skoda': ['Octavia', 'Rapid', 'Superb', 'Kodiaq'],
    'Renault': ['Logan', 'Sandero', 'Duster', 'Kaptur'],
    'Peugeot': ['308', '408', '3008', '2008'],
    'Ford': ['Focus', 'Mondeo', 'Kuga', 'EcoSport'],
    'Toyota': ['Corolla', 'Camry', 'RAV4', 'Prius'],
    'Hyundai': ['Solaris', 'Elantra', 'Tucson', 'Creta'],
    'Kia': ['Rio', 'Ceed', 'Sportage', 'Seltos'],
    'BMW': ['3 Series', '5 Series', 'X1', 'X3'],
    'Mercedes': ['C-Class', 'E-Class', 'GLA', 'GLC'],
    'Audi': ['A3', 'A4', 'A6', 'Q3', 'Q5'],
    'Lada': ['Granta', 'Vesta', 'Largus', 'XRAY'],
    'Geely': ['Coolray', 'Atlas', 'Emgrand'],
    'BYD': ['Song Plus', 'Han', 'Tang'],
    'Chery': ['Tiggo 7 Pro', 'Tiggo 8 Pro']
}



def random_date(start_year=2020, end_year=2025):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


print(f"Генерация данных... (сохраняем в: {data_dir})")


print("1. locations...")
locations = []
for i in range(1, N_LOCATIONS + 1):
    locations.append({
        'location_id': i,
        'city': random.choice(belarus_cities),
        'address': f'Street {random.randint(1, 100)}, Building {random.randint(1, 50)}',
        'phone': f'+37529{random.randint(1000000, 9999999)}',
        'opening_hours': '08:00-20:00',
        'is_active': random.choice(['true', 'false'])
    })
locations_df = pd.DataFrame(locations)


print("2. cars...")
cars = []
car_prices = {
    'Volkswagen': {'Polo': (35000, 50000), 'Golf': (45000, 65000), 'Passat': (60000, 85000), 'Tiguan': (70000, 95000),
                   'Jetta': (40000, 55000)},
    'Skoda': {'Octavia': (40000, 60000), 'Rapid': (35000, 48000), 'Superb': (65000, 90000), 'Kodiaq': (75000, 100000)},
    'Renault': {'Logan': (30000, 42000), 'Sandero': (28000, 40000), 'Duster': (45000, 65000), 'Kaptur': (50000, 70000)},
    'Peugeot': {'308': (40000, 55000), '408': (45000, 60000), '3008': (60000, 80000), '2008': (45000, 60000)},
    'Ford': {'Focus': (38000, 52000), 'Mondeo': (55000, 75000), 'Kuga': (65000, 85000), 'EcoSport': (45000, 60000)},
    'Toyota': {'Corolla': (45000, 65000), 'Camry': (70000, 95000), 'RAV4': (75000, 100000), 'Prius': (60000, 80000)},
    'Hyundai': {'Solaris': (35000, 48000), 'Elantra': (40000, 55000), 'Tucson': (60000, 80000),
                'Creta': (50000, 70000)},
    'Kia': {'Rio': (35000, 48000), 'Ceed': (40000, 55000), 'Sportage': (65000, 85000), 'Seltos': (55000, 75000)},
    'BMW': {'3 Series': (80000, 120000), '5 Series': (100000, 150000), 'X1': (90000, 130000), 'X3': (110000, 160000)},
    'Mercedes': {'C-Class': (85000, 125000), 'E-Class': (110000, 160000), 'GLA': (95000, 135000),
                 'GLC': (120000, 170000)},
    'Audi': {'A3': (75000, 110000), 'A4': (90000, 130000), 'A6': (100000, 150000), 'Q3': (95000, 135000),
             'Q5': (115000, 165000)},
    'Lada': {'Granta': (25000, 35000), 'Vesta': (30000, 42000), 'Largus': (32000, 45000), 'XRAY': (35000, 50000)},
    'Geely': {'Coolray': (40000, 55000), 'Atlas': (45000, 60000), 'Emgrand': (35000, 48000)},
    'BYD': {'Song Plus': (55000, 75000), 'Han': (70000, 95000), 'Tang': (80000, 110000)},
    'Chery': {'Tiggo 7 Pro': (45000, 60000), 'Tiggo 8 Pro': (55000, 75000)}
}

for i in range(1, N_CARS + 1):
    brand = random.choice(list(car_brands_models.keys()))
    model = random.choice(car_brands_models[brand])
    year = random.randint(2018, 2024)


    min_price, max_price = car_prices[brand][model]
    purchase_price = round(random.uniform(min_price, max_price), 2)


    daily_rental_price = round(purchase_price * random.uniform(0.008, 0.025), 2)

    cars.append({
        'car_id': i,
        'brand': brand,
        'model': model,
        'year': year,
        'color': random.choice(['White', 'Black', 'Silver', 'Gray', 'Blue', 'Red']),
        'license_plate': f'{random.choice(["AB", "MR", "NE"])}{random.randint(1000, 9999)}{random.choice(["BE", "BY"])}',
        'vin': f'VIN{random.randint(10000000000000000, 99999999999999999)}',
        'current_location_id': random.randint(1, N_LOCATIONS),
        'purchase_date': random_date(2018, 2023),
        'purchase_price': purchase_price,
        'daily_rental_price': daily_rental_price,
        'fuel_type': random.choice(['Petrol', 'Diesel', 'Hybrid']),
        'transmission': random.choice(['Automatic', 'Manual']),
        'seats': random.choice([4, 5, 7]),
        'status': random.choice(['Available', 'Rented', 'Maintenance', 'Reserved']),
        'mileage': random.randint(1000, 120000)
    })
cars_df = pd.DataFrame(cars)


print("3. customers...")
first_names = ['Ivan', 'Alexander', 'Dmitry', 'Sergey', 'Andrey', 'Mikhail',
               'Anna', 'Maria', 'Elena', 'Olga', 'Natalia', 'Tatiana']
last_names = ['Ivanov', 'Petrov', 'Sidorov', 'Smirnov', 'Kuznetsov', 'Popov',
              'Volkov', 'Kozlov', 'Novikov', 'Morozov', 'Pavlov', 'Semenov']

customers = []
for i in range(1, N_CUSTOMERS + 1):
    customers.append({
        'customer_id': i,
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'email': f'customer{i}@example.com',
        'phone': f'+37529{random.randint(1000000, 9999999)}',
        'date_of_birth': random_date(1950, 2002),
        'driving_license': f'AB{random.randint(1000000, 9999999)}',
        'license_issue_date': random_date(2010, 2023),
        'license_expiry_date': random_date(2025, 2030),
        'address': f'Street {random.randint(1, 100)}, Apt {random.randint(1, 200)}',
        'city': random.choice(belarus_cities),
        'registration_date': random_date(2020, 2024),
        'loyalty_level': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
    })
customers_df = pd.DataFrame(customers)


print("4. employees...")
employees = []
for i in range(1, N_EMPLOYEES + 1):
    position = random.choice(['Rental Agent', 'Manager', 'Support Agent', 'Car Inspector'])


    if position == 'Rental Agent':
        salary = round(random.uniform(1200, 1800), 2)
    elif position == 'Manager':
        salary = round(random.uniform(2000, 3000), 2)
    elif position == 'Support Agent':
        salary = round(random.uniform(1000, 1500), 2)
    else:  # Car Inspector
        salary = round(random.uniform(1300, 1900), 2)

    employees.append({
        'employee_id': i,
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'email': f'employee{i}@rental.by',
        'phone': f'+37529{random.randint(1000000, 9999999)}',
        'position': position,
        'hire_date': random_date(2019, 2024),
        'salary': salary,
        'location_id': random.randint(1, N_LOCATIONS),
        'is_active': random.choice(['true', 'false'])
    })
employees_df = pd.DataFrame(employees)


print("5. tariffs...")
tariffs = []
tariff_configs = {
    'Economy': {'base_price': (35, 60), 'km_included': 200, 'multiplier': 1.0},
    'Standard': {'base_price': (55, 90), 'km_included': 300, 'multiplier': 1.2},
    'Premium': {'base_price': (85, 140), 'km_included': 400, 'multiplier': 1.5},
    'Business': {'base_price': (120, 200), 'km_included': 500, 'multiplier': 2.0},
    'Luxury': {'base_price': (200, 350), 'km_included': 1000, 'multiplier': 3.0}
}

for i, (tariff_name, config) in enumerate(tariff_configs.items(), 1):
    if i > N_TARIFFS:
        break

    base_price = round(random.uniform(config['base_price'][0], config['base_price'][1]), 2)

    tariffs.append({
        'tariff_id': i,
        'tariff_name': tariff_name,
        'base_price_per_day': base_price,
        'km_included_per_day': config['km_included'],
        'additional_km_price': round(base_price * 0.02, 2),  # 2% от базовой цены за дополнительный км
        'deposit_amount': round(base_price * random.uniform(3, 7), 2),  # Залог 3-7 дней
        'min_rental_days': random.choice([1, 2, 3]),
        'max_rental_days': random.choice([30, 60]),
        'insurance_type': random.choice(['Basic', 'Standard', 'Full']),
        'cancellation_policy': random.choice(['Flexible', 'Standard', 'Strict'])
    })
tariffs_df = pd.DataFrame(tariffs)


print("6. rentals...")
rentals = []
for i in range(1, N_RENTALS + 1):
    rental_days = random.randint(1, 30)
    rental_date = random_date(2023, 2025)


    tariff = tariffs_df.sample(1).iloc[0]
    base_cost = round(tariff['base_price_per_day'] * rental_days, 2)


    km_driven = random.randint(50, rental_days * 300)
    additional_km = max(0, km_driven - (tariff['km_included_per_day'] * rental_days))
    additional_km_cost = round(additional_km * tariff['additional_km_price'], 2)


    total_cost = round(base_cost + additional_km_cost, 2)


    if random.random() < 0.02:
        total_cost = -abs(total_cost)


    deposit_amount = round(tariff['deposit_amount'] * random.uniform(0.8, 1.2), 2)


    deposit_returned = round(deposit_amount * random.uniform(0.8, 1.0), 2)
    if random.random() < 0.1:
        deposit_returned = round(deposit_amount * random.uniform(0.5, 0.8), 2)

    rentals.append({
        'rental_id': i,
        'car_id': random.randint(1, N_CARS),
        'customer_id': random.randint(1, N_CUSTOMERS),
        'employee_id': random.randint(1, N_EMPLOYEES),
        'tariff_id': tariff['tariff_id'],
        'pickup_location_id': random.randint(1, N_LOCATIONS),
        'return_location_id': random.randint(1, N_LOCATIONS),
        'rental_date': rental_date,
        'scheduled_return_date': (datetime.strptime(rental_date, '%Y-%m-%d') + timedelta(days=rental_days)).strftime(
            '%Y-%m-%d'),
        'actual_return_date': (datetime.strptime(rental_date, '%Y-%m-%d') + timedelta(
            days=rental_days + random.randint(-2, 3))).strftime('%Y-%m-%d'),
        'rental_days': rental_days,
        'km_driven': km_driven,
        'additional_km': additional_km,
        'base_cost': base_cost,
        'additional_km_cost': additional_km_cost,
        'total_cost': total_cost,
        'deposit_amount': deposit_amount,
        'deposit_returned': deposit_returned,
        'status': random.choice(['Completed', 'Active', 'Cancelled', 'No-show']),
        'notes': random.choice(['', 'Early return', 'Late return', 'Car dirty', 'No issues', 'Minor scratch'])
    })
rentals_df = pd.DataFrame(rentals)


print("7. payments...")
payments = []
payment_id = 1
for rental_id in range(1, N_RENTALS + 1):
    rental = rentals_df[rentals_df['rental_id'] == rental_id].iloc[0]
    total_amount = rental['total_cost'] + rental['deposit_amount']


    n_payments = random.randint(1, 3)

    remaining_amount = total_amount
    for payment_num in range(n_payments):
        if remaining_amount <= 0:
            break


        if payment_num == n_payments - 1:
            amount = round(remaining_amount, 2)
        else:
            max_part = remaining_amount * 0.8
            min_part = remaining_amount * 0.3
            amount = round(random.uniform(min_part, max_part), 2)

        remaining_amount -= amount


        if random.random() < 0.01:
            amount = -abs(amount)
        elif random.random() < 0.005:
            amount = 0

        payments.append({
            'payment_id': payment_id,
            'rental_id': rental_id,
            'payment_date': (datetime.strptime(rental['rental_date'], '%Y-%m-%d') +
                             timedelta(days=random.randint(0, 5))).strftime('%Y-%m-%d'),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Bank Transfer', 'Cash']),
            'amount': amount,
            'payment_type': 'Deposit' if payment_num == 0 and amount >= rental['deposit_amount'] * 0.8 else 'Rental',
            'status': random.choice(['Completed', 'Pending', 'Failed', 'Refunded'])
        })
        payment_id += 1
payments_df = pd.DataFrame(payments)


print("8. car_condition...")
car_condition = []
condition_id = 1


repair_costs = {
    'Scratch': (50, 300),
    'Dent': (200, 800),
    'Broken Window': (300, 1200),
    'Tire Damage': (100, 400),
    'Interior Stain': (80, 500),
    'Headlight Broken': (400, 1500),
    'Bumper Damage': (500, 2000),
    'Mirror Damage': (300, 1000),
    'No Damage': (0, 0)
}


maintenance_costs = {
    'Oil Change': (80, 200),
    'Tire Replacement': (400, 1200),
    'Brake Service': (300, 800),
    'Engine Repair': (1000, 5000),
    'Transmission Service': (1500, 7000),
    'Suspension Repair': (800, 3000),
    'Regular Maintenance': (200, 600),
    'Battery Replacement': (300, 800),
    'AC Service': (400, 1200)
}

for car_id in range(1, N_CARS + 1):
    n_entries = random.randint(1, 8)
    for _ in range(n_entries):
        entry_type = random.choice(['Damage', 'Maintenance'])

        if entry_type == 'Damage':
            description = random.choice(list(repair_costs.keys()))
            min_cost, max_cost = repair_costs[description]
        else:
            description = random.choice(list(maintenance_costs.keys()))
            min_cost, max_cost = maintenance_costs[description]


        cost = round(random.uniform(min_cost, max_cost), 2)


        if random.random() < 0.02:
            cost = -abs(cost)
        elif random.random() < 0.01 and cost > 0:
            cost = cost * random.uniform(5, 10)

        is_repaired = random.choice(['true', 'false']) if entry_type == 'Damage' else 'true'


        if is_repaired == 'true':
            entry_date = random_date(2023, 2025)
            repair_date = (datetime.strptime(entry_date, '%Y-%m-%d') +
                           timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d')
        else:
            entry_date = random_date(2023, 2025)
            repair_date = ''

        car_condition.append({
            'condition_id': condition_id,
            'car_id': car_id,
            'entry_date': entry_date,
            'entry_type': entry_type,
            'description': description,
            'cost': cost,
            'reported_by': random.choice(['Customer', 'Employee', 'System']),
            'is_repaired': is_repaired,
            'repair_date': repair_date,
            'notes': random.choice(['', 'Minor issue', 'Major repair required', 'Regular check', 'Warranty repair'])
        })
        condition_id += 1
car_condition_df = pd.DataFrame(car_condition)


car_condition_df['repair_date'] = car_condition_df['repair_date'].fillna('')
car_condition_df['notes'] = car_condition_df['notes'].fillna('')
payments_df = payments_df.fillna('')
rentals_df['notes'] = rentals_df['notes'].fillna('')


print(f"\nСохранение файлов в: {data_dir}")
files_to_save = [
    ('locations', locations_df),
    ('cars', cars_df),
    ('customers', customers_df),
    ('employees', employees_df),
    ('tariffs', tariffs_df),
    ('rentals', rentals_df),
    ('payments', payments_df),
    ('car_condition', car_condition_df)
]

for name, df in files_to_save:
    filename = os.path.join(data_dir, f'{name}.csv')
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"  ✓ {name}.csv: {len(df)} записей")










