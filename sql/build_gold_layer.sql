

CREATE SCHEMA IF NOT EXISTS gold;



DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    month_name TEXT,
    day_of_week INTEGER,
    day_type TEXT
);


DROP TABLE IF EXISTS gold.dim_car CASCADE;
CREATE TABLE gold.dim_car (
    car_key INTEGER PRIMARY KEY,
    car_id INTEGER,
    brand TEXT,
    model TEXT,
    year INTEGER,
    color TEXT,
    license_plate TEXT,
    vin TEXT,
    status TEXT,
    daily_rental_price DECIMAL,
    current_location_id INTEGER,
    purchase_date DATE,
    purchase_price DECIMAL
);


DROP TABLE IF EXISTS gold.dim_client CASCADE;
CREATE TABLE gold.dim_client (
    client_key INTEGER PRIMARY KEY,
    customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    city TEXT,
    loyalty_level TEXT,
    date_of_birth DATE,
    driving_license TEXT,
    license_issue_date DATE,
    license_expiry_date DATE,
    registration_date DATE
);


DROP TABLE IF EXISTS gold.dim_employee CASCADE;
CREATE TABLE gold.dim_employee (
    employee_key INTEGER PRIMARY KEY,
    employee_id INTEGER,
    full_name TEXT,
    email TEXT,
    position TEXT,
    hire_date DATE,
    is_active BOOLEAN
);


DROP TABLE IF EXISTS gold.dim_location CASCADE;
CREATE TABLE gold.dim_location (
    location_key INTEGER PRIMARY KEY,
    location_id INTEGER,
    city TEXT,
    address TEXT,
    opening_hours TEXT,
    is_active BOOLEAN
);


DROP TABLE IF EXISTS gold.dim_tariff CASCADE;
CREATE TABLE gold.dim_tariff (
    tariff_key INTEGER PRIMARY KEY,
    tariff_id INTEGER,
    tariff_name TEXT,
    base_price_per_day DECIMAL,
    km_included_per_day INTEGER,
    additional_km_price DECIMAL,
    min_rental_days INTEGER,
    insurance_type TEXT,
    cancellation_policy TEXT
);


INSERT INTO gold.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    TO_CHAR(d, 'TMMonth'),
    EXTRACT(DOW FROM d),
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') d
ON CONFLICT (date_key) DO NOTHING;


INSERT INTO gold.dim_car
SELECT
    ROW_NUMBER() OVER (ORDER BY car_id),
    car_id,
    brand,
    model,
    year,
    color,
    license_plate,
    vin,
    status,
    daily_rental_price,
    current_location_id,
    purchase_date,
    purchase_price
FROM silver.cars
ON CONFLICT (car_key) DO NOTHING;


INSERT INTO gold.dim_client
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id),
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name),
    email,
    phone,
    city,
    loyalty_level,
    date_of_birth,
    driving_license,
    license_issue_date,
    license_expiry_date,
    registration_date
FROM silver.customers
ON CONFLICT (client_key) DO NOTHING;


INSERT INTO gold.dim_employee
SELECT
    ROW_NUMBER() OVER (ORDER BY employee_id),
    employee_id,
    CONCAT(first_name, ' ', last_name),
    email,
    position,
    hire_date,
    is_active
FROM silver.employees
ON CONFLICT (employee_key) DO NOTHING;


INSERT INTO gold.dim_location
SELECT
    ROW_NUMBER() OVER (ORDER BY location_id),
    location_id,
    city,
    address,
    opening_hours,
    is_active
FROM silver.locations
ON CONFLICT (location_key) DO NOTHING;


INSERT INTO gold.dim_tariff
SELECT
    ROW_NUMBER() OVER (ORDER BY tariff_id),
    tariff_id,
    tariff_name,
    base_price_per_day,
    km_included_per_day,
    additional_km_price,
    min_rental_days,
    insurance_type,
    cancellation_policy
FROM silver.tariffs
ON CONFLICT (tariff_key) DO NOTHING;



DROP TABLE IF EXISTS gold.fact_rentals CASCADE;
CREATE TABLE gold.fact_rentals (
    rental_id INTEGER PRIMARY KEY,

    car_key INTEGER REFERENCES gold.dim_car(car_key),
    client_key INTEGER REFERENCES gold.dim_client(client_key),
    employee_key INTEGER REFERENCES gold.dim_employee(employee_key),
    tariff_key INTEGER REFERENCES gold.dim_tariff(tariff_key),
    pickup_location_key INTEGER REFERENCES gold.dim_location(location_key),
    return_location_key INTEGER REFERENCES gold.dim_location(location_key),
    rental_date_key INTEGER REFERENCES gold.dim_date(date_key),
    actual_return_date_key INTEGER REFERENCES gold.dim_date(date_key),


    rental_days INTEGER,
    total_cost DECIMAL,
    base_cost DECIMAL,
    additional_km_cost DECIMAL,
    deposit_returned DECIMAL,
    status TEXT,
    km_driven INTEGER,
    additional_km INTEGER,
    scheduled_return_date DATE,
    is_late_return BOOLEAN,
    late_days INTEGER,
    rental_date DATE,
    actual_return_date DATE,
    duration_days INTEGER,
    total_paid DECIMAL,
    payment_count INTEGER,
    unique_payment_methods INTEGER
);


INSERT INTO gold.fact_rentals
SELECT
    r.rental_id,
    dc.car_key,
    dcl.client_key,
    de.employee_key,
    dt.tariff_key,
    dl_pickup.location_key,
    dl_return.location_key,
    dd_rental.date_key,
    dd_actual.date_key,
    r.rental_days,
    r.total_cost,
    r.base_cost,
    r.additional_km_cost,
    COALESCE(r.deposit_returned, 0),
    r.status,
    r.km_driven,
    r.additional_km,
    r.scheduled_return_date,
    r.is_late_return,
    r.late_days,
    r.rental_date,
    r.actual_return_date,
    (r.actual_return_date - r.rental_date)::INTEGER AS duration_days,
    COALESCE(payments.total_paid, 0),
    COALESCE(payments.payment_count, 0),
    COALESCE(payments.unique_payment_methods, 0)
FROM silver.rentals r
LEFT JOIN gold.dim_car dc ON r.car_id = dc.car_id
LEFT JOIN gold.dim_client dcl ON r.customer_id = dcl.customer_id
LEFT JOIN gold.dim_employee de ON r.employee_id = de.employee_id
LEFT JOIN gold.dim_tariff dt ON r.tariff_id = dt.tariff_id
LEFT JOIN gold.dim_location dl_pickup ON r.pickup_location_id = dl_pickup.location_id
LEFT JOIN gold.dim_location dl_return ON r.return_location_id = dl_return.location_id
LEFT JOIN gold.dim_date dd_rental ON TO_CHAR(r.rental_date, 'YYYYMMDD')::INT = dd_rental.date_key
LEFT JOIN gold.dim_date dd_actual ON TO_CHAR(r.actual_return_date, 'YYYYMMDD')::INT = dd_actual.date_key
LEFT JOIN (
    SELECT
        rental_id,
        SUM(amount) AS total_paid,
        COUNT(*) AS payment_count,
        COUNT(DISTINCT payment_method) AS unique_payment_methods
    FROM silver.payments
    WHERE status = 'Completed'
    GROUP BY rental_id
) payments ON r.rental_id = payments.rental_id;


CREATE INDEX IF NOT EXISTS idx_fact_rentals_car ON gold.fact_rentals(car_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_client ON gold.fact_rentals(client_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_employee ON gold.fact_rentals(employee_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_tariff ON gold.fact_rentals(tariff_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_pickup_location ON gold.fact_rentals(pickup_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_return_location ON gold.fact_rentals(return_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_rental_date ON gold.fact_rentals(rental_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_rentals_actual_return ON gold.fact_rentals(actual_return_date_key);



