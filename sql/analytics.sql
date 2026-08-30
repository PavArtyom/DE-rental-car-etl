-- Аналитические запросы для Metabase.
-- total_cost — начисленная стоимость аренды;
-- total_paid — сумма завершённых платежей.

-- 1. Аналитика тарифных планов
SELECT
    tariff.tariff_name,
    tariff.base_price_per_day,
    COUNT(*) AS rentals_count,
    ROUND(SUM(rental.total_cost), 2) AS total_revenue,
    ROUND(SUM(rental.total_paid), 2) AS received_payments
FROM gold.fact_rentals AS rental
JOIN gold.dim_tariff AS tariff
    ON tariff.tariff_key = rental.tariff_key
GROUP BY tariff.tariff_name, tariff.base_price_per_day
ORDER BY total_revenue DESC;

-- 2. Топ-10 автомобилей по начисленной выручке
SELECT
    car.brand,
    car.model,
    car.license_plate,
    car.daily_rental_price,
    COUNT(*) AS rentals_count,
    ROUND(SUM(rental.total_cost), 2) AS total_revenue
FROM gold.fact_rentals AS rental
JOIN gold.dim_car AS car
    ON car.car_key = rental.car_key
GROUP BY
    car.brand,
    car.model,
    car.license_plate,
    car.daily_rental_price
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Самые популярные марки автомобилей
SELECT
    car.brand,
    COUNT(*) AS rentals_count
FROM gold.fact_rentals AS rental
JOIN gold.dim_car AS car
    ON car.car_key = rental.car_key
GROUP BY car.brand
ORDER BY rentals_count DESC;

-- 4. Топ-10 клиентов по начисленной выручке
SELECT
    client.full_name,
    client.city,
    client.loyalty_level,
    COUNT(*) AS rentals_count,
    ROUND(SUM(rental.total_cost), 2) AS total_revenue
FROM gold.fact_rentals AS rental
JOIN gold.dim_client AS client
    ON client.client_key = rental.client_key
GROUP BY client.full_name, client.city, client.loyalty_level
ORDER BY total_revenue DESC
LIMIT 10;

-- 5. География клиентов по количеству аренд
SELECT
    client.city AS client_city,
    COUNT(*) AS rentals_count
FROM gold.fact_rentals AS rental
JOIN gold.dim_client AS client
    ON client.client_key = rental.client_key
GROUP BY client.city
ORDER BY rentals_count DESC;

-- 6. Количество аренд по месяцам
SELECT
    DATE_TRUNC('month', rental.rental_date)::DATE AS month,
    COUNT(*) AS rentals_count
FROM gold.fact_rentals AS rental
GROUP BY DATE_TRUNC('month', rental.rental_date)
ORDER BY month;

-- 7. Динамика начисленной выручки по месяцам
SELECT
    DATE_TRUNC('month', rental.rental_date)::DATE AS month,
    ROUND(SUM(rental.total_cost), 2) AS total_revenue
FROM gold.fact_rentals AS rental
GROUP BY DATE_TRUNC('month', rental.rental_date)
ORDER BY month;

-- 8. Эффективность менеджеров
SELECT
    employee.full_name AS employee_name,
    COUNT(*) AS rentals_count,
    ROUND(SUM(rental.total_cost), 2) AS total_revenue,
    ROUND(AVG(rental.total_cost), 2) AS average_rental_value
FROM gold.fact_rentals AS rental
JOIN gold.dim_employee AS employee
    ON employee.employee_key = rental.employee_key
GROUP BY employee.full_name
ORDER BY total_revenue DESC;

-- 9. Сравнение месячной выручки за 2023–2025 годы
SELECT
    EXTRACT(MONTH FROM rental.rental_date)::INTEGER AS month_number,
    TO_CHAR(rental.rental_date, 'TMMonth') AS month_name,
    ROUND(SUM(rental.total_cost) FILTER (
        WHERE EXTRACT(YEAR FROM rental.rental_date) = 2023
    ), 2) AS revenue_2023,
    ROUND(SUM(rental.total_cost) FILTER (
        WHERE EXTRACT(YEAR FROM rental.rental_date) = 2024
    ), 2) AS revenue_2024,
    ROUND(SUM(rental.total_cost) FILTER (
        WHERE EXTRACT(YEAR FROM rental.rental_date) = 2025
    ), 2) AS revenue_2025
FROM gold.fact_rentals AS rental
WHERE rental.rental_date >= DATE '2023-01-01'
  AND rental.rental_date < DATE '2026-01-01'
GROUP BY
    EXTRACT(MONTH FROM rental.rental_date),
    TO_CHAR(rental.rental_date, 'TMMonth')
ORDER BY month_number;
