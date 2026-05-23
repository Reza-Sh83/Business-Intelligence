SELECT
    p.category,
    d.year,
    d.month,
    SUM(f.line_amount) AS total_sales
FROM fact_sales f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY p.category, d.year, d.month
ORDER BY d.year, d.month, total_sales DESC;

SELECT
    s.store_name,
    SUM(f.quantity) AS total_items_sold
FROM fact_sales f
JOIN dim_store s ON f.store_sk = s.store_sk
WHERE s.is_current   -- only current store name
GROUP BY s.store_name
ORDER BY total_items_sold DESC
LIMIT 10;

SELECT
    t.part_of_day,
    COUNT(DISTINCT f.ticket_id) AS number_of_tickets,
    SUM(f.line_amount) AS total_revenue
FROM fact_sales f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.part_of_day
ORDER BY total_revenue DESC;