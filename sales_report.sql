
SELECT
    p.product_id,
    p.product_name,
    SUM(f.quantity) AS total_quantity,
    SUM(f.sales_amount) AS total_sales
FROM fact_sales f
INNER JOIN dim_product p
    ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name
ORDER BY total_sales DESC;
