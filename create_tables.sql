
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product (
    product_id STRING,
    product_name STRING
)
STORED AS PARQUET
LOCATION 's3://sales-etl-project-priya26/output/product_dimension/';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_sales (
    order_id INT,
    order_date DATE,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE,
    sales_amount DOUBLE
)
STORED AS PARQUET
LOCATION 's3://sales-etl-project-priya26/output/sales_fact/';
