
import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ==========================================
# 1. INITIALIZE AWS GLUE
# ==========================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("AWS GLUE JOB STARTED")


# ==========================================
# 2. S3 BUCKET CONFIGURATION
# ==========================================

source_path = (
    "s3://sales-etl-project-priya26/input/sales_data.csv"
)

fact_path = (
    "s3://sales-etl-project-priya26/output/sales_fact/"
)

dimension_path = (
    "s3://sales-etl-project-priya26/output/product_dimension/"
)

print("SOURCE PATH:", repr(source_path))
print("FACT PATH:", repr(fact_path))
print("DIMENSION PATH:", repr(dimension_path))


# ==========================================
# 3. DEFINE SOURCE CSV SCHEMA
# ==========================================

schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True)
])


# ==========================================
# 4. EXTRACT DATA FROM AMAZON S3
# ==========================================

print("EXTRACTING SALES DATA...")

sales_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(schema)
    .load(source_path)
)

print("CSV DATA LOADED SUCCESSFULLY")

sales_df.show(5, truncate=False)

total_records = sales_df.count()

print("TOTAL SOURCE RECORDS:", total_records)


# ==========================================
# 5. TRANSFORM SALES DATA
# ==========================================

print("STARTING DATA TRANSFORMATION...")

sales_df = (
    sales_df
    .withColumn(
        "order_date",
        F.to_date(F.col("OrderDate"), "yyyy-MM-dd")
    )
    .withColumn(
        "sales_amount",
        F.col("Quantity") * F.col("UnitPrice")
    )
)


# ==========================================
# 6. FILTER ZERO-DOLLAR SALES
# ==========================================

sales_df = sales_df.filter(
    (F.col("sales_amount") > 0) &
    F.col("order_date").isNotNull() &
    F.col("OrderID").isNotNull() &
    F.col("ProductID").isNotNull() &
    F.col("ProductName").isNotNull() &
    F.col("Quantity").isNotNull() &
    F.col("UnitPrice").isNotNull() &
    (F.col("Quantity") > 0)
)

sales_df = sales_df.cache()

valid_records = sales_df.count()

print("VALID SALES RECORDS:", valid_records)
print("FILTERED RECORDS:", total_records - valid_records)


# ==========================================
# 7. CREATE PRODUCT DIMENSION TABLE
# ==========================================

print("CREATING PRODUCT DIMENSION TABLE...")

dim_product = (
    sales_df
    .select(
        F.col("ProductID").alias("product_id"),
        F.col("ProductName").alias("product_name")
    )
    .dropDuplicates(["product_id"])
)

print("PRODUCT DIMENSION DATA:")

dim_product.show(truncate=False)


# ==========================================
# 8. CREATE SALES FACT TABLE
# ==========================================

print("CREATING SALES FACT TABLE...")

fact_sales = sales_df.select(
    F.col("OrderID").alias("order_id"),
    F.col("order_date"),
    F.col("ProductID").alias("product_id"),
    F.col("Quantity").alias("quantity"),
    F.col("UnitPrice").alias("unit_price"),
    F.col("sales_amount")
)

print("SALES FACT DATA:")

fact_sales.show(truncate=False)


# ==========================================
# 9. LOAD DATA INTO AMAZON S3
# ==========================================

print("SAVING PRODUCT DIMENSION...")

dim_product.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(dimension_path)

print("PRODUCT DIMENSION SAVED SUCCESSFULLY")


print("SAVING SALES FACT...")

fact_sales.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(fact_path)

print("SALES FACT SAVED SUCCESSFULLY")


# ==========================================
# 10. FINAL DATA VALIDATION
# ==========================================

total_quantity = fact_sales.agg(
    F.sum("quantity").alias("total_quantity")
).first()["total_quantity"]

total_sales = fact_sales.agg(
    F.sum("sales_amount").alias("total_sales")
).first()["total_sales"]

print("===================================")
print("ETL PIPELINE COMPLETED SUCCESSFULLY")
print("===================================")

print("TOTAL SOURCE RECORDS:", total_records)
print("VALID TRANSACTIONS:", valid_records)
print("FILTERED RECORDS:", total_records - valid_records)
print("TOTAL QUANTITY:", total_quantity)
print("TOTAL SALES:", total_sales)

print("===================================")


# ==========================================
# 11. CLEANUP AND COMMIT
# ==========================================

sales_df.unpersist()

job.commit()

print("AWS GLUE JOB COMMITTED SUCCESSFULLY")
