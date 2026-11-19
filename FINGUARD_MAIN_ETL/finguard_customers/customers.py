from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(name="finguard.silver.customers")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customer_silver():
    # Read as batch: finguard.bronze.customers receives MERGE updates, which
    # streaming sources do not allow. A batch read makes this a materialized view.
    bronze_df = spark.read.table("finguard.bronze.customers")
    transformed_df = (
        bronze_df
        .withColumn("account_open_date", F.col("account_open_date").cast("date"))
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )
    return transformed_df