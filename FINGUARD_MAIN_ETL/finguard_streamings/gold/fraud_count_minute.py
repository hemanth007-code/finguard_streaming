from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(name="finguard.gold.fraud_count_minutes")
def get_fraud_count_minutes() -> DataFrame:

    transactions=spark.read.table("finguard.silver.transactions_silver")

    transactions_count_df=transactions.groupBy(
                        F.window("transaction_timestamp", "1 minutes")
                        ).agg(F.count("*").alias("transaction_count")).select(

                            F.col("window.start").alias("start"),
                            F.col("window.end").alias("end"),
                            F.col("transaction_count").alias("transaction_count"),
                        )
    return transactions_count_df