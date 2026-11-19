from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="finguard.gold.high_value_transactions_alert",
    comment="Transactions where the amount exceeds the customer's transaction limit"
)
def high_value_transactions_alert() -> DataFrame:

    # Silver transactions is a batch view/materialized view
    t = (
        spark.read
        .table("finguard.silver.transactions_silver")
        .alias("t")
    )

    # Customers is also read as batch
    c = (
        spark.read
        .table("finguard.silver.customers")
        .alias("c")
    )

    return (
        t.join(
            c,
            F.col("t.customer_id") == F.col("c.customer_id"),
            "inner"
        )
        .filter(
            F.col("c.transaction_limit").isNotNull()
            &
            (
                F.col("t.amount").cast("decimal(18,2)")
                >
                F.col("c.transaction_limit").cast("decimal(18,2)")
            )
        )
        .select(

            F.concat_ws(
                "-",
                F.lit("ALERT"),
                F.col("t.transaction_id")
            ).alias("alert_id"),

            F.lit(
                "High Value Transaction"
            ).alias("alert_type"),

            F.current_timestamp().alias("alert_timestamp"),

            F.col("t.transaction_id"),

            F.col("t.customer_id"),

            F.concat_ws(
                " ",
                F.col("c.first_name"),
                F.col("c.last_name")
            ).alias("customer_name"),

            F.col("c.email").alias("customer_email"),

            F.col("t.amount")
            .cast("decimal(18,2)")
            .alias("transaction_amount"),

            F.col("c.transaction_limit")
            .cast("decimal(18,2)")
            .alias("transaction_limit"),

            (
                F.col("t.amount").cast("decimal(18,2)")
                -
                F.col("c.transaction_limit").cast("decimal(18,2)")
            ).alias("amount_over_limit"),

            F.to_date(
                F.col("t.transaction_timestamp")
            ).alias("transaction_date"),

            F.col("t.transaction_timestamp"),

            F.col("t.merchant_name"),

            F.col("t.currency"),

            F.col("t.transaction_type"),

            F.col("t.payment_channel"),

            F.col("t.is_international"),

            F.col("t.country"),

            F.col("t.city"),

            F.col("t.status")
        )
    )