from pyspark import pipelines as dp
from pyspark.sql import functions as F


dp.create_streaming_table(
    name="silver_transactions",
    comment="Unified and cleaned transaction data from GDrive and S3 sources"
)

@dp.append_flow(
    target="silver_transactions",
    name="s3_transactions",
    comment="Stream cleaned transactions from S3 bronze table"
)
def s3_transactions():
    return (
        spark.readStream.table("myproject.bronze.bronze_s3_transactions")
        .withColumn("product_name", F.trim(F.lower(F.col("product_name"))))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("store_location", F.trim(F.col("store_location")))
        .withColumn("payment_method", F.trim(F.col("payment_method")))
        .select(
            "transaction_id",
            "transaction_date",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "store_location",
            "payment_method"
        )
        .dropDuplicates(["transaction_id"])
    )

@dp.append_flow(
    target="silver_transactions",
    name="gdrive_transactions",
    comment="Stream cleaned transactions from GDrive bronze table with type casting"
)
def gdrive_transactions():
    return (
        spark.readStream.option("ignoreDeletes", "true").table("myproject.bronze.bronze_gdrive_transactions")
        .withColumn("transaction_date", F.col("transaction_date").cast("timestamp"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("product_name", F.trim(F.lower(F.col("product_name"))))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("store_location", F.trim(F.col("store_location")))
        .withColumn("payment_method", F.trim(F.col("payment_method")))
        .select(
            "transaction_id",
            "transaction_date",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "store_location",
            "payment_method"
        )
        .dropDuplicates(["transaction_id"])
    )
