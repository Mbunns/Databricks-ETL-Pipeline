from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="myproject.gold.gold_daily_transaction_metrics",
    comment="Daily aggregated transaction metrics including revenue, volume, and breakdowns by category, location, and payment method",
    cluster_by=["transaction_date"]
)
def gold_daily_transaction_metrics():
    return (
        spark.read.table("silver_transactions")
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .groupBy("transaction_date")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.sum("quantity").alias("total_items_sold"),
            
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products"),
            
            F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_transactions"),
            F.sum(F.when(F.col("category") == "Electronics", F.col("total_amount"))).alias("electronics_revenue"),
            
            F.count(F.when(F.col("category") == "Apparel", 1)).alias("apparel_transactions"),
            F.sum(F.when(F.col("category") == "Apparel", F.col("total_amount"))).alias("apparel_revenue"),
            
            F.count(F.when(F.col("category") == "Accessories", 1)).alias("accessories_transactions"),
            F.sum(F.when(F.col("category") == "Accessories", F.col("total_amount"))).alias("accessories_revenue"),
            
            F.approx_count_distinct("store_location").alias("active_store_locations"),
            
            F.count(F.when(F.col("payment_method") == "Credit Card", 1)).alias("credit_card_transactions"),
            F.count(F.when(F.col("payment_method") == "Debit Card", 1)).alias("debit_card_transactions"),
            F.count(F.when(F.col("payment_method") == "Cash", 1)).alias("cash_transactions"),
            F.count(F.when(F.col("payment_method") == "Apple Pay", 1)).alias("apple_pay_transactions"),
            
            F.first(F.struct(
                F.col("total_amount").alias("revenue"),
                F.col("product_id"),
                F.col("product_name")
            )).alias("top_product_struct")
        )
        .withColumn("top_product_id", F.col("top_product_struct.product_id"))
        .withColumn("top_product_name", F.col("top_product_struct.product_name"))
        .withColumn("top_product_revenue", F.col("top_product_struct.revenue"))
        .drop("top_product_struct")
        .orderBy("transaction_date")
    )
