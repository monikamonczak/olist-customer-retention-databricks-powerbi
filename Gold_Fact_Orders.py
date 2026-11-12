# Databricks notebook source
# Databricks notebook source
# Gold layer: fact_orders (grain: ONE ROW PER ORDER)
from pyspark.sql import functions as F

orders = spark.read.table("olist_project.silver.orders")

customers = (spark.read.table("olist_project.silver.customers")
    .select(F.trim("customer_id").alias("customer_id"), "customer_unique_id", "customer_state"))

items = (spark.read.table("olist_project.silver.order_items")
    .withColumn("order_id", F.trim("order_id"))
    .groupBy("order_id")
    .agg(F.sum("price").alias("items_value"),
         F.sum("freight_value").alias("freight_value"),
         F.count("*").alias("items_count")))

reviews = (spark.read.table("olist_project.silver.order_reviews")
    .withColumn("order_id", F.trim("order_id"))
    .groupBy("order_id")
    .agg(F.avg("review_score").alias("review_score"),
         F.count("*").alias("reviews_count")))

fact_orders = (orders
    .withColumn("order_id", F.trim("order_id"))
    .withColumn("customer_id", F.trim("customer_id"))
    .join(customers, "customer_id", "inner")
    .join(items, "order_id", "left")
    .join(reviews, "order_id", "left")
    .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
    .withColumn("is_delivered", F.when(F.col("order_status") == "delivered", 1).otherwise(0))
    .withColumn("delay_days",
        F.when(F.col("order_status") == "delivered",
               F.datediff(F.to_date("order_delivered_customer_date"),
                          F.to_date("order_estimated_delivery_date"))))
    .withColumn("is_late",
        F.when(F.col("delay_days") > 0, 1).when(F.col("delay_days").isNotNull(), 0))
    .select("order_id", "customer_unique_id", "customer_state", "order_status", "is_delivered",
            F.col("order_purchase_timestamp").alias("purchase_ts"), "purchase_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "delay_days", "is_late",
            "items_value", "freight_value", "items_count", "review_score", "reviews_count"))

(fact_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.fact_orders"))

print("gold.fact_orders written, rows:", fact_orders.count())