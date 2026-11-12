# Databricks notebook source
# Databricks notebook source
# Gold layer: rfm_segments (grain: ONE ROW PER CUSTOMER, delivered orders only)
# Segment thresholds follow the observed return gaps: median 74 days, 75th percentile 175 days.
#   Loyal - active : returned on a later day, last purchase within 180 days
#   Loyal - lapsed : returned on a later day, last purchase older than 180 days
#   New            : one purchase day, within 90 days
#   At risk        : one purchase day, 91-180 days (still inside the typical return window)
#   Lost           : one purchase day, more than 180 days

from pyspark.sql import functions as F

dim = spark.read.table("olist_project.gold.dim_customers")

end_date = (spark.read.table("olist_project.gold.fact_orders")
            .filter(F.col("is_delivered") == 1)
            .agg(F.max("purchase_date")).first()[0])

reference_date = F.date_add(F.lit(end_date), 1)

rfm = (dim
    .select("customer_unique_id",
            F.datediff(reference_date, F.col("last_purchase_date")).alias("recency_days"),
            F.col("total_orders").alias("frequency"),
            "purchase_days",
            F.round("total_spent", 2).alias("monetary"))
    .withColumn("segment",
        F.when((F.col("purchase_days") > 1) & (F.col("recency_days") <= 180), "Loyal - active")
         .when(F.col("purchase_days") > 1, "Loyal - lapsed")
         .when(F.col("recency_days") <= 90, "New")
         .when(F.col("recency_days") <= 180, "At risk")
         .otherwise("Lost")))

(rfm.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.rfm_segments"))

print("gold.rfm_segments written, customers:", rfm.count())