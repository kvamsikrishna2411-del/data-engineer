from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =========================
# BRONZE
# =========================

@dp.table
def bronze_customer():

    source_path = "/Volumes/workspace/default/my_volume/customers-100.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )

    for col_name in df.columns:
        df = df.withColumnRenamed(
            col_name,
            col_name.strip().replace(" ", "_")
        )

    return df


# =========================
# SILVER
# =========================

@dp.table
def silver_customer():

    bronze_df = spark.read.table("bronze_customer")

    silver_df = (
        bronze_df
        .dropDuplicates()
        .filter(F.col("Customer_Id").isNotNull())
        .withColumn("First_Name", F.trim(F.col("First_Name")))
        .withColumn("Last_Name", F.trim(F.col("Last_Name")))
        .withColumn("City", F.trim(F.col("City")))
        .withColumn("Country", F.trim(F.col("Country")))
        .withColumn("Email", F.trim(F.col("Email")))
    )

    return silver_df


# =========================
# GOLD
# =========================

@dp.table
def gold_customer_country():

    silver_df = spark.read.table("silver_customer")

    gold_df = (
        silver_df
        .groupBy("Country")
        .agg(
            F.count("*").alias("customer_count")
        )
    )

    return gold_df