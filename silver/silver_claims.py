# silver/silver_claims.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    regexp_replace,
    when,
    to_date,
    current_timestamp
)

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_claims_silver(spark: SparkSession) -> None:
    # 1. Read Bronze claims (created from claims.json)
    df = spark.read.parquet(bronze_path("claims"))
    show_nulls(df, "claims bronze")

    # 2. Basic cleaning: fill nulls, trim spaces
    df = (
        df.na.fill("NA", ["disease_name", "SUB_ID", "Claim_Or_Rejected", "claim_type"])
          .na.fill("0", ["claim_amount"])   # keep string, convert to numeric next
          .withColumn("disease_name", upper(trim(col("disease_name"))))
          .withColumn("SUB_ID", upper(trim(col("SUB_ID"))))
          .withColumn("claim_type", upper(trim(col("claim_type"))))
    )

    # 3. Standardize claim_status from Claim_Or_Rejected
    # Original values: 'Y', 'N', 'NaN' (string)
    df = df.withColumn(
        "claim_status",
        when(upper(trim(col("Claim_Or_Rejected"))) == "Y", "APPROVED")
        .when(upper(trim(col("Claim_Or_Rejected"))) == "N", "REJECTED")
        .otherwise("UNKNOWN")
    )

    # 4. Convert claim_amount to numeric
    # Remove any non-digit characters just in case (e.g., commas)
    df = df.withColumn(
        "claim_amount_clean",
        regexp_replace(col("claim_amount"), "[^0-9.]", "")
    ).withColumn(
        "claim_amount_num",
        col("claim_amount_clean").cast("double")
    )

    # 5. Parse claim_date as proper date
    df = df.withColumn(
        "claim_date_parsed",
        to_date(col("claim_date"), "yyyy-MM-dd")
    )

    # 6. Drop duplicates and add audit fields
    df = (
        df.dropDuplicates(["claim_id"])
          .withColumn("updated_timestamp", current_timestamp())
    )

    # 7. Select and rename columns for Silver output
    silver_df = df.select(
        col("claim_id"),
        col("patient_id"),
        col("SUB_ID").alias("sub_id"),
        col("disease_name"),
        col("claim_type"),
        col("claim_status"),
        col("claim_amount_num").alias("claim_amount"),
        col("claim_date_parsed").alias("claim_date"),
        col("updated_timestamp")
    )

    # 8. Write Silver claims
    silver_df.write.mode("overwrite").parquet(silver_path("claims"))
    print("Silver claims written to", silver_path("claims"))
