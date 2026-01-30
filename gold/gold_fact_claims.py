# gold/gold_fact_claims.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    trim,
    upper,
    when,
)

from common.s3_utils import silver_path, gold_path


def build_fact_claims(spark: SparkSession) -> None:
    claims = spark.read.parquet(silver_path("claims"))
    patients = spark.read.parquet(silver_path("patients"))
    disease = spark.read.parquet(silver_path("disease"))
    subscriber = spark.read.parquet(silver_path("subscriber"))
    group_df = spark.read.parquet(silver_path("group"))
    subgroup = spark.read.parquet(silver_path("subgroup"))
    group_subgroup = spark.read.parquet(silver_path("group_subgroup"))

    # Standardize claim_status (if not already done in Silver)
    #I already did this in silver
    """
    claims = claims.withColumn(
        "claim_status",
        when(upper(trim(col("Claim_Or_Rejected"))) == "Y", "APPROVED")
        .when(upper(trim(col("Claim_Or_Rejected"))) == "N", "REJECTED")
        .otherwise("NOT_APPLICABLE"),
    )
"""
    fact = (
        claims.alias("c")
        .join(patients.alias("p"), col("c.patient_id") == col("p.Patient_id"), "left")
        .join(disease.alias("d"), col("c.disease_name") == col("d.Disease_name"), "left")
        .join(subscriber.alias("s"), col("c.SUB_ID") == col("s.sub_id"), "left")
        .join(subgroup.alias("sg"), col("d.SubGrpID") == col("sg.SubGrp_id"), "left")
        .join(group_subgroup.alias("gs"), col("sg.SubGrp_id") == col("gs.SubGrp_ID"), "left")
        .join(group_df.alias("g"), col("gs.Grp_Id") == col("g.Grp_Id"), "left")
    )

    fact_out = (
        fact.select(
            col("c.claim_id").cast("bigint"),
            col("c.patient_id").cast("bigint"),
            col("p.Patient_name").alias("patient_name"),
            col("p.patient_gender").cast("string").alias("patient_gender"),
            col("p.patient_birth_date").cast("date").alias("patient_birth_date"),
            col("p.city").alias("patient_city"),

            col("c.SUB_ID").cast("string").alias("sub_id"),

            col("d.Disease_ID").cast("int").alias("disease_id"),
            col("d.Disease_name").alias("disease_name"),

            col("sg.SubGrp_id").cast("string").alias("subgrp_id"),
            col("sg.SubGrp_Name").alias("subgrp_name"),
            col("sg.Monthly_Premium").cast("double").alias("monthly_premium"),

            col("g.Grp_Id").cast("string").alias("grp_id"),
            col("g.Grp_Name").alias("grp_name"),
            col("g.Grp_Type").alias("grp_type"),
            col("g.Country").alias("grp_country"),
            col("g.city").alias("grp_city"),
            col("g.zipcode").cast("int").alias("grp_zipcode"),
            col("g.year").cast("int").alias("grp_year"),
            col("g.premium_written").cast("double").alias("premium_written"),

            col("c.claim_type"),
            col("c.claim_amount").cast("double").alias("claim_amount"),
            col("c.claim_date").cast("date").alias("claim_date"),
            col("c.claim_status"),

            lit(1).cast("int").alias("claim_count"),
            current_timestamp().alias("updated_timestamp"),
        )
    )

    fact_out.write.mode("overwrite").parquet(gold_path("fact_claims"))
    print("Gold fact_claims written to", gold_path("fact_claims"))
