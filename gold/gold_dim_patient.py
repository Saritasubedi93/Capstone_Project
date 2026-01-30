# gold/gold_dim_patient.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import silver_path, gold_path


def build_dim_patient(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("patients"))

    dim = (
        df.select(
            col("Patient_id").alias("patient_id"),
            col("Patient_name").alias("patient_name"),
            col("patient_gender").alias("gender"),
            col("patient_birth_date").alias("birth_date"),
            col("patient_phone").alias("phone"),
            col("city"),
            col("hospital_id"),
        )
        .withColumn("gender", upper(col("gender")))
        .withColumn("updated_timestamp", current_timestamp())
        .dropDuplicates(["patient_id"])
    )

    dim.write.mode("overwrite").parquet(gold_path("dim_patient"))
    print("Gold dim_patient written to", gold_path("dim_patient"))
