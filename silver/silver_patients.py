# silver/silver_patients.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_patients_silver(spark: SparkSession) -> None:
    # Read Bronze patients parquet
    df = spark.read.parquet(bronze_path("patients"))
    show_nulls(df, "patients bronze")

    # Clean and standardize
    df = (
        df.na.fill("NA", [
                "Patient_name",
                "patient_gender",
                "patient_birth_date",
                "patient_phone",
                "disease_name",
                "city",
                "hospital_id"
            ])
          .dropDuplicates(["Patient_id"])
          .withColumn("Patient_name", upper(col("Patient_name")))
          .withColumn("patient_gender", upper(col("patient_gender")))
          .withColumn("city", upper(col("city")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    # Write Silver
    df.write.mode("overwrite").parquet(silver_path("patients"))
    print("Silver patients written to", silver_path("patients"))
