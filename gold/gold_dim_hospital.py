# gold/gold_dim_hospital.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from common.s3_utils import silver_path, gold_path


def build_dim_hospital(spark: SparkSession) -> None:
    """
    Build hospital dimension from Silver hospital data.

    Silver schema (from silver_hospital.py):
      - hospital_id
      - Hospital_name
      - city
      - state
      - country
      - updated_timestamp
    """

    df = spark.read.parquet(silver_path("hospital"))

    dim = (
        df.select(
            col("hospital_id").alias("hospital_id"),
            col("Hospital_name").alias("hospital_name"),
            col("city"),
            col("state"),
            col("country")
        )

    )

    dim.write.mode("overwrite").parquet(gold_path("dim_hospital"))
    print("Gold dim_hospital written to", gold_path("dim_hospital"))
