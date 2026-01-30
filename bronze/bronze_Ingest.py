from common.spark_utils import create_spark
from common.s3_utils import input_path, bronze_path
from config.settings import APP_NAME_BRONZE
from pyspark.sql.functions import input_file_name, current_timestamp

def ingest_csv_dataset(spark, name: str, filename: str):
    src_path = input_path(filename)  # no wildcard
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(src_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    df.write.mode("overwrite").parquet(bronze_path(name))
    print(f"Wrote bronze {name} from {src_path}")

def ingest_json_dataset(spark, name: str, filename: str):
    src_path = input_path(filename)
    df = (
        spark.read
        .json(src_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    df.write.mode("overwrite").parquet(bronze_path(name))
    print(f"Wrote bronze {name} from {src_path}")

def main():
    spark = create_spark(APP_NAME_BRONZE)

    ingest_csv_dataset(spark, "patients", "Patient_records.csv")
    ingest_json_dataset(spark, "claims", "claims.json")
    ingest_csv_dataset(spark, "disease", "disease.csv")
    ingest_csv_dataset(spark, "group", "group.csv")
    ingest_csv_dataset(spark, "group_subgroup", "grpsubgrp.csv")
    ingest_csv_dataset(spark, "hospital", "hospital.csv")
    ingest_csv_dataset(spark, "subscriber", "subscriber.csv")
    ingest_csv_dataset(spark, "subgroup", "subgroup.csv")

    spark.stop()

if __name__ == "__main__":
    main()
