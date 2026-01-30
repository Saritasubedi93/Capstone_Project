# silver/silver_main.py

from common.spark_utils import create_spark
from config.settings import APP_NAME_SILVER

from silver.silver_patients import run_patients_silver
from silver.silver_suscribers import run_subscribers_silver
from silver.silver_group import run_group_silver
from silver.silver_subgroup import run_subgroup_silver
from silver.silver_group_subgroup import run_group_subgroup_silver
from silver.silver_disease import run_disease_silver
from silver.silver_hospital import run_hospital_silver
from silver.silver_claims import run_claims_silver


def main():
    # Create Spark session with S3A configured
    spark = create_spark(APP_NAME_SILVER)

    # Run all Silver transformations (order doesn’t matter much here)
    run_patients_silver(spark)
    run_subscribers_silver(spark)
    run_group_silver(spark)
    run_subgroup_silver(spark)
    run_group_subgroup_silver(spark)
    run_disease_silver(spark)
    run_hospital_silver(spark)
    run_claims_silver(spark)

    spark.stop()


if __name__ == "__main__":
    main()
