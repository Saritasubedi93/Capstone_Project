# gold/gold_main.py

from pyspark.sql import SparkSession

from gold.gold_dim_patient import build_dim_patient
from gold.gold_dim_subscriber import build_dim_subscriber
from gold.gold_dim_group import build_dim_group
from gold.gold_dim_subgroup import build_dim_subgroup
from gold.gold_dim_disease import build_dim_disease
from gold.gold_bridge_group_subgroup import build_bridge_group_subgroup
from gold.gold_fact_claims import build_fact_claims
from gold.gold_dim_hospital import build_dim_hospital

def main():
    spark = (
        SparkSession.builder
        .appName("Capstone-Gold-Layer")
        .getOrCreate()
    )

    #build_dim_patient(spark)
    #build_dim_subscriber(spark)
    #build_dim_group(spark)
    #build_dim_subgroup(spark)
    #build_dim_disease(spark)
    #build_bridge_group_subgroup(spark)
    #build_fact_claims(spark)
    build_dim_hospital(spark)
    spark.stop()


if __name__ == "__main__":
    main()
