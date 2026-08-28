# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

CATALOG_NAME = "diabetes_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_patient = (
    spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.patient")
    .select(
        F.col("Patient_ID").cast(DoubleType()).cast(IntegerType()).alias("patient_id"),
        F.col("Age").cast(DoubleType()).cast(IntegerType()).alias("age"),
        F.coalesce(F.col("Gender"), F.lit("Unknown")).alias("gender"),
        F.coalesce(F.col("Country"), F.lit("Unknown")).alias("country"),
        F.coalesce(F.col("Work_Type"), F.lit("Unknown")).alias("work_type"),
        F.coalesce(F.col("Residence_Type"), F.lit("Unknown")).alias("residence_type"),
        F.col("Family_History_Diabetes").alias("family_history_diabetes"),
        F.col("Hypertension").alias("hypertension"),
        F.col("Heart_Disease").alias("heart_disease"),
        F.col("Fatty_Liver").alias("fatty_liver"),
        F.col("PCOS").alias("pcos"),
    )
    .dropDuplicates(["patient_id"])
)

target_table_path = f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_patient"

if spark.catalog.tableExists(target_table_path):
    (
        DeltaTable.forName(spark, target_table_path)
        .alias("target")
        .merge(
            bronze_patient.alias("source"),
            "target.patient_id = source.patient_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    bronze_patient.write.format("delta").saveAsTable(target_table_path)

print(f"Successfully processed {target_table_path}")
