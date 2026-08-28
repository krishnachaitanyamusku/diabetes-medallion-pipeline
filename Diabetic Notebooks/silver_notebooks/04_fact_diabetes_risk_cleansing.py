# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

CATALOG_NAME = "diabetes_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_risk = (
    spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.diabetes_risk")
    .select(
        F.col("Patient_ID").cast(IntegerType()).alias("patient_id"),
        F.col("snapshot_date").cast(DateType()).alias("snapshot_date"),
        F.col("Diabetes_Risk_Score").cast(IntegerType()).alias("diabetes_risk_score"),
        F.col("AI_Health_Recommendation").alias("ai_health_recommendation"),
        F.col("Doctor_Consultation_Needed").alias("doctor_consultation_needed"),
        F.col("Diabetes_Risk").alias("diabetes_risk"),
    )
    .dropDuplicates(["patient_id", "snapshot_date"])
)

target_table_path = f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_diabetes_risk"

if spark.catalog.tableExists(target_table_path):
    (
        DeltaTable.forName(spark, target_table_path)
        .alias("target")
        .merge(
            bronze_risk.alias("source"),
            "target.patient_id = source.patient_id AND target.snapshot_date = source.snapshot_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    bronze_risk.write.format("delta").saveAsTable(target_table_path)

print(f"Successfully processed {target_table_path}")
