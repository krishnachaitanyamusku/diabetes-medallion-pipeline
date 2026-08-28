# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

CATALOG_NAME = "diabetes_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_lifestyle = (
    spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.lifestyle")
    .select(
        F.col("Patient_ID").cast(IntegerType()).alias("patient_id"),
        F.col("snapshot_date").cast(DateType()).alias("snapshot_date"),
        F.coalesce(F.col("Physical_Activity_Level"), F.lit("Unknown")).alias("physical_activity_level"),
        F.coalesce(F.col("Exercise_Hours_Per_Week").cast(DoubleType()), F.lit(0.0)).alias("exercise_hours_per_week"),
        F.coalesce(F.col("Daily_Walking_Minutes").cast(DoubleType()), F.lit(0.0)).alias("daily_walking_minutes"),
        F.coalesce(F.col("Diet_Quality"), F.lit("Unknown")).alias("diet_quality"),
        F.col("Sugar_Intake_Level").alias("sugar_intake_level"),
        F.coalesce(F.col("Sleep_Hours").cast(DoubleType()), F.lit(7.0)).alias("sleep_hours"),
        F.col("Stress_Level").alias("stress_level"),
        F.col("Smoking_Status").alias("smoking_status"),
        F.col("Alcohol_Consumption").alias("alcohol_consumption"),
        F.coalesce(F.col("Medication_Adherence"), F.lit("Unknown")).alias("medication_adherence"),
        F.col("Daily_Water_Intake_L").cast(DoubleType()).alias("daily_water_intake_l"),
    )
    .dropDuplicates(["patient_id", "snapshot_date"])
)

target_table_path = f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_lifestyle"

if spark.catalog.tableExists(target_table_path):
    (
        DeltaTable.forName(spark, target_table_path)
        .alias("target")
        .merge(
            bronze_lifestyle.alias("source"),
            "target.patient_id = source.patient_id AND target.snapshot_date = source.snapshot_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    bronze_lifestyle.write.format("delta").saveAsTable(target_table_path)

print(f"Successfully processed {target_table_path}")
