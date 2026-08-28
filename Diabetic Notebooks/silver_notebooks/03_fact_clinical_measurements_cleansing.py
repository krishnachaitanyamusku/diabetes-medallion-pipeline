# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

CATALOG_NAME = "diabetes_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_clinical = (
    spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.clinical_measurements")
    .select(
        F.col("Patient_ID").cast(IntegerType()).alias("patient_id"),
        F.col("snapshot_date").cast(DateType()).alias("snapshot_date"),
        F.col("Height_cm").cast(DoubleType()).alias("height_cm"),
        F.col("Weight_kg").cast(DoubleType()).alias("weight_kg"),
        F.col("BMI").cast(DoubleType()).alias("bmi"),
        F.col("Waist_Circumference_cm").cast(DoubleType()).alias("waist_circumference_cm"),
        F.col("Blood_Glucose").cast(DoubleType()).alias("blood_glucose"),
        F.col("HbA1c").cast(DoubleType()).alias("hba1c"),
        F.col("Fasting_Blood_Sugar").cast(DoubleType()).alias("fasting_blood_sugar"),
        F.col("Insulin_Level").cast(DoubleType()).alias("insulin_level"),
        F.col("Blood_Pressure_Systolic").cast(IntegerType()).alias("bp_systolic"),
        F.col("Blood_Pressure_Diastolic").cast(IntegerType()).alias("bp_diastolic"),
        F.col("Total_Cholesterol").cast(DoubleType()).alias("total_cholesterol"),
        F.col("HDL").cast(DoubleType()).alias("hdl"),
        F.col("LDL").cast(DoubleType()).alias("ldl"),
        F.col("Triglycerides").cast(DoubleType()).alias("triglycerides"),
        F.col("Heart_Rate").cast(IntegerType()).alias("heart_rate"),
    )
    .withColumn(
        "height_cm",
        F.when(
            F.col("height_cm").isNull() & F.col("weight_kg").isNotNull() & F.col("bmi").isNotNull(),
            F.sqrt(F.col("weight_kg") / F.col("bmi")) * 100,
        ).otherwise(F.col("height_cm")),
    )
    .withColumn(
        "weight_kg",
        F.when(
            F.col("weight_kg").isNull() & F.col("height_cm").isNotNull() & F.col("bmi").isNotNull(),
            F.col("bmi") * F.pow(F.col("height_cm") / 100, 2),
        ).otherwise(F.col("weight_kg")),
    )
    .dropDuplicates(["patient_id", "snapshot_date"])
)

target_table_path = f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_clinical_measurements"

if spark.catalog.tableExists(target_table_path):
    (
        DeltaTable.forName(spark, target_table_path)
        .alias("target")
        .merge(
            bronze_clinical.alias("source"),
            "target.patient_id = source.patient_id AND target.snapshot_date = source.snapshot_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    bronze_clinical.write.format("delta").saveAsTable(target_table_path)

print(f"Successfully processed {target_table_path}")
