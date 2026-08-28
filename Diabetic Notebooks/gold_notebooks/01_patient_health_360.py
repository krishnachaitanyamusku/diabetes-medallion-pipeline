# Databricks notebook source
from pyspark.sql import functions as F

CATALOG_NAME = "diabetes_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

dim_patient = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_patient")
dim_lifestyle = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_lifestyle")
fact_clinical = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_clinical_measurements")
fact_risk = spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_diabetes_risk")

gold_patient_360 = (
    fact_clinical.alias("fc")
    .join(dim_patient.alias("dp"), on="patient_id", how="inner")
    .join(
        fact_risk.alias("fr"),
        on=["patient_id", "snapshot_date"],
        how="inner",
    )
    .join(
        dim_lifestyle.alias("dl"),
        on=["patient_id", "snapshot_date"],
        how="inner",
    )
    .select(
        F.col("patient_id"),
        F.col("snapshot_date"),
        F.col("dp.age"),
        F.col("dp.gender"),
        F.col("dp.country"),
        F.col("fc.bmi"),
        F.when(F.col("fc.bmi") < 18.5, "Underweight")
        .when((F.col("fc.bmi") >= 18.5) & (F.col("fc.bmi") < 25.0), "Normal")
        .when((F.col("fc.bmi") >= 25.0) & (F.col("fc.bmi") < 30.0), "Overweight")
        .otherwise("Obese")
        .alias("bmi_category"),
        F.col("fc.hba1c"),
        F.when(F.col("fc.hba1c") < 5.7, "Normal")
        .when((F.col("fc.hba1c") >= 5.7) & (F.col("fc.hba1c") <= 6.4), "Prediabetes")
        .otherwise("Diabetes")
        .alias("hba1c_condition"),
        F.col("dl.physical_activity_level"),
        F.col("dl.diet_quality"),
        F.col("dl.smoking_status"),
        F.col("fr.diabetes_risk_score"),
        F.col("fr.diabetes_risk"),
        F.col("fr.doctor_consultation_needed"),
    )
)

gold_patient_360.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG_NAME}.{GOLD_SCHEMA}.patient_health_360"
)

print("Successfully written patient_health_360 gold table.")
