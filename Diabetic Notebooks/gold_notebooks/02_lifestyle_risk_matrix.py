# Databricks notebook source
from pyspark.sql import functions as F

CATALOG_NAME = "diabetes_catalog"
GOLD_SCHEMA = "gold"

gold_patient_360 = spark.read.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.patient_health_360")

gold_lifestyle_matrix = (
    gold_patient_360.groupBy("physical_activity_level", "diet_quality", "smoking_status")
    .agg(
        F.count("patient_id").alias("patient_cohort_size"),
        F.round(F.avg("bmi"), 2).alias("avg_bmi"),
        F.round(F.avg("diabetes_risk_score"), 2).alias("avg_risk_score"),
        F.sum(F.when(F.col("hba1c_condition") == "Diabetes", 1).otherwise(0)).alias("diabetic_cases_count"),
    )
    .withColumn(
        "diabetes_prevalence_rate",
        F.round((F.col("diabetic_cases_count") / F.col("patient_cohort_size")) * 100, 2),
    )
)

gold_lifestyle_matrix.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG_NAME}.{GOLD_SCHEMA}.lifestyle_risk_matrix"
)

print("Successfully written lifestyle_risk_matrix gold table.")
