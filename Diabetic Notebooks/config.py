# Databricks notebook source
class PipelineConfig:
    """Centralized configuration management for catalog and table names."""
    CATALOG_NAME: str = "diabetes_catalog"
    BRONZE_SCHEMA: str = "bronze"
    SILVER_SCHEMA: str = "silver"
    GOLD_SCHEMA: str = "gold"

    # Bronze Tables
    BRONZE_PATIENT: str = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.patient"
    BRONZE_LIFESTYLE: str = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.lifestyle"
    BRONZE_CLINICAL: str = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.clinical_measurements"
    BRONZE_RISK: str = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.diabetes_risk"

    # Silver Tables
    SILVER_PATIENT: str = f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_patient"
    SILVER_LIFESTYLE: str = f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_lifestyle"
    SILVER_CLINICAL: str = f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_clinical_measurements"
    SILVER_RISK: str = f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_diabetes_risk"

    # Gold Tables
    GOLD_PATIENT_360: str = f"{CATALOG_NAME}.{GOLD_SCHEMA}.patient_health_360"
    GOLD_LIFESTYLE_MATRIX: str = f"{CATALOG_NAME}.{GOLD_SCHEMA}.lifestyle_risk_matrix"
