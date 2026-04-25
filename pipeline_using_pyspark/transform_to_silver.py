"""
This module to transforms data from Bronze Stage to Silver Stage in BigQuery. 

This module aims to: handle null data, type casting, remove outliers,
deduplicate, join tables (train and test of application), normalize data, and check data quality.
The transformed data will be stored in the silver stage in BigQuery.

==> Using Pyspark instead
"""

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, FloatType
import numpy as np
from spark_config import get_spark_session

ANCHOR_DATE = "2025-10-01"
PROJECT_ID = 'loan-risk-home-credit'
DATASET_BRONZE = 'bronze_stage_spark'
DATASET_SILVER = 'silver_stage_spark'

spark = get_spark_session("Transformation Silver")

def read_bq(table):
    return spark.read.format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_BRONZE}.{table}") \
        .load()

def write_bq(df, table):
    temp_bucket = "loan-risk-spark-temp-bucket"
    df.write.format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_SILVER}.{table}") \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()

def round_float_columns(df):
    float_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (DoubleType, FloatType))]
    for c in float_cols:
        df = df.withColumn(c, round(col(c), 4))
    return df

def data_quality_check_spark(df, table_name, subset_dedup=None):
    print(f"--- Running Quality Check for {table_name} ---")

    # 1. Deduplicate
    if subset_dedup:
        df = df.dropDuplicates(subset_dedup)
    else:
        df = df.dropDuplicates()

    # 2. Missing values logging (Fix Data Type Mismatch)
    exprs = []
    for f in df.schema.fields:
        c = f.name
        # just use isnan() for (Float/Double)
        if isinstance(f.dataType, (DoubleType, FloatType)):
            exprs.append(count(when(col(c).isNull() | isnan(col(c)), c)).alias(c))
        else:
            # others types (String, Boolean, Integer) only use isNull()
            exprs.append(count(when(col(c).isNull(), c)).alias(c))

    null_counts = df.select(exprs)
    
    print(f"Missing values for {table_name}:")
    null_counts.show()

    # 3. Handle DAYS outliers (>36500)
    days_cols = [c for c in df.columns if "days" in c]
    for c in days_cols:
        df = df.withColumn(
            c,
            when(col(c) > 36500, lit(None).cast(df.schema[c].dataType)).otherwise(col(c))
        )

    return df


def transform_application_spark():
    df_train = read_bq("application_train")
    df_test = read_bq("application_test")

    # lowercase columns
    df_train = df_train.toDF(*[c.lower() for c in df_train.columns])
    df_test = df_test.toDF(*[c.lower() for c in df_test.columns])

    # target table
    df_target = df_train.select("sk_id_curr", "target")
    df_train = df_train.drop("target")

    # union
    df = df_train.unionByName(df_test)

    df = data_quality_check_spark(df, "application", subset_dedup=["sk_id_curr"])
    df = round_float_columns(df)

    df = df.withColumn("cnt_fam_members", col("cnt_fam_members").cast(IntegerType()))

    df = df.withColumn("age", (abs(col("days_birth")) / 365).cast(IntegerType()))
    
    #('%Y-%m-%d')
    date_cols_mapping = {
        "date_birth": "days_birth",
        "date_employed": "days_employed",
        "date_registration": "days_registration",
        "date_last_phone_change": "days_last_phone_change",
        "date_id_publish": "days_id_publish"
    }
    for new_col, old_col in date_cols_mapping.items():
        df = df.withColumn(new_col, date_format(date_add(lit(ANCHOR_DATE), col(old_col).cast(IntegerType())), "yyyy-MM-dd"))


    df = df.withColumn("annuity_to_credit_ratio", col("amt_annuity") / when(col("amt_credit") == 0, None).otherwise(col("amt_credit")))
    df = df.withColumn("credit_to_income_ratio", col("amt_credit") / when(col("amt_income_total") == 0, None).otherwise(col("amt_income_total")))
    df = df.withColumn("annuity_to_income_ratio", col("amt_annuity") / when(col("amt_income_total") == 0, None).otherwise(col("amt_income_total")))
    df = df.withColumn("credit_term", col("amt_credit") / when(col("amt_annuity") == 0, None).otherwise(col("amt_annuity")))


    write_bq(df, "application")
    write_bq(df_target, "application_target")

def transform_bureau_spark():
    df = read_bq("bureau")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'bureau')
    df = round_float_columns(df)

    # time -1 if > 0
    df = df.withColumn("days_credit_update", when(col("days_credit_update") > 0, col("days_credit_update") * -1).otherwise(col("days_credit_update")))

    df = df.withColumn("annuity_to_credit_ratio", col("amt_annuity") / when(col("amt_credit_sum") == 0, None).otherwise(col("amt_credit_sum")))

    write_bq(df, "bureau")

def transform_bureau_balance_spark():
    df = read_bq("bureau_balance")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'bureau_balance')

    # Mapping status
    status_map = {
        "C": "Closed", "X": "Unknown", "0": "No DPD", "1": "DPD 1-30 days",
        "2": "DPD 31-60 days", "3": "DPD 61-90 days", "4": "DPD 91-120 days", "5": "DPD > 120 days"
    }
    mapping_expr = create_map([lit(x) for k, v in status_map.items() for x in (k, v)])
    df = df.withColumn("status", mapping_expr.getItem(col("status")))

    df = df.withColumn("year_month_balance", date_format(date_add(lit(ANCHOR_DATE), (col("months_balance") * 30).cast(IntegerType())), "yyyy-MM"))

    write_bq(df, "bureau_balance")

def transform_credit_card_balance_spark():
    df = read_bq("credit_card_balance")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'credit_card_balance')
    df = round_float_columns(df)

    df = df.withColumn("year_month_balance", date_format(date_add(lit(ANCHOR_DATE), (col("months_balance") * 30).cast(IntegerType())), "yyyy-MM"))

    write_bq(df, "credit_card_balance")

def transform_pos_cash_balance_spark():
    df = read_bq("POS_CASH_balance")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'pos_cash_balance')
    df = round_float_columns(df)

    df = df.withColumn("year_month_balance", date_format(date_add(lit(ANCHOR_DATE), (col("months_balance") * 30).cast(IntegerType())), "yyyy-MM"))

    write_bq(df, "pos_cash_balance")

def transform_installments_payments_spark():
    df = read_bq("installments_payments")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'installments_payments')

    df = df.withColumn("date_installment", date_format(date_add(lit(ANCHOR_DATE), col("days_instalment").cast(IntegerType())), "yyyy-MM-dd"))
    df = df.withColumn("date_entry_payment", date_format(date_add(lit(ANCHOR_DATE), col("days_entry_payment").cast(IntegerType())), "yyyy-MM-dd"))

    df = df.withColumn("ratio_payment_installment", round(col("amt_payment") / when(col("amt_instalment") == 0, None).otherwise(col("amt_instalment")), 4))
    df = df.withColumn("payment_delay", col("amt_instalment") - col("amt_payment"))

    write_bq(df, "installments_payments")

def transform_previous_application_spark():
    df = read_bq("previous_application")
    df = df.toDF(*[c.lower() for c in df.columns])
    
    df = data_quality_check_spark(df, 'previous_application')

    date_cols_mapping = {
        "date_decision": "days_decision",
        "date_first_due": "days_first_due",
        "date_last_due_1st_version": "days_last_due_1st_version",
        "date_last_due": "days_last_due",
        "date_termination": "days_termination"
    }
    for new_col, old_col in date_cols_mapping.items():
        df = df.withColumn(new_col, date_format(date_add(lit(ANCHOR_DATE), col(old_col).cast(IntegerType())), "yyyy-MM-dd"))

    df = df.withColumn("nflag_insured_on_approval", col("nflag_insured_on_approval").cast(IntegerType()))
    
    # Mapping flag
    mapping_flag = create_map([lit(0), lit('Not Insured'), lit(1), lit('Insured')])
    df = df.withColumn("nflag_insured_on_approval", mapping_flag.getItem(col("nflag_insured_on_approval")))

    write_bq(df, "previous_application")