"""
This module to transform data from silver stage to gold stage in BigQuery. 
The transformation includes: feature engineering, dimensional modeling, and data aggregation.

==> Using Pyspark instead
"""

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from spark_config import get_spark_session

ANCHOR_DATE = "2025-10-01"
PROJECT_ID = 'loan-risk-home-credit'
DATASET_SILVER = 'silver_stage_spark'
DATASET_GOLD = 'gold_stage_spark'

spark = get_spark_session("Transformation Gold")

def read_bq(table):
    return spark.read.format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_SILVER}.{table}") \
        .load()

def write_bq(df, table):
    temp_bucket = "loan-risk-spark-temp-bucket"
    print(f"Loading data to {PROJECT_ID}.{DATASET_GOLD}.{table}...")
    df.write.format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_GOLD}.{table}") \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()
    print(f"Successfully loaded {table}.")

def create_dimension(df, cols, id_col_name):
    """
    Helper function to deduplicate and create a surrogate key (like df.index in Pandas).
    """
    dim_df = df.select(*cols).dropDuplicates()
    # Using window function to generate a sequential ID
    w = Window.orderBy(monotonically_increasing_id())
    dim_df = dim_df.withColumn(id_col_name, row_number().over(w) - 1)
    
    # Reorder columns to put ID first
    return dim_df.select(id_col_name, *cols)

def transform_application_spark():
    # 1. Read Data
    df = read_bq("application")
    df_app_target = read_bq("application_target")

    dim_date_registration = create_dimension(df, ['date_registration'], 'date_reg_id')
    dim_date_id_publish = create_dimension(df, ['date_id_publish'], 'date_pub_id')

    info_cols = ['date_birth', 'code_gender', 'name_family_status', 'cnt_children', 'name_education_type', 'cnt_fam_members']
    dim_customer_info = create_dimension(df, info_cols, 'info_id')

    career_cols = ['amt_income_total', 'name_income_type', 'organization_type', 'date_employed']
    dim_customer_career = create_dimension(df, career_cols, 'career_id')
    
    asset_cols = ['flag_own_car', 'flag_own_realty', 'name_housing_type', 'own_car_age']
    dim_customer_asset = create_dimension(df, asset_cols, 'asset_id')

    place_cols = [
        'region_population_relative', 'region_rating_client', 'region_rating_client_w_city',
        'reg_region_not_live_region', 'reg_region_not_work_region', 'live_region_not_work_region',
        'reg_city_not_live_city', 'reg_city_not_work_city', 'live_city_not_work_city'
    ]
    dim_customer_place = create_dimension(df, place_cols, 'place_id')

    contract_cols = ['name_contract_type', 'name_type_suite', 'weekday_appr_process_start', 'hour_appr_process_start']
    dim_contract = create_dimension(df, contract_cols, 'contract_id')

    doc_cols = [col for col in df.columns if col.startswith('flag_document')]
    dim_documents = create_dimension(df, doc_cols, 'doc_id')

    contact_cols = ['flag_mobil', 'flag_emp_phone', 'flag_work_phone', 'flag_cont_mobile', 'flag_phone', 'flag_email']
    dim_contact = create_dimension(df, contact_cols, 'contact_id')


    df = df.join(dim_date_id_publish, on='date_id_publish', how='left')
    df = df.join(dim_date_registration, on='date_registration', how='left')
    df = df.join(dim_customer_info, on=info_cols, how='left')
    df = df.join(dim_customer_asset, on=asset_cols, how='left')
    df = df.join(dim_customer_place, on=place_cols, how='left')
    df = df.join(dim_contract, on=contract_cols, how='left')
    df = df.join(dim_customer_career, on=career_cols, how='left')
    df = df.join(dim_documents, on=doc_cols, how='left')
    df = df.join(dim_contact, on=contact_cols, how='left')


    fact_cols = [
        'sk_id_curr', 'date_pub_id', 'date_reg_id', 'career_id', 'asset_id', 'place_id', 'contract_id',
        'amt_credit', 'amt_annuity', 'amt_goods_price', 
        'ext_source_1', 'ext_source_2', 'ext_source_3',
        'annuity_to_credit_ratio', 'credit_to_income_ratio', 'annuity_to_income_ratio', 'credit_term',
        'obs_30_cnt_social_circle', 'def_30_cnt_social_circle', 
        'obs_60_cnt_social_circle', 'def_60_cnt_social_circle',
        'amt_req_credit_bureau_hour', 'amt_req_credit_bureau_day', 
        'amt_req_credit_bureau_week', 'amt_req_credit_bureau_mon', 
        'amt_req_credit_bureau_qrt', 'amt_req_credit_bureau_year'
    ]
    fact_application = df.select(*fact_cols)


    tables_to_load = {
        'dim_date_id_publish': dim_date_id_publish,
        'dim_date_registration': dim_date_registration,
        'dim_customer_info': dim_customer_info,
        'dim_customer_career': dim_customer_career,
        'dim_customer_asset': dim_customer_asset,
        'dim_customer_place': dim_customer_place,
        'dim_contract': dim_contract,
        'dim_documents': dim_documents,
        'dim_contact': dim_contact,
        'fact_application': fact_application,
        'dim_app_target': df_app_target
    }
    
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "Transformation and loading to Gold stage completed successfully."


def transform_bureau_spark():
    df_bureau = read_bq("bureau")
    df_bb = read_bq("bureau_balance")

    status_cols = ['credit_active', 'credit_currency', 'credit_type']
    dim_credit_status = create_dimension(df_bureau, status_cols, 'status_id')

    date_cols = ['days_credit', 'days_credit_enddate', 'days_enddate_fact', 'days_credit_update']
    dim_bureau_date = create_dimension(df_bureau, date_cols, 'bureau_date_id')

    dim_bureau_balance = df_bb

    # dim risk_agg using bureau_balance
    status_type = {
        "Closed": 0, "Unknown": 0, "No DPD": 0, "1": 1,
        "DPD 31-60 days": 2, "DPD 61-90 days": 3, "DPD 91-120 days": 4, "DPD > 120 days": 5
    }
    mapping_expr = create_map([lit(x) for k, v in status_type.items() for x in (k, v)])
    df_bb = df_bb.withColumn('status_num', mapping_expr.getItem(col('status')))

    df_bb = df_bb.withColumn('is_delinquent', when(col('status_num') > 0, 1).otherwise(0))
    df_bb = df_bb.withColumn('is_severe', when(col('status_num') >= 3, 1).otherwise(0))

    # General aggregations
    agg_bb = df_bb.groupBy('sk_id_bureau').agg(
        max('status_num').alias('max_dpd'),
        avg('status_num').alias('avg_dpd'),
        avg('is_delinquent').alias('delinquency_ratio'),
        avg('is_severe').alias('severe_delinquency_ratio'),
        count('months_balance').alias('history_length'),
        min('months_balance').alias('history_min'),
        max('months_balance').alias('history_max')
    )

    # Recent aggregations
    recent_df = df_bb.filter(col('months_balance') >= -3)
    agg_recent = recent_df.groupBy('sk_id_bureau').agg(
        max('status_num').alias('recent_max_dpd'),
        avg('status_num').alias('recent_avg_dpd')
    )

    agg_bb = agg_bb.join(agg_recent, on='sk_id_bureau', how='left')

    # DPD buckets
    for i in range(6):
        df_bb = df_bb.withColumn(f'dpd_{i}', when(col('status_num') == i, 1).otherwise(0))

    agg_bucket_exprs = [sum(f'dpd_{i}').alias(f'dpd_{i}') for i in range(6)]
    agg_bucket = df_bb.groupBy('sk_id_bureau').agg(*agg_bucket_exprs)

    agg_bb = agg_bb.join(agg_bucket, on='sk_id_bureau', how='left')

    # Add bb_id
    w_bb = Window.orderBy(monotonically_increasing_id())
    dim_bureau_behavior = agg_bb.withColumn('bb_id', row_number().over(w_bb) - 1)
    
    # Reorder to keep bb_id, sk_id_bureau first
    behavior_cols = ['bb_id', 'sk_id_bureau'] + [c for c in dim_bureau_behavior.columns if c not in ['bb_id', 'sk_id_bureau']]
    dim_bureau_behavior = dim_bureau_behavior.select(*behavior_cols)

    # Merge back
    df_bureau = df_bureau.join(dim_credit_status, on=status_cols, how='left')
    df_bureau = df_bureau.join(dim_bureau_date, on=date_cols, how='left')
    df_bureau = df_bureau.join(dim_bureau_behavior.select('bb_id', 'sk_id_bureau'), on='sk_id_bureau', how='left')

    fact_cols = [
        'sk_id_curr', 'sk_id_bureau', 'status_id', 'bureau_date_id', 'bb_id',
        'credit_day_overdue', 'amt_credit_max_overdue', 'cnt_credit_prolong', 
        'amt_credit_sum', 'amt_credit_sum_debt', 'amt_credit_sum_limit', 
        'amt_credit_sum_overdue', 'amt_annuity', 'annuity_to_credit_ratio'
    ]
    fact_bureau = df_bureau.select(*fact_cols)

    tables_to_load = {
        'dim_credit_status': dim_credit_status,
        'dim_bureau_date': dim_bureau_date,
        'dim_bureau_balance': dim_bureau_balance,
        'dim_bureau_behavior': dim_bureau_behavior,
        'fact_bureau': fact_bureau
    }
    
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "Bureau transformation and loading to Gold stage completed successfully."


def transform_credit_card_balance_spark():
    df_ccb = read_bq("credit_card_balance")

    status_cols = ['name_contract_status']
    dim_cc_status = create_dimension(df_ccb, status_cols, 'cc_status_id')

    date_cols = ['months_balance', 'year_month_balance']
    dim_cc_date = create_dimension(df_ccb, date_cols, 'cc_date_id')

    df_ccb = df_ccb.join(dim_cc_status, on=status_cols, how='left')
    df_ccb = df_ccb.join(dim_cc_date, on=date_cols, how='left')

    fact_cols = [
        'sk_id_prev', 'sk_id_curr', 'cc_status_id', 'cc_date_id',
        'amt_balance', 'amt_credit_limit_actual', 'amt_drawings_atm_current', 
        'amt_drawings_current', 'amt_drawings_other_current', 'amt_drawings_pos_current', 
        'amt_inst_min_regularity', 'amt_payment_current', 'amt_payment_total_current', 
        'amt_receivable_principal', 'amt_recivable', 'amt_total_receivable', 
        'cnt_drawings_atm_current', 'cnt_drawings_current', 'cnt_drawings_other_current', 
        'cnt_drawings_pos_current', 'cnt_instalment_mature_cum', 
        'sk_dpd', 'sk_dpd_def'
    ]
    fact_credit_card_balance = df_ccb.select(*fact_cols)
    
    tables_to_load = {
        'dim_cc_status': dim_cc_status,
        'dim_cc_date': dim_cc_date,
        'fact_credit_card_balance': fact_credit_card_balance
    }
    
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "Credit Card Balance transformation and loading to Gold stage completed successfully."


def transform_installments_payments_spark():
    df = read_bq("installments_payments")
    
    info_cols = ['num_instalment_version', 'num_instalment_number']
    dim_ins_info = create_dimension(df, info_cols, 'ins_info_id')

    date_cols = ['date_installment', 'date_entry_payment', 'days_instalment', 'days_entry_payment']
    dim_ins_date = create_dimension(df, date_cols, 'ins_date_id')

    df = df.join(dim_ins_info, on=info_cols, how='left')
    df = df.join(dim_ins_date, on=date_cols, how='left')

    fact_cols = [ 
        'sk_id_prev', 'sk_id_curr', 'ins_info_id', 'ins_date_id',
        'amt_instalment', 'amt_payment', 'ratio_payment_installment', 'payment_delay'
    ]
    fact_installments_payments = df.select(*fact_cols)

    tables_to_load = {
        'dim_ins_info': dim_ins_info,
        'dim_ins_date': dim_ins_date,
        'fact_installments_payments': fact_installments_payments
    }
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "Installments Payments transformation and loading to Gold stage completed successfully."


def transform_previous_applications_spark():
    df_prev = read_bq("previous_application")

    contract_cols = [
        'name_contract_type', 'name_contract_status', 'name_cash_loan_purpose', 
        'name_payment_type', 'code_reject_reason', 'name_client_type', 'name_type_suite'
    ]
    dim_prev_contract_info = create_dimension(df_prev, contract_cols, 'prev_contract_id')

    product_cols = [
        'name_goods_category', 'name_portfolio', 'name_product_type', 
        'channel_type', 'name_seller_industry', 'name_yield_group', 'product_combination'
    ]
    dim_prev_product_info = create_dimension(df_prev, product_cols, 'prev_product_id')

    date_cols = [
        'weekday_appr_process_start', 'hour_appr_process_start', 'days_decision',
        'days_first_drawing', 'days_first_due', 'days_last_due_1st_version',
        'days_last_due', 'days_termination', 'date_decision', 'date_first_due',
        'date_last_due_1st_version', 'date_last_due', 'date_termination'
    ]
    dim_prev_date = create_dimension(df_prev, date_cols, 'prev_date_id')

    df_prev = df_prev.join(dim_prev_contract_info, on=contract_cols, how='left')
    df_prev = df_prev.join(dim_prev_product_info, on=product_cols, how='left')
    df_prev = df_prev.join(dim_prev_date, on=date_cols, how='left')
    
    fact_cols = [
        'sk_id_prev', 'sk_id_curr', 'prev_contract_id', 'prev_product_id', 'prev_date_id',
        'amt_annuity', 'amt_application', 'amt_credit', 'amt_down_payment', 'amt_goods_price',
        'rate_down_payment', 'rate_interest_primary', 'rate_interest_privileged', 
        'cnt_payment', 'sellerplace_area', 'flag_last_appl_per_contract', 
        'nflag_last_appl_in_day', 'nflag_insured_on_approval'
    ]
    fact_previous_applications = df_prev.select(*fact_cols)

    tables_to_load = {
        'dim_prev_contract_info': dim_prev_contract_info,
        'dim_prev_product_info': dim_prev_product_info,
        'dim_prev_date': dim_prev_date,
        'fact_previous_applications': fact_previous_applications
    }
    
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "Previous Applications transformation and loading to Gold stage completed successfully."


def transform_pos_cash_balance_spark():
    df_pos = read_bq("pos_cash_balance")

    # In Spark, it's safer to ensure all columns are lowercased if not already
    df_pos = df_pos.toDF(*[c.lower() for c in df_pos.columns])

    status_cols = ['name_contract_status']
    dim_pos_status = create_dimension(df_pos, status_cols, 'pos_status_id')

    date_cols = ['months_balance', 'year_month_balance']
    dim_pos_date = create_dimension(df_pos, date_cols, 'pos_date_id')

    df_pos = df_pos.join(dim_pos_status, on=status_cols, how='left')
    df_pos = df_pos.join(dim_pos_date, on=date_cols, how='left')

    fact_cols = [
        'sk_id_prev', 'sk_id_curr', 'pos_status_id', 'pos_date_id',    
        'cnt_instalment', 'cnt_instalment_future', 'sk_dpd', 'sk_dpd_def'   
    ]
    fact_pos_cash_balance = df_pos.select(*fact_cols)

    tables_to_load = {
        'dim_pos_status': dim_pos_status,
        'dim_pos_date': dim_pos_date,
        'fact_pos_cash_balance': fact_pos_cash_balance
    }
    
    for table_name, table_df in tables_to_load.items():
        write_bq(table_df, table_name)

    return "POS Cash Balance transformation and loading to Gold stage completed successfully."





"""
This module creates Aggregation / Data Mart tables for BI Reporting (Looker Studio).
All tables are aggregated at the Application Level (sk_id_curr).
"""

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import IntegerType

# # Assume spark session and read_bq, write_bq functions are already defined as in previous steps

# def agg_bureau_summary_spark():
#     """
#     Behavioral History at External Credit Bureau.
#     Summary of customer's credit history at external institutions.
#     """
#     print("Aggregating Bureau Summary...")
#     df_bureau = read_bq("bureau") # Read from Silver or Gold stage
    
#     # Create auxiliary flags
#     df_bureau = df_bureau.withColumn("is_active", when(col("credit_active") == "Active", 1).otherwise(0))
#     df_bureau = df_bureau.withColumn("is_bad_debt", when(col("credit_active") == "Bad debt", 1).otherwise(0))
#     df_bureau = df_bureau.withColumn("is_overdue", when(col("amt_credit_sum_overdue") > 0, 1).otherwise(0))

#     agg_df = df_bureau.groupBy("sk_id_curr").agg(
#         count("sk_id_bureau").alias("bureau_total_loans"),
#         sum("is_active").alias("bureau_active_loans"),
#         sum("is_bad_debt").alias("bureau_bad_debts"),
#         sum("is_overdue").alias("bureau_loans_with_overdue"),
        
#         sum("amt_credit_sum").alias("bureau_total_credit_amt"),
#         sum("amt_credit_sum_debt").alias("bureau_total_debt_amt"),
#         sum("amt_credit_sum_overdue").alias("bureau_total_overdue_amt"),
        
#         # Average Debt-to-Credit Ratio
#         avg(when(col("amt_credit_sum") > 0, col("amt_credit_sum_debt") / col("amt_credit_sum")).otherwise(0)).alias("bureau_avg_debt_ratio")
#     )
    
#     # Fill null values with 0 after aggregation
#     agg_df = agg_df.fillna(0)
#     write_bq(agg_df, "mart_bureau_summary")
#     return agg_df

# def agg_previous_app_summary_spark():
#     """
#     Behavioral History summary at Home Credit.
#     Summary of application history within the organization.
#     """
#     print("Aggregating Previous Application Summary...")
#     df_prev = read_bq("previous_application")
    
#     # Create flags
#     df_prev = df_prev.withColumn("is_approved", when(col("name_contract_status") == "Approved", 1).otherwise(0))
#     df_prev = df_prev.withColumn("is_refused", when(col("name_contract_status") == "Refused", 1).otherwise(0))
    
#     agg_df = df_prev.groupBy("sk_id_curr").agg(
#         count("sk_id_prev").alias("prev_total_apps"),
#         sum("is_approved").alias("prev_approved_apps"),
#         sum("is_refused").alias("prev_refused_apps"),
        
#         sum("amt_application").alias("prev_total_applied_amt"),
#         sum("amt_credit").alias("prev_total_granted_credit"),
        
#         # Average down payment amount
#         avg("amt_down_payment").alias("prev_avg_down_payment")
#     )
    
#     # Calculate Refusal Rate - extremely important feature for Risk
#     agg_df = agg_df.withColumn(
#         "prev_refusal_rate", 
#         round(col("prev_refused_apps") / col("prev_total_apps"), 4)
#     )
    
#     agg_df = agg_df.fillna(0)
#     write_bq(agg_df, "mart_previous_app_summary")
#     return agg_df

# def agg_payment_behavior_spark():
#     """
#     Additional function: Summary of installment payment behavior.
#     Very important for visualization in Looker Studio to analyze late payments.
#     """
#     print("Aggregating Payment Behavior Summary...")
#     df_ins = read_bq("installments_payments")
    
#     # Calculate days late (actual payment date minus scheduled payment date)
#     # Negative = early payment, Positive = late payment
#     df_ins = df_ins.withColumn("days_late", col("days_entry_payment") - col("days_instalment"))
#     df_ins = df_ins.withColumn("is_late", when(col("days_late") > 0, 1).otherwise(0))
#     df_ins = df_ins.withColumn("is_underpaid", when(col("amt_payment") < col("amt_instalment"), 1).otherwise(0))

#     agg_df = df_ins.groupBy("sk_id_curr").agg(
#         count("sk_id_prev").alias("pay_total_installments"),
#         sum("is_late").alias("pay_late_installments"),
#         sum("is_underpaid").alias("pay_underpaid_installments"),
        
#         max("days_late").alias("pay_max_days_late"),
#         avg("days_late").alias("pay_avg_days_late"),
        
#         sum("amt_instalment").alias("pay_total_required_amt"),
#         sum("amt_payment").alias("pay_total_paid_amt")
#     )
    
#     # Late payment rate
#     agg_df = agg_df.withColumn(
#         "pay_late_rate", 
#         round(col("pay_late_installments") / col("pay_total_installments"), 4)
#     )
    
#     agg_df = agg_df.fillna(0)
#     write_bq(agg_df, "mart_payment_behavior")
#     return agg_df

# def create_master_looker_datamart_spark():
#     """
#     Additional function: Create a wide Master Table (Flat Table) for Looker Studio.
#     This table joins Application with all summary tables above.
#     """
#     print("Creating Master Data Mart for Looker Studio...")
    
#     # Get current application data & target (for risk analysis)
#     df_app = read_bq("application") # Select necessary columns or call Fact_Application builder
#     df_target = read_bq("application_target")
    
#     # Get summary tables
#     df_bureau = read_bq("mart_bureau_summary")
#     df_prev = read_bq("mart_previous_app_summary")
#     df_pay = read_bq("mart_payment_behavior")
    
#     # Join all tables using sk_id_curr (Left Join to retain new customers)
#     master_df = df_app.join(df_target, on="sk_id_curr", how="inner") \
#                       .join(df_bureau, on="sk_id_curr", how="left") \
#                       .join(df_prev, on="sk_id_curr", how="left") \
#                       .join(df_pay, on="sk_id_curr", how="left")
                      
#     # Fill default values for customers with no history (new customers)
#     master_df = master_df.fillna(0, subset=[c for c in master_df.columns if c.startswith(("bureau_", "prev_", "pay_"))])
    
#     # Categorize Risk for Looker Studio (easy for Pie Chart / Bar Chart)
#     master_df = master_df.withColumn(
#         "risk_label", 
#         when(col("target") == 1, "Default (Risk)").otherwise("Good (No Risk)")
#     )
    
#     write_bq(master_df, "looker_master_datamart")
#     print("Master Data Mart created successfully!")
#     return master_df