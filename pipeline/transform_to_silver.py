"""
This module aims to: handle null data, type casting, remove outliers,
deduplicate, join tables (train and test of application), normalize data, and check data quality.
The transformed data will be stored in the silver stage in BigQuery.
"""
from datetime import datetime
from google.cloud import bigquery
import pandas as pd 
import numpy as np

# Fixed anchor date to ensure DAG idempotency (replace with a suitable project anchor date)
ANCHOR_DATE = pd.to_datetime('2025-10-01') # Example anchor date (2025-10-01)
PROJECT_ID = 'loan-risk-home-credit'
DATASET_BRONZE = 'bronze_stage'
DATASET_SILVER = 'silver_stage'

def data_quality_check(df, table_name, subset_dedup=None):
    """
    Basic data quality checks based on business and data understanding.
    """
    print(f"--- Running Quality Check for {table_name} ---")
    
    # 1. Check Duplicates
    if subset_dedup:
        duplicates = df.duplicated(subset=subset_dedup).sum()
    else:
        duplicates = df.duplicated().sum()
    print(f"Duplicate rows found: {duplicates}")
    if duplicates > 0:
        if subset_dedup:
            df.drop_duplicates(subset=subset_dedup, inplace=True)
        else:
            df.drop_duplicates(inplace=True)
        print(f"Duplicates removed.")
        
    # 2. Check Missing Values (Logging only for this basic check)
    missing_data = df.isnull().sum()
    missing_cols = missing_data[missing_data > 0]
    if not missing_cols.empty:
        print(f"Columns with missing values:\n{missing_cols.to_string()}")
    else:
        print("No missing values found.")
        
    # 3. Handle Extreme Outliers in DAYS columns (Home Credit specific: 365243)
    cols_days = df.filter(like='days').columns
    if len(cols_days) > 0:
        outlier_mask = df[cols_days] > 36500
        outliers_count = outlier_mask.sum().sum()
        print(f"Outliers (> 36500) in 'days' columns found and set to NaN: {outliers_count}")
        df.loc[:, cols_days] = df[cols_days].mask(outlier_mask, np.nan)
        
    print(f"--- Quality Check for {table_name} Complete ---\n")
    return df

def transform_application():
    df1 = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.application_train`", project_id=PROJECT_ID)
    df2 = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.application_test`", project_id=PROJECT_ID)
    
    df1.columns = df1.columns.str.lower()
    df2.columns = df2.columns.str.lower()

    df_target = df1[['sk_id_curr', 'target']].copy()
    df1.drop(columns='target', inplace=True)

    df = pd.concat([df1, df2], ignore_index=True)
    
    # Quality Check & Deduplication
    df = data_quality_check(df, 'application', subset_dedup=['sk_id_curr'])

    # Float reduction
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(4)

    # Date transformations
    df['age'] = (abs(df['days_birth'])/365).astype('Int64')
    df['date_birth'] = (ANCHOR_DATE + pd.to_timedelta(df['days_birth'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_employed'] = (ANCHOR_DATE + pd.to_timedelta(df['days_employed'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_registration'] = (ANCHOR_DATE + pd.to_timedelta(df['days_registration'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_last_phone_change'] = (ANCHOR_DATE + pd.to_timedelta(df['days_last_phone_change'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_id_publish'] = (ANCHOR_DATE + pd.to_timedelta(df['days_id_publish'], unit='D')).dt.strftime('%Y-%m-%d')

    # Ratios
    df['annuity_to_credit_ratio'] = df['amt_annuity'] / df['amt_credit'].replace(0, np.nan)
    df['credit_to_income_ratio'] = df['amt_credit'] / df['amt_income_total'].replace(0, np.nan)
    df['annuity_to_income_ratio'] =  df['amt_annuity'] / df['amt_income_total'].replace(0, np.nan)
    df['credit_term'] = df['amt_credit'] / df['amt_annuity'].replace(0, np.nan)

    # target table
    # dict_target ={
    #     0: "Risk",
    #     1: "No Risk"
    # }
    # df['target_type'] = df_target['target'].map(dict_target)
    df.to_gbq(f"{DATASET_SILVER}.application", project_id=PROJECT_ID, if_exists='replace')
    df_target.to_gbq(f"{DATASET_SILVER}.application_target", project_id=PROJECT_ID, if_exists='replace')

def transform_bureau():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.bureau`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'bureau')

    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(4)

    df['last_credit_date'] = (ANCHOR_DATE + pd.to_timedelta(df['days_credit'], unit='D'))
    df['date_credit_enddate'] = (ANCHOR_DATE + pd.to_timedelta(df['days_credit_enddate'], unit='D'))
    
    df.loc[df['days_credit_update'] > 0, 'days_credit_update'] *= -1
    df['date_credit_update'] = (ANCHOR_DATE + pd.to_timedelta(df['days_credit_update'], unit='D'))

    df['annuity_to_credit_ratio'] = df['amt_annuity'] / df['amt_credit_sum'].replace(0, np.nan)
    
    df.to_gbq(f"{DATASET_SILVER}.bureau", project_id=PROJECT_ID, if_exists='replace')

def transform_bureau_balance():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.bureau_balance`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'bureau_balance')

    status_type = {
        "C" : "Closed", "X" : "Unknown", "0" : "No DPD", "1" : "DPD 1-30 days",
        "2" : "DPD 31-60 days", "3" : "DPD 61-90 days", "4" : "DPD 91-120 days", "5" : "DPD > 120 days"
    }
    df['status'] = df['status'].map(status_type)
    
    df['year_month_balance'] = (ANCHOR_DATE + pd.to_timedelta(df['months_balance'] * 30, unit='D')).dt.strftime('%Y-%m')

    df.to_gbq(f"{DATASET_SILVER}.bureau_balance", project_id=PROJECT_ID, if_exists='replace')

def transform_credit_card_balance():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.credit_card_balance`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'credit_card_balance')

    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(4)

    df['year_month_balance'] = (ANCHOR_DATE + pd.to_timedelta(df['months_balance'] * 30, unit='D')).dt.strftime('%Y-%m')

    df.to_gbq(f"{DATASET_SILVER}.credit_card_balance", project_id=PROJECT_ID, if_exists='replace')

def transform_pos_cash_balance():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.pos_cash_balance`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'pos_cash_balance')

    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(4)

    df['year_month_balance'] = (ANCHOR_DATE + pd.to_timedelta(df['months_balance'] * 30, unit='D')).dt.strftime('%Y-%m')

    df.to_gbq(f"{DATASET_SILVER}.pos_cash_balance", project_id=PROJECT_ID, if_exists='replace')

def transform_installments_payments():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.installments_payments`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'installments_payments')

    df['date_installment'] = (ANCHOR_DATE + pd.to_timedelta(df['days_instalment'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_entry_payment'] = (ANCHOR_DATE + pd.to_timedelta(df['days_entry_payment'], unit='D')).dt.strftime('%Y-%m-%d')
    
    df['ratio_payment_installment'] = df['amt_payment'] / df['amt_instalment'].replace(0, np.nan).round(4)
    df['payment_delay'] = df['amt_instalment'] - df['amt_payment']

    df.to_gbq(f"{DATASET_SILVER}.installments_payments", project_id=PROJECT_ID, if_exists='replace')

def transform_previous_application():
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_BRONZE}.previous_application`", project_id=PROJECT_ID)
    df.columns = df.columns.str.lower()
    df = data_quality_check(df, 'previous_application')

    df['date_decision'] = (ANCHOR_DATE + pd.to_timedelta(df['days_decision'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_first_due'] = (ANCHOR_DATE + pd.to_timedelta(df['days_first_due'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_last_due_1st_version'] = (ANCHOR_DATE + pd.to_timedelta(df['days_last_due_1st_version'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_last_due'] = (ANCHOR_DATE + pd.to_timedelta(df['days_last_due'], unit='D')).dt.strftime('%Y-%m-%d')
    df['date_termination'] = (ANCHOR_DATE + pd.to_timedelta(df['days_termination'], unit='D')).dt.strftime('%Y-%m-%d')

    df['nflag_insured_on_approval'] = df['nflag_insured_on_approval'].astype('Int64')
    dict_flag = {0: 'Not Insured', 1: 'Insured'}
    df['nflag_insured_on_approval'] = df['nflag_insured_on_approval'].map(dict_flag)

    df.to_gbq(f"{DATASET_SILVER}.previous_application", project_id=PROJECT_ID, if_exists='replace')