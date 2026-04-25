"""
This module to transform data from silver stage to gold stage in BigQuery. 
The transformation includes: feature engineering, dimensional modeling, and data aggregation.
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime 

ANCHOR_DATE = pd.to_datetime('2025-10-01')
PROJECT_ID = 'loan-risk-home-credit'
DATASET_SILVER = 'silver_stage'
DATASET_GOLD = 'gold_stage'

def transform_application():

    # 1. Read Data
    df = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.application`", project_id=PROJECT_ID)
    df_app_target = pd.read_gbq(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.application_target`", 
        project_id=PROJECT_ID
    )
    # ---------------------------------------------------------
    # 2. Dimensions Creation
    # ---------------------------------------------------------

    # dim date table
    dim_date_registration = df[['date_registration']].drop_duplicates().reset_index(drop=True)
    dim_date_registration['date_reg_id'] = dim_date_registration.index
    dim_date_registration = dim_date_registration[['date_reg_id','date_registration']]

    dim_date_id_publish = df[['date_id_publish']].drop_duplicates().reset_index(drop=True)
    dim_date_id_publish['date_pub_id'] = dim_date_id_publish.index
    dim_date_id_publish = dim_date_id_publish[['date_pub_id', 'date_id_publish']]


    # dim_customer_info
    info_cols = ['date_birth','code_gender', 'name_family_status',
                'cnt_children', 'name_education_type', 'cnt_fam_members']
    dim_customer_info = df[info_cols].drop_duplicates().reset_index(drop=True)
    dim_customer_info['info_id'] = dim_customer_info.index 
    dim_customer_info = dim_customer_info[['info_id'] + info_cols]

    # dim_customer_career
    career_cols = ['amt_income_total', 'name_income_type','organization_type', 'date_employed']
    dim_customer_career = df[career_cols].drop_duplicates().reset_index(drop=True)
    dim_customer_career['career_id'] = dim_customer_career.index
    dim_customer_career = dim_customer_career[['career_id'] + career_cols]
    
    # dim_customer_asset
    asset_cols = ['flag_own_car','flag_own_realty', 'name_housing_type', 'own_car_age']
    dim_customer_asset = df[asset_cols].drop_duplicates().reset_index(drop=True)
    dim_customer_asset['asset_id'] = dim_customer_asset.index
    dim_customer_asset = dim_customer_asset[['asset_id'] + asset_cols]

    # dim_customer_place
    place_cols = [
        'region_population_relative', 'region_rating_client', 'region_rating_client_w_city',
        'reg_region_not_live_region', 'reg_region_not_work_region', 'live_region_not_work_region',
        'reg_city_not_live_city', 'reg_city_not_work_city', 'live_city_not_work_city'
    ]
    dim_customer_place = df[place_cols].drop_duplicates().reset_index(drop=True)
    dim_customer_place['place_id'] = dim_customer_place.index
    dim_customer_place = dim_customer_place[['place_id'] + place_cols]

    # dim_contract
    contract_cols = [
        'name_contract_type', 'name_type_suite', 
        'weekday_appr_process_start', 'hour_appr_process_start'
    ]
    dim_contract = df[contract_cols].drop_duplicates().reset_index(drop=True)
    dim_contract['contract_id'] = dim_contract.index
    dim_contract = dim_contract[['contract_id'] + contract_cols]

    # dim flag doc
    doc_cols = [col for col in df.columns if col.startswith('flag_document')]
    dim_documents = df[doc_cols].drop_duplicates().reset_index(drop=True)
    dim_documents['doc_id'] = dim_documents.index
    dim_documents = dim_documents[['doc_id'] + doc_cols]

    # dim contact
    contact_cols = [
    'flag_mobil',
    'flag_emp_phone',
    'flag_work_phone',
    'flag_cont_mobile',
    'flag_phone',
    'flag_email'
    ]

    dim_contact = df[contact_cols].drop_duplicates().reset_index(drop=True)
    dim_contact['contact_id'] = dim_contact.index
    dim_contact = dim_contact[['contact_id'] + contact_cols]

    # ---------------------------------------------------------
    # 3. Merging Surrogate Keys back to base dataframe for the Fact Table
    # ---------------------------------------------------------
    
    # For dimensions that were deduplicated, we merge on their features to get the appropriate IDs
    df = df.merge(dim_date_id_publish, on='date_id_publish', how='left')
    df = df.merge(dim_date_registration, on='date_registration', how='left')
    df = df.merge(dim_customer_info, on=info_cols, how='left')
    df = df.merge(dim_customer_asset, on=asset_cols, how='left')
    df = df.merge(dim_customer_place, on=place_cols, how='left')
    df = df.merge(dim_contract, on=contract_cols, how='left')
    df = df.merge(dim_customer_career, on=career_cols, how='left')
    df = df.merge(dim_documents, on=doc_cols, how='left')
    df = df.merge(dim_contact, on=contact_cols, how='left')
    # ---------------------------------------------------------
    # 4. Fact Table Creation
    # ---------------------------------------------------------
    
    # Selecting the primary key, foreign keys, and statistical/financial measures
    fact_cols = [
        'sk_id_curr','date_pub_id', 'date_reg_id', 'career_id', 'asset_id', 'place_id', 'contract_id',
        'amt_credit', 'amt_annuity', 'amt_goods_price', 
        'ext_source_1', 'ext_source_2', 'ext_source_3',
        'annuity_to_credit_ratio', 'credit_to_income_ratio', 'annuity_to_income_ratio', 'credit_term',
        'obs_30_cnt_social_circle', 'def_30_cnt_social_circle', 
        'obs_60_cnt_social_circle', 'def_60_cnt_social_circle',
        'amt_req_credit_bureau_hour', 'amt_req_credit_bureau_day', 
        'amt_req_credit_bureau_week', 'amt_req_credit_bureau_mon', 
        'amt_req_credit_bureau_qrt', 'amt_req_credit_bureau_year'
    ]
    fact_application = df[fact_cols]

    del df

    # ---------------------------------------------------------
    # 5. Load Data to BigQuery Gold Stage
    # ---------------------------------------------------------
    
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
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        # Load via pandas_gbq
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")

    return "Transformation and loading to Gold stage completed successfully."


def transform_bureau():

    df_bureau = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.bureau`", project_id=PROJECT_ID)
    
    # Load the bureau balance table which is already prepared as a dimension
    df_bb = pd.read_gbq(f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.bureau_balance`", project_id=PROJECT_ID)

    # dim_credit_status: Captures descriptive attributes about the type and status of the credit
    status_cols = ['credit_active', 'credit_currency', 'credit_type']
    dim_credit_status = df_bureau[status_cols].drop_duplicates().reset_index(drop=True)
    dim_credit_status['status_id'] = dim_credit_status.index
    dim_credit_status = dim_credit_status[['status_id'] + status_cols]

    # dim_bureau_date: Centralizes all the temporal attributes and date markers
    date_cols = [
        'days_credit', 'days_credit_enddate', 'days_enddate_fact', 'days_credit_update'
    ]
    dim_bureau_date = df_bureau[date_cols].drop_duplicates().reset_index(drop=True)
    dim_bureau_date['bureau_date_id'] = dim_bureau_date.index
    dim_bureau_date = dim_bureau_date[['bureau_date_id'] + date_cols]

    # dim_bureau_balance: Using your pre-processed table directly
    dim_bureau_balance = df_bb.copy()

    # dim risk_agg using bureau_balance
    status_type = {
         "Closed" : 0, "Unknown" : 0, "No DPD" : 0, "1" : 1,
        "DPD 31-60 days" : 2, "DPD 61-90 days" : 3, "DPD 91-120 days" : 4, "DPD > 120 days" : 5
    } # had transformed before. 
    df_bb['status_num'] = df_bb['status'].map(status_type)

    df_bb['is_delinquent'] = (df_bb['status_num'] > 0).astype(int)
    df_bb['is_severe'] = (df_bb['status_num'] >= 3).astype(int)

    agg_bb = df_bb.groupby('sk_id_bureau').agg(
        max_dpd=('status_num', 'max'),
        avg_dpd=('status_num', 'mean'),
        delinquency_ratio=('is_delinquent', 'mean'),
        severe_delinquency_ratio=('is_severe', 'mean'),
        history_length=('months_balance', 'count'),
        history_min=('months_balance', 'min'),
        history_max=('months_balance', 'max')
    ).reset_index()

    recent_df = df_bb[df_bb['months_balance'] >= -3]

    agg_recent = recent_df.groupby('sk_id_bureau').agg(
        recent_max_dpd=('status_num', 'max'),
        recent_avg_dpd=('status_num', 'mean')
    ).reset_index()

    # Merge recent into main agg
    agg_bb = agg_bb.merge(agg_recent, on='sk_id_bureau', how='left')

    for i in range(6):
        df_bb[f'dpd_{i}'] = (df_bb['status_num'] == i).astype(int)

    agg_bucket = df_bb.groupby('sk_id_bureau').agg({
        f'dpd_{i}': 'sum' for i in range(6)
    }).reset_index()

    agg_bb = agg_bb.merge(agg_bucket, on='sk_id_bureau', how='left')

    dim_bureau_behavior = agg_bb.copy()
    dim_bureau_behavior['bb_id'] = dim_bureau_behavior.index

    dim_bureau_behavior = dim_bureau_behavior[
        ['bb_id', 'sk_id_bureau'] +
        [col for col in dim_bureau_behavior.columns if col not in ['bb_id', 'sk_id_bureau']]
    ]

    # Merge the generated IDs back into the main bureau dataframe using the original columns
    df_bureau = df_bureau.merge(dim_credit_status, on=status_cols, how='left')
    df_bureau = df_bureau.merge(dim_bureau_date, on=date_cols, how='left')
    df_bureau = df_bureau.merge(dim_bureau_behavior[['bb_id', 'sk_id_bureau']],on='sk_id_bureau',how='left')

    # ---------------------------------------------------------
    # 4. Fact Table Creation
    # ---------------------------------------------------------
    
    # The fact table contains primary keys, foreign keys mapping to dimensions, and numerical facts/metrics.
    fact_cols = [
        'sk_id_curr',              # Maps to Application table
        'sk_id_bureau',            # Maps to dim_bureau_balance
        'status_id',               # Maps to dim_credit_status
        'bureau_date_id',          # Maps to dim_bureau_date
        'bb_id',
        'credit_day_overdue', 
        'amt_credit_max_overdue', 
        'cnt_credit_prolong', 
        'amt_credit_sum', 
        'amt_credit_sum_debt', 
        'amt_credit_sum_limit', 
        'amt_credit_sum_overdue', 
        'amt_annuity', 
        'annuity_to_credit_ratio'
    ]
    fact_bureau = df_bureau[fact_cols]

    tables_to_load = {
        'dim_credit_status': dim_credit_status,
        'dim_bureau_date': dim_bureau_date,
        'dim_bureau_balance': dim_bureau_balance,
        'dim_bureau_behavior': dim_bureau_behavior,
        'fact_bureau': fact_bureau
    }
    
    for table_name, table_df in tables_to_load.items():
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        # Load via pandas_gbq
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")

    return "Bureau transformation and loading to Gold stage completed successfully."

def transform_credit_card_balance():

    df_ccb = pd.read_gbq(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.credit_card_balance`", 
        project_id=PROJECT_ID
    )

    # dim_cc_status: Captures the status of the credit card contract
    status_cols = ['name_contract_status']
    dim_cc_status = df_ccb[status_cols].drop_duplicates().reset_index(drop=True)
    dim_cc_status['cc_status_id'] = dim_cc_status.index
    dim_cc_status = dim_cc_status[['cc_status_id'] + status_cols] 

    # dim_cc_date: Centralizes the historical tracking periods
    date_cols = ['months_balance', 'year_month_balance']
    dim_cc_date = df_ccb[date_cols].drop_duplicates().reset_index(drop=True)
    dim_cc_date['cc_date_id'] = dim_cc_date.index
    dim_cc_date = dim_cc_date[['cc_date_id'] + date_cols]

    # Merge the generated IDs back into the main credit card dataframe 
    df_ccb = df_ccb.merge(dim_cc_status, on=status_cols, how='left')
    df_ccb = df_ccb.merge(dim_cc_date, on=date_cols, how='left')

    # Selecting the primary key, foreign keys, and financial/transactional measures
    fact_cols = [
        'sk_id_prev',                 # Primary key for previous application context
        'sk_id_curr',                 # Maps to core application table
        'cc_status_id',               # Maps to dim_cc_status
        'cc_date_id',                 # Maps to dim_cc_date
        
        # Balance & Limit metrics
        'amt_balance', 
        'amt_credit_limit_actual', 
        
        # Drawing amounts
        'amt_drawings_atm_current', 
        'amt_drawings_current', 
        'amt_drawings_other_current', 
        'amt_drawings_pos_current', 
        
        # Payment amounts
        'amt_inst_min_regularity', 
        'amt_payment_current', 
        'amt_payment_total_current', 
        
        # Receivable amounts
        'amt_receivable_principal', 
        'amt_recivable', 
        'amt_total_receivable', 
        
        # Frequency counts
        'cnt_drawings_atm_current', 
        'cnt_drawings_current', 
        'cnt_drawings_other_current', 
        'cnt_drawings_pos_current', 
        'cnt_instalment_mature_cum', 
        
        # Delinquency metrics
        'sk_dpd',                     # Days past due
        'sk_dpd_def'                  # Days past due with tolerance
    ]
    
    fact_credit_card_balance = df_ccb[fact_cols]
    
    tables_to_load = {
        'dim_cc_status': dim_cc_status,
        'dim_cc_date': dim_cc_date,
        'fact_credit_card_balance': fact_credit_card_balance
    }
    
    for table_name, table_df in tables_to_load.items():
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        # Load via pandas_gbq
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")

    return "Credit Card Balance transformation and loading to Gold stage completed successfully."

def transform_installments_payments():
    df = pd.read_gbq(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.installments_payments`", 
        project_id=PROJECT_ID
    )
    
    # dim_ins_info 
    dim_ins_info = df[['num_instalment_version', 'num_instalment_number']].drop_duplicates().reset_index(drop=True)
    dim_ins_info['ins_info_id'] = dim_ins_info.index
    dim_ins_info = dim_ins_info[['ins_info_id','num_instalment_version', 'num_instalment_number']]

    # dim_date
    date_cols = ['date_installment', 'date_entry_payment', 'days_instalment', 'days_entry_payment']
    dim_ins_date = df[date_cols].drop_duplicates().reset_index(drop=True)
    dim_ins_date['ins_date_id'] = dim_ins_date.index
    dim_ins_date = dim_ins_date[['ins_date_id'] + date_cols]

    # fact 
    df = df.merge(dim_ins_info, on=['num_instalment_version', 'num_instalment_number'], how ='left')
    df = df.merge(dim_ins_date, on=date_cols, how = 'left')

    fact_cols = [ 
        'sk_id_prev',
        'sk_id_curr',
        'ins_info_id',
        'ins_date_id',
        'amt_instalment',
        'amt_payment',
        'ratio_payment_installment', 
        'payment_delay'
    ]
    fact_installments_payments = df[fact_cols]

    tables_to_load = {
        'dim_ins_info': dim_ins_info,
        'dim_ins_date': dim_ins_date,
        'fact_installments_payments': fact_installments_payments
    }
    for table_name, table_df in tables_to_load.items():
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        # Load via pandas_gbq
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")
    return  "Installments Payments transformation and loading to Gold stage completed successfully."

def transform_previous_applications():

    df_prev = pd.read_gbq(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.previous_application`", 
        project_id=PROJECT_ID
    )

    # dim_prev_contract_info: Details about the contract type, status, payment methods, and rejection reasons
    contract_cols = [
        'name_contract_type', 'name_contract_status', 'name_cash_loan_purpose', 
        'name_payment_type', 'code_reject_reason', 'name_client_type', 'name_type_suite'
    ]
    dim_prev_contract_info = df_prev[contract_cols].drop_duplicates().reset_index(drop=True)
    dim_prev_contract_info['prev_contract_id'] = dim_prev_contract_info.index
    dim_prev_contract_info = dim_prev_contract_info[['prev_contract_id'] + contract_cols]

    # dim_prev_product_info: Captures the physical goods or financial product groupings and seller channels
    product_cols = [
        'name_goods_category', 'name_portfolio', 'name_product_type', 
        'channel_type', 'name_seller_industry', 'name_yield_group', 'product_combination'
    ]
    dim_prev_product_info = df_prev[product_cols].drop_duplicates().reset_index(drop=True)
    dim_prev_product_info['prev_product_id'] = dim_prev_product_info.index
    dim_prev_product_info = dim_prev_product_info[['prev_product_id'] + product_cols]

    # dim_prev_date: Centralizes all absolute dates and relative days mapping 
    date_cols = [
        'weekday_appr_process_start', 'hour_appr_process_start', 'days_decision',
        'days_first_drawing', 'days_first_due', 'days_last_due_1st_version',
        'days_last_due', 'days_termination', 'date_decision', 'date_first_due',
        'date_last_due_1st_version', 'date_last_due', 'date_termination'
    ]
    dim_prev_date = df_prev[date_cols].drop_duplicates().reset_index(drop=True)
    dim_prev_date['prev_date_id'] = dim_prev_date.index
    dim_prev_date = dim_prev_date[['prev_date_id'] + date_cols]

    # Merge the generated IDs back into the main dataframe based on the column groupings
    df_prev = df_prev.merge(dim_prev_contract_info, on=contract_cols, how='left')
    df_prev = df_prev.merge(dim_prev_product_info, on=product_cols, how='left')
    df_prev = df_prev.merge(dim_prev_date, on=date_cols, how='left')
    
    # Selecting the primary key, foreign keys, and raw numerical attributes (amounts, rates, flags)
    fact_cols = [
        'sk_id_prev',                 # Primary key
        'sk_id_curr',                 # Foreign key mapping to core application table
        'prev_contract_id',           # Maps to dim_prev_contract_info
        'prev_product_id',            # Maps to dim_prev_product_info
        'prev_date_id',               # Maps to dim_prev_date
        
        # Financial Measures
        'amt_annuity', 
        'amt_application', 
        'amt_credit', 
        'amt_down_payment', 
        'amt_goods_price',
        'rate_down_payment', 
        'rate_interest_primary', 
        'rate_interest_privileged', 
        
        # Transactional Counts and Store Area
        'cnt_payment',
        'sellerplace_area', 
        
        # Flags (can remain in fact table if used heavily as fast binary masks/filters)
        'flag_last_appl_per_contract', 
        'nflag_last_appl_in_day',
        'nflag_insured_on_approval'
    ]
    
    fact_previous_applications = df_prev[fact_cols]

    tables_to_load = {
        'dim_prev_contract_info': dim_prev_contract_info,
        'dim_prev_product_info': dim_prev_product_info,
        'dim_prev_date': dim_prev_date,
        'fact_previous_applications': fact_previous_applications
    }
    
    for table_name, table_df in tables_to_load.items():
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        # Load via pandas_gbq
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")

    return "Previous Applications transformation and loading to Gold stage completed successfully."


def transform_pos_cash_balance():

    df_pos = pd.read_gbq(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_SILVER}.pos_cash_balance`", 
        project_id=PROJECT_ID
    )

    df_pos.columns = df_pos.columns.str.lower()

    # dim_pos_status
    status_cols = ['name_contract_status']
    dim_pos_status = df_pos[status_cols].drop_duplicates().reset_index(drop=True)
    dim_pos_status['pos_status_id'] = dim_pos_status.index
    dim_pos_status = dim_pos_status[['pos_status_id'] + status_cols] 

    # dim_pos_date
    date_cols = ['months_balance', 'year_month_balance']
    
    dim_pos_date = df_pos[date_cols].drop_duplicates().reset_index(drop=True)

    dim_pos_date['pos_date_id'] = dim_pos_date.index
    dim_pos_date = dim_pos_date[['pos_date_id'] + date_cols]

    df_pos = df_pos.merge(dim_pos_status, on=status_cols, how='left')
    df_pos = df_pos.merge(dim_pos_date, on=date_cols, how='left')

    fact_cols = [
        'sk_id_prev',                 
        'sk_id_curr',             
        'pos_status_id',            
        'pos_date_id',    
        
        'cnt_instalment',           
        'cnt_instalment_future',      
        'sk_dpd',                     
        'sk_dpd_def'   
    ]
    
    fact_pos_cash_balance = df_pos[fact_cols]

    tables_to_load = {
        'dim_pos_status': dim_pos_status,
        'dim_pos_date': dim_pos_date,
        'fact_pos_cash_balance': fact_pos_cash_balance
    }
    
    for table_name, table_df in tables_to_load.items():
        destination_table = f"{PROJECT_ID}.{DATASET_GOLD}.{table_name}"
        print(f"Loading data to {destination_table}...")
        
        table_df.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'
        )
        print(f"Successfully loaded {table_name}.")

    return "POS Cash Balance transformation and loading to Gold stage completed successfully."


# aggregation table for analysis!

# Behavioral History at Bureau Creduit
def agg_bureau_summary():
    return 

# Behavioral History summary at Home Credit
def agg_previous_app_summary():
    return 