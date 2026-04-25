# LOAN RISK DATA WAREHOUSE

## Dataset Overview: Home Credit

<p align="center">
  <img src="image/home_credit.png" alt="Home Credit Data Schema" width="800">
</p>

This project utilizes the Home Credit dataset to build the data warehouse pipeline. The dataset consists of multiple relational tables containing historical loan application and financial history data. 

Below is a detailed breakdown of the source data files:

* **`application_{train|test}.csv`**: The primary main table containing static data for all applications. Each row represents a single loan in the data sample. It is split into Train (which includes the `TARGET` label) and Test (without the `TARGET` label) sets.
* **`bureau.csv`**: Records of clients' previous credits provided by other financial institutions that were reported to the Credit Bureau. A client may have multiple rows corresponding to their number of previous credits.
* **`bureau_balance.csv`**: Monthly balance logs of the previous credits detailed in `bureau.csv`. It contains one row for each month of historical data available for every previous credit.
* **`POS_CASH_balance.csv`**: Monthly balance snapshots of previous POS (point of sales) and cash loans that the applicant held specifically with Home Credit. 
* **`credit_card_balance.csv`**: Monthly balance snapshots of previous credit cards that the applicant held with Home Credit.
* **`previous_application.csv`**: Records of all previous applications for Home Credit loans made by clients who have loans in our current sample. One row corresponds to one previous application.
* **`installments_payments.csv`**: Repayment history for previously disbursed credits at Home Credit. It records both successful payments and missed payments (one row equates to one payment of one installment, or one missed installment).
* **`HomeCredit_columns_description.csv`**: The data dictionary containing detailed definitions and descriptions for all columns across the various data files mentioned above.

## Architecture

<p align="center">
  <img src="image/architecture.png" alt="Data Warehouse Architecture" width="800">
</p>

The data pipeline is built natively on Google Cloud Platform (GCP) and follows a **Medallion Architecture** pattern to ensure data quality, scalability, and logical separation of transformations.

### Key Components & Data Flow:

1. **Data Ingestion (Raw Data Layer):** * **Google Cloud Storage (GCS):** Acts as the initial data lake and landing zone for raw data files extracted from source systems.

2. **Orchestration & Compute Engine:**
   * **Apache Airflow:** Deployed on a **Google Compute Engine (GCE)** virtual machine. It acts as the central orchestrator, scheduling and monitoring all data extraction, loading, and transformation jobs.

3. **Data Warehouse (Google BigQuery):**
   Data is progressively transformed and promoted through three distinct layers:
   * **DWH Bronze (Raw):** Unprocessed, historical data ingested directly from GCS.
   * **DWH Silver (Cleansed):** Data that has been cleaned, filtered, and standardized for structured querying.
   * **DWH Gold (Curated):** Highly refined, aggregated, and business-level data optimized for analytics and reporting.

4. **BI & Visualization:**
   * **Looker Studio:** Connects directly to the **DWH Gold** layer to generate interactive dashboards and deliver actionable business insights to end-users.

## Data Processing Pipeline (Medallion Architecture)

The data transformation logic is handled entirely by PySpark and orchestrated by Airflow. The pipeline follows a multi-hop Medallion structure (Bronze ➡️ Silver ➡️ Gold) within BigQuery.

### 1. Ingestion Layer (Bronze)
**Script:** `pipeline_using_pyspark/el_to_bronze.py`
* **Objective:** Extract and Load (EL).
* **Process:** Extracts raw CSV files from Google Cloud Storage (GCS) and loads them directly into the BigQuery `bronze_stage` without any structural modifications. 
* **Tool:** Google BigQuery Python Client (`LoadJobConfig`).

### 2. Cleansing Layer (Silver)
**Script:** `pipeline_using_pyspark/transform_to_silver.py`
* **Objective:** Data Quality, Filtering, and Standardization.
* **Process:**
  * **Data Quality Checks:** Identifies missing values and drops exact duplicates.
  * **Outlier Handling:** Replaces anomalous historical values (e.g., `DAYS` columns > 36500) with `NULL`.
  * **Data Formatting:** Converts relative day counts (e.g., `days_birth`, `days_employed`) into readable `yyyy-MM-dd` date formats using an anchor date.
  * **Initial Feature Engineering:** Calculates base financial metrics such as `credit_to_income_ratio` and `annuity_to_credit_ratio`.

### 3. Curated Layer (Gold)
**Script:** `pipeline_using_pyspark/transform_to_gold.py`
* **Objective:** Dimensional Modeling and Aggregation for BI.
* **Process:**
  * **Dimensional Modeling:** Transforms wide, flat Silver tables into a structured Data Warehouse schema. It generates Surrogate Keys (using PySpark's `monotonically_increasing_id()` and Window functions) to split data into centralized **Fact tables** (e.g., `fact_application`, `fact_bureau`) and multiple **Dimension tables** (e.g., `dim_customer_info`, `dim_contract`).
  * **Behavioral Aggregations:** Aggregates historical credit behaviors, calculating features like severe delinquency ratios, Days Past Due (DPD) buckets, and installment payment delays.
  * **Data Mart Creation:** Prepares the final optimized tables ready for Looker Studio visualization.

## Setup on GCP (Airflow + PySpark)

This guide provides step-by-step instructions for configuring an Apache Airflow environment with PySpark capabilities on a Google Cloud Platform (GCP) Virtual Machine.

### 1. Prerequisites & GCP Configuration

Before running the setup scripts, ensure your GCP environment is properly configured.

* **Virtual Machine (VM):** This work use the VM `n2-standard-8` (8 vCPUs, 32 GB Memory) with a 128GB disk.
* **API Access:** When creating the VM, ensure the Access Scopes are set to **"Allow full access to all Cloud APIs"**.
* **Firewall Rule:** Create a firewall rule in GCP to open **TCP Port 8080** to all IP addresses (`0.0.0.0/0`) to access the Airflow Web UI.
* **Google Cloud Storage (GCS):** Create necessary buckets (including a temporary bucket for Spark BigQuery writes). If not public, ensure the Service Account has access.
* **BigQuery:** Create the required datasets (e.g., `bronze_stage`, `silver_stage`, `gold_stage`, etc.) before triggering the DAGs.
* **Service Account (SA) Roles:** The SA attached to the VM must have the following roles:
  * `BigQuery Data Editor`
  * `BigQuery Job User`
  * `BigQuery User`
  * `Storage Object Admin`
  * `Organization Administrator` *(Note: Use with caution, apply only if strictly required by your org policies).*

---

## 2. System Dependencies & Java Setup

Update the OS and install Python, PostgreSQL, and Java (JVM) 17 required for Spark.

```bash
# Update and install basic packages
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv wget postgresql postgresql-contrib

# Install OpenJDK 17 for PySpark
sudo apt-get install -y openjdk-17-jdk 

# Configure Java Environment Variables
echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

# Apply changes immediately
source ~/.bashrc
``` 
### 3. PostgreSQL Database Configuration
Do not use SQLite as the metadata database in production. Configure PostgreSQL for Airflow.

```bash
# Access PostgreSQL prompt
sudo -u postgres psql
```
Execute the following SQL commands inside the psql prompt:
```SQL 
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
-- Transfer ownership (Required for newer PostgreSQL versions)
ALTER DATABASE airflow_db OWNER TO airflow_user;
\q
```
### 4. Airflow Setup & Initialization
Create a Python virtual environment, install dependencies, and configure Airflow.
```bash
# Create and activate virtual environment
python3 -m venv dwh-airflow
source dwh-airflow/bin/activate

# Upgrade pip and install requirements
pip install --upgrade pip
pip install -r requirements.txt --force-reinstall

# Set Airflow Environment Variables
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/home/minhthanh2004kid/airflow/dags
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db
export AIRFLOW__CORE__LOAD_EXAMPLES=False # Hides default Airflow example DAGs

# Initialize the Airflow Metadata Database
airflow db init

# Create an Admin user for the Web UI
airflow users create \
    --username admin \
    --firstname Thanh \
    --lastname Le \
    --role Admin \
    --email admin@leetun.com
# Enter password when prompted (e.g., admin123)
```

### 5. Download Spark Connectors
Download the required JAR files to allow PySpark to communicate with BigQuery and Google Cloud Storage.
```bash
# Create target directory if it doesn't exist
mkdir -p /home/minhthanh2004kid/airflow/dags/pipeline_using_pyspark/

# Download BigQuery Connector
wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar -O /home/minhthanh2004kid/airflow/dags/pipeline_using_pyspark/spark-bigquery.jar

# Download GCS Connector
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -O /home/minhthanh2004kid/airflow/dags/pipeline_using_pyspark/gcs-connector.jar
```

### 6. Project Directory Structure
Use Linux commands (e.g., mkdir, touch, nano) to construct your project directories and files. Your final workspace should mirror this structure:
```plaintext
~/airflow/dags/
    |-- dag_dwh.py
    |-- dag_optimize.py
    |-- pipeline/
        |-- el_to_bronze.py
        |-- transform_to_silver.py
        |-- transform_to_gold.py
    |-- pipeline_using_pyspark/
        |-- spark_config.py
        |-- el_to_bronze.py
        |-- transform_to_silver.py
        |-- transform_to_gold.py
        |-- gcs-connector.jar
        |-- spark-bigquery.jar
```

### 7. Starting the Services
Once everything is configured, start the Airflow Scheduler and Webserver as background daemon processes (-D).
```bash
# Start Airflow services in the background
airflow scheduler -D
airflow webserver -p 8080 -D
```
Access the UI:
Open your web browser and navigate to:
http://<your-vm-external-ip>:8080

(Note: Alternatively, airflow standalone can be used for quick local testing as it initializes the DB, starts the scheduler, and launches the web server in a single command, but it is not recommended for production).

### 8. Troubleshooting & Maintenance
- Killing Old Airflow Processes: If you need to restart or shut down Airflow, use the following commands:
```bash
pkill -f airflow 
# Or kill by specific PID: kill -9 <PID>
```
- Production Executors: Ensure you are using LocalExecutor (configured in Step 4) or CeleryExecutor for parallel task execution. SequentialExecutor should never be used in a production environment.

## Results & Performance Comparison

This implementation compares the performance of the data pipeline using a distributed engine (PySpark) versus a traditional single-node processing method (Pandas).

The results indicate that the run duration using Pandas was a major bottleneck for large datasets. By migrating the data transformations to PySpark, the total execution time was drastically reduced from **58 minutes 6 seconds** down to **8 minutes 17 seconds** (an approximate **7x speedup**).

### 1. Airflow DAG Graphs
Structure comparison of the orchestrated pipelines:
* [DAG Graph - Pandas Implementation](image/dag_graph_pandas.png)
* [DAG Graph - PySpark Implementation](image/dag_graph_spark.png)

### 2. Total Run Duration
Comparison of the end-to-end pipeline execution time:
* **Pandas Engine:** 58m 06s ➡️ [View Run Duration](image/run_duration_pandas.png)
* **PySpark Engine:** 08m 17s ➡️ [View Run Duration](image/run_duration_spark.png)

### 3. Task-Level Duration
Detailed breakdowns of individual task execution times within the DAG:
* [Task Duration Breakdown - Pandas](image/task_duration_pandas.png)
* [Task Duration Breakdown - PySpark](image/task_duration_spark.png)

---
**Conclusion:** The benchmark clearly demonstrates PySpark's superiority in handling large-scale data transformations within GCP. The distributed nature of Spark effectively eliminates the memory and compute bottlenecks encountered with Pandas.

## Business Intelligence

