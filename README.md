# LOAN RISK DATA WAREHOUSE

## Introduction
About Data:
application_{train|test}.csv

This is the main table, broken into two files for Train (with TARGET) and Test (without TARGET).
Static data for all applications. One row represents one loan in our data sample.
bureau.csv

All client's previous credits provided by other financial institutions that were reported to Credit Bureau (for clients who have a loan in our sample).
For every loan in our sample, there are as many rows as number of credits the client had in Credit Bureau before the application date.
bureau_balance.csv

Monthly balances of previous credits in Credit Bureau.
This table has one row for each month of history of every previous credit reported to Credit Bureau – i.e the table has (#loans in sample * # of relative previous credits * # of months where we have some history observable for the previous credits) rows.
POS_CASH_balance.csv

Monthly balance snapshots of previous POS (point of sales) and cash loans that the applicant had with Home Credit.
This table has one row for each month of history of every previous credit in Home Credit (consumer credit and cash loans) related to loans in our sample – i.e. the table has (#loans in sample * # of relative previous credits * # of months in which we have some history observable for the previous credits) rows.
credit_card_balance.csv

Monthly balance snapshots of previous credit cards that the applicant has with Home Credit.
This table has one row for each month of history of every previous credit in Home Credit (consumer credit and cash loans) related to loans in our sample – i.e. the table has (#loans in sample * # of relative previous credit cards * # of months where we have some history observable for the previous credit card) rows.
previous_application.csv

All previous applications for Home Credit loans of clients who have loans in our sample.
There is one row for each previous application related to loans in our data sample.
installments_payments.csv

Repayment history for the previously disbursed credits in Home Credit related to the loans in our sample.
There is a) one row for every payment that was made plus b) one row each for missed payment.
One row is equivalent to one payment of one installment OR one installment corresponding to one payment of one previous Home Credit credit related to loans in our sample.
HomeCredit_columns_description.csv

This file contains descriptions for the columns in the various data files.

<img src = 'image/home_credit.png'>

## Architecture

<img src ='image/architecture.png'>
## Data Warehouse Pipeline Setup on GCP (Airflow + PySpark)

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


## Pipeline 

## Result
This implementation compared the performance between spark engine and traditional processing data method (pandas)

The result indicated time run duration of pandas too high and when i used pyspark instead, the time performance reduce from 58 minutes 6 seconds to 8 minutes 17 seconds

