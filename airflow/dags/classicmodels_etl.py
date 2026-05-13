"""
ClassicModels MySQL to Iceberg ETL Pipeline

This DAG orchestrates the ETL pipeline that:
1. Waits for MySQL and MinIO to be healthy
2. Creates the S3 bucket if not exists
3. Runs the Spark ETL job to sync data from MySQL to Iceberg
4. Optionally runs a query to verify the results

Schedule: Every 5 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from requests.exceptions import RequestException


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def check_mysql_health(**context):
    """Check if MySQL is healthy by making a connection test"""
    import mysql.connector
    try:
        conn = mysql.connector.connect(
            host="mysql",
            port=3306,
            user="etl",
            password="etl",
            database="classicmodels"
        )
        conn.close()
        return True
    except Exception as e:
        print(f"MySQL health check failed: {e}")
        raise


def wait_for_minio(**context):
    """Wait for MinIO to be available"""
    import time
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get("http://minio:9000/minio/health/live", timeout=5)
            if response.status_code == 200:
                print("MinIO is healthy")
                return True
        except RequestException:
            pass
        print(f"Waiting for MinIO... attempt {i + 1}/{max_retries}")
        time.sleep(2)
    raise Exception("MinIO health check failed after max retries")


def create_warehouse_bucket(**context):
    """Create the warehouse bucket in MinIO if it doesn't exist"""
    import subprocess
    try:
        # Set up mc alias
        subprocess.run([
            "mc", "alias", "set", "lakehouse",
            "http://minio:9000",
            "minioadmin", "minioadmin"
        ], check=True, capture_output=True)

        # Create bucket (idempotent with --ignore-existing)
        subprocess.run([
            "mc", "mb", "--ignore-existing", "lakehouse/warehouse"
        ], check=True, capture_output=True)

        print("Warehouse bucket ready")
    except subprocess.CalledProcessError as e:
        print(f"Bucket creation warning: {e}")


with DAG(
    dag_id="classicmodels_etl_sync",
    default_args=default_args,
    description="ETL pipeline to sync ClassicModels MySQL to Iceberg on MinIO",
    schedule="@every 5 minutes",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "iceberg", "classicmodels"],
    max_active_runs=1,  # Prevent concurrent runs
) as dag:

    # Task 1: Wait for MySQL to be healthy
    wait_mysql = PythonOperator(
        task_id="wait_for_mysql",
        python_callable=check_mysql_health,
        retries=10,
        retry_delay=timedelta(seconds=10),
    )

    # Task 2: Wait for MinIO to be available
    wait_minio = PythonOperator(
        task_id="wait_for_minio",
        python_callable=wait_for_minio,
        retries=10,
        retry_delay=timedelta(seconds=10),
    )

    # Task 3: Create warehouse bucket
    create_bucket = PythonOperator(
        task_id="create_warehouse_bucket",
        python_callable=create_warehouse_bucket,
    )

    # Task 4: Run the ETL pipeline
    run_etl = BashOperator(
        task_id="run_etl",
        bash_command="""
        export SPARK_HOME=/opt/spark
        export PATH=$SPARK_HOME/bin:$PATH
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

        spark-submit \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.mysql:mysql-connector-j:8.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://warehouse/iceberg \
            --conf spark.sql.defaultCatalog=local \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.sql.defaultCatalog=local \
            /opt/spark/jobs/build_star_schema.py
        """,
        env={
            "MYSQL_HOST": "mysql",
            "MYSQL_PORT": "3306",
            "MYSQL_DATABASE": "classicmodels",
            "MYSQL_USER": "etl",
            "MYSQL_PASSWORD": "etl",
            "ICEBERG_CATALOG": "local",
            "ICEBERG_WAREHOUSE": "s3a://warehouse/iceberg",
            "ICEBERG_BRONZE_NAMESPACE": "bronze",
            "ICEBERG_STAR_NAMESPACE": "star_schema",
        },
        retry_delay=timedelta(minutes=1),
    )

    # Task 5: Run query to verify results (optional)
    run_query = BashOperator(
        task_id="run_query",
        bash_command="""
        export SPARK_HOME=/opt/spark
        export PATH=$SPARK_HOME/bin:$PATH
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

        spark-submit \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.mysql:mysql-connector-j:8.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://warehouse/iceberg \
            --conf spark.sql.defaultCatalog=local \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.sql.defaultCatalog=local \
            /opt/spark/jobs/query_star_schema.py
        """,
        env={
            "ICEBERG_CATALOG": "local",
            "ICEBERG_STAR_NAMESPACE": "star_schema",
        },
    )

    # Define task dependencies
    wait_mysql >> wait_minio >> create_bucket >> run_etl >> run_query
