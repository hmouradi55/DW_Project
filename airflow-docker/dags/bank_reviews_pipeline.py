# bank_reviews_pipeline.py
"""
Airflow DAG for Bank Reviews Data Pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import subprocess
import os

default_args = {
    'owner': 'HABA',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bank_reviews_pipeline',
    default_args=default_args,
    start_date      = datetime(2025, 1, 1),
    description='End-to-end bank reviews data pipeline',
    schedule_interval='@weekly',
    catchup=False,
    tags=['reviews', 'banks', 'nlp']
)

# Since we're in Docker, we'll use different paths
PROJECT_PATH = '/opt/airflow/project'

# Task 1: Run web scraper
def run_scraper(**context):
    """Execute the web scraper"""
    result = subprocess.run(
        ['python', f'{PROJECT_PATH}/google_maps_scraper.py'],
        capture_output=True,
        text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"Scraper failed: {result.stderr}")
    print(f"Scraper output: {result.stdout}")
    context['task_instance'].xcom_push(key='scraper_status', value='completed')

scrape_task = PythonOperator(
    task_id='scrape_reviews',
    python_callable=run_scraper,
    dag=dag
)

# Task 2: Data cleaning pipeline
def run_cleaning(**context):
    """Execute data cleaning pipeline"""
    result = subprocess.run(
        ['python', f'{PROJECT_PATH}/data_cleaning_pipeline.py'],
        capture_output=True,
        text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"Cleaning failed: {result.stderr}")
    print(f"Cleaning output: {result.stdout}")
    context['task_instance'].xcom_push(key='cleaning_status', value='completed')

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=run_cleaning,
    dag=dag
)

# Task 3: Load to PostgreSQL
def load_to_staging(**context):
    """Load cleaned data to PostgreSQL staging"""
    result = subprocess.run(
        ['python', f'{PROJECT_PATH}/load_to_postgres.py'],
        capture_output=True,
        text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"Loading failed: {result.stderr}")
    print(f"Loading output: {result.stdout}")
    context['task_instance'].xcom_push(key='staging_status', value='completed')

load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    dag=dag
)

# Task 4: NLP Analysis
def run_nlp_analysis(**context):
    """Run sentiment analysis and topic modeling"""
    result = subprocess.run(
        ['python', f'{PROJECT_PATH}/nlp_analysis.py'],
        capture_output=True,
        text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"NLP analysis failed: {result.stderr}")
    print(f"NLP output: {result.stdout}")
    context['task_instance'].xcom_push(key='nlp_status', value='completed')

nlp_task = PythonOperator(
    task_id='nlp_analysis',
    python_callable=run_nlp_analysis,
    dag=dag
)

# Task 5: DBT transformations
dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {PROJECT_PATH}/bank_reviews_dw && dbt run',
    dag=dag
)

# Task 6: Data quality checks
with TaskGroup('data_quality_checks', dag=dag) as quality_checks:
    
    check_review_count = PostgresOperator(
        task_id='check_review_count',
        postgres_conn_id='postgres_dw',
        sql="""
        SELECT 
            CASE 
                WHEN COUNT(*) > 1000 THEN 'OK'
                ELSE 'FAIL: Less than 1000 reviews'
            END as status
        FROM warehouse.fact_reviews
        WHERE loaded_at::date = CURRENT_DATE;
        """
    )
    
    check_sentiment_distribution = PostgresOperator(
        task_id='check_sentiment_distribution',
        postgres_conn_id='postgres_dw',
        sql="""
        SELECT 
            sentiment_label,
            COUNT(*) as count,
            ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 2) as percentage
        FROM analytics.sentiment_analysis
        GROUP BY sentiment_label
        ORDER BY count DESC;
        """
    )

# Define task dependencies
scrape_task >> clean_task >> load_staging_task >> nlp_task >> dbt_run_task >> quality_checks
