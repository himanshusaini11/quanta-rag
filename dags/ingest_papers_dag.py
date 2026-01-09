"""
Quanta-RAG Paper Ingestion DAG

This DAG orchestrates the daily ingestion of Arxiv papers:
1. Fetch metadata from Arxiv
2. Store metadata in PostgreSQL
3. Download PDFs asynchronously
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from dags.tasks.ingestion_tasks import fetch_metadata, store_papers, download_pdfs


# Default arguments for all tasks
default_args = {
    'owner': 'quanta-rag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
with DAG(
    dag_id='ingest_papers_dag',
    default_args=default_args,
    description='Daily ingestion of Arxiv research papers',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'arxiv', 'etl'],
    max_active_runs=1,
) as dag:
    
    # Start marker
    start = EmptyOperator(
        task_id='start',
        dag=dag,
    )
    
    # Task 1: Fetch metadata from Arxiv
    fetch_metadata_task = PythonOperator(
        task_id='fetch_metadata',
        python_callable=fetch_metadata,
        op_kwargs={
            'query': 'cat:cs.AI OR cat:cs.LG OR cat:cs.CL',  # AI/ML/NLP papers
            'max_results': 50,
        },
        dag=dag,
    )
    
    # Task 2: Store papers in PostgreSQL
    store_papers_task = PythonOperator(
        task_id='store_papers',
        python_callable=store_papers,
        dag=dag,
    )
    
    # Task 3: Download PDFs
    download_pdfs_task = PythonOperator(
        task_id='download_pdfs',
        python_callable=download_pdfs,
        op_kwargs={
            'max_concurrent': 5,
        },
        dag=dag,
    )
    
    # End marker
    end = EmptyOperator(
        task_id='end',
        dag=dag,
    )
    
    # Define task dependencies (linear flow)
    start >> fetch_metadata_task >> store_papers_task >> download_pdfs_task >> end
