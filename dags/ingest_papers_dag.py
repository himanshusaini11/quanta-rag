"""
Quanta-RAG Paper Ingestion DAG (Phase 3 - with OpenSearch Integration)

This DAG orchestrates the daily ingestion of Agentic RAG papers:
0. Initialize database and create tables
1. Fetch metadata from Arxiv
2. Check idempotency (filter existing papers)
3. Download and parse PDFs in parallel (Dynamic Task Mapping)
4. Store parsed content to PostgreSQL
5. Index papers to OpenSearch for keyword search (NEW - Phase 3)
"""
import sys
import os
# EXPLICITLY tell Python where the 'src' folder is
sys.path.insert(0, "/opt/airflow")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

from tasks.ingestion_tasks import (
    init_db_task,
    fetch_metadata_task,
    check_idempotency_task,
    download_and_parse_task,
    store_to_db_task,
    index_papers_task
)


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
    description='Daily ingestion of Agentic RAG papers with OpenSearch indexing',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'arxiv', 'agentic-rag', 'opensearch', 'phase3'],
    max_active_runs=1,
) as dag:
    
    # Start marker
    start = EmptyOperator(
        task_id='start',
        dag=dag,
    )
    
    # Step 0: Initialize database (CREATE TABLES FIRST!)
    @task
    def init_db():
        """Initialize database and create all tables"""
        return init_db_task()
    
    # Step 1: Fetch metadata from Arxiv
    @task
    def fetch_metadata():
        """Fetch Agentic RAG papers from Arxiv"""
        return fetch_metadata_task(
            query="Agentic RAG",
            max_results=5
        )
    
    # Step 2: Check idempotency (filter existing papers)
    @task
    def check_idempotency(papers):
        """Filter out papers that already exist in database"""
        return check_idempotency_task(papers=papers)
    
    # Step 3: Download and parse PDFs (Dynamic Task Mapping)
    @task(
            max_active_tis_per_dag=1,  # FORCE: Only 1 PDF parser runs at a time
            retries=3,
            retry_delay=timedelta(seconds=30)
        ) # type: ignore
    def download_and_parse(paper):
        """Download PDF and parse with Docling"""
        return download_and_parse_task(paper=paper)
    
    # Step 4: Store to database (Dynamic Task Mapping)
    @task
    def store_to_db(data):
        """Store parsed content to PostgreSQL"""
        return store_to_db_task(data=data)
    
    # Step 5: Index papers to OpenSearch (PHASE 3 - NEW!)
    @task(
            retries=5, 
            retry_delay=timedelta(minutes=1) # Give OpenSearch time to wake up
         ) # type: ignore
    def index_papers():
        """Index all papers to OpenSearch for keyword search"""
        return index_papers_task()
    
    # End marker
    end = EmptyOperator(
        task_id='end',
        dag=dag,
    )
    
    # DAG flow with dynamic task mapping
    db_ready = init_db()
    papers = fetch_metadata()
    new_papers = check_idempotency(papers)
    
    # Dynamic Task Mapping: Create one task per paper
    # .expand() creates parallel tasks for each item in the list
    processed_data = download_and_parse.expand(paper=new_papers)
    
    # Store each processed paper to database in parallel
    stored = store_to_db.expand(data=processed_data)
    
    # Index all papers to OpenSearch
    indexed = index_papers()
    
    # Define dependencies - NEW: added index_papers after stored
    start >> db_ready >> papers >> new_papers >> processed_data >> stored >> indexed >> end
