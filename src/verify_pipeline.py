"""
Verification script for Quanta-RAG Pipeline

Connects to PostgreSQL and verifies:
1. Database connection
2. Papers table exists
3. Row count

Usage:
   python src/verify_pipeline.py
"""

import os
import sys
from loguru import logger

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.session import DatabaseSession
from src.database.models import Paper
from sqlalchemy import inspect


def verify_pipeline():
    """
    Verify the pipeline setup and database status
    """
    print("\n" + "="*60)
    print("QUANTA-RAG PIPELINE VERIFICATION")
    print("="*60 + "\n")
    
    success = True
    
    # Step 1: Test database connection
    print("1. Testing database connection...")
    try:
        DatabaseSession.initialize()
        print("   ✅ Database connection: SUCCESS")
    except Exception as e:
        print(f"   ❌ Database connection: FAILED - {e}")
        success = False
        print("\n" + "="*60)
        print("Final Result: ❌ FAILED")
        print("="*60 + "\n")
        return
    
    # Step 2: Check if tables exist
    print("\n2. Checking if 'papers' table exists...")
    try:
        inspector = inspect(DatabaseSession._engine)
        tables = inspector.get_table_names()
        
        if 'papers' in tables:
            print("   ✅ Table 'papers' status: EXISTS")
        else:
            print("   ❌ Table 'papers' status: MISSING")
            print(f"   Available tables: {tables}")
            success = False
    except Exception as e:
        print(f"   ❌ Error checking tables: {e}")
        success = False
    
    # Step 3: Count rows
    print("\n3. Counting rows in 'papers' table...")
    try:
        with DatabaseSession.session_scope() as session:
            count = session.query(Paper).count()
            print(f"   ✅ Row count: {count}")
            
            if count > 0:
                # Show sample papers
                print("\n   Sample papers:")
                papers = session.query(Paper).limit(3).all()
                for paper in papers:
                    print(f"     - {paper.arxiv_id}: {paper.title[:50]}...")
    except Exception as e:
        print(f"   ❌ Error counting rows: {e}")
        success = False
    
    # Step 4: Test database write capability
    print("\n4. Testing database write capability...")
    try:
        DatabaseSession.create_tables()
        print("   ✅ create_tables() executed successfully")
    except Exception as e:
        print(f"   ⚠️  Warning during create_tables: {e}")
    
    # Final result
    print("\n" + "="*60)
    if success:
        print("Final Result: ✅ SUCCESS")
        print("="*60 + "\n")
        print("✅ Pipeline is ready to run!")
        print("   Next step: Trigger the DAG in Airflow UI")
    else:
        print("Final Result: ❌ FAILED")
        print("="*60 + "\n")
        print("❌ Pipeline has issues that need to be fixed")
        print("   Action: Check error messages above")
    
    return success


if __name__ == "__main__":
    verify_pipeline()
