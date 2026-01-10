"""
Airflow tasks for paper ingestion pipeline (Phase 2 Implementation)
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

import arxiv
import aiohttp
import asyncio
from loguru import logger
from sqlalchemy.exc import IntegrityError
from tenacity import retry, stop_after_attempt, wait_exponential

from src.database.session import DatabaseSession
from src.database.models import Paper
from src.ingestion.parser import DoclingParser


def init_db_task(**kwargs) -> bool:
    """
    Initialize database and create all tables
    
    This task MUST run before any other database operations
    to ensure the 'papers' table exists.
    
    Args:
        **kwargs: Airflow context
    
    Returns:
        True if successful
    """
    logger.info("Initializing database and creating tables...")
    
    try:
        DatabaseSession.initialize()
        DatabaseSession.create_tables()
        logger.info("Database initialized successfully - tables created")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise



def fetch_metadata_task(query: str = "Agentic RAG", max_results: int = 5, **kwargs) -> List[Dict[str, Any]]:
    """
    Fetch paper metadata from Arxiv (Quantum Computing papers)
    
    Args:
        query: Arxiv search query (default: Quantum Computing)
        max_results: Maximum number of results
        **kwargs: Airflow context (ti, execution_date, etc.)
    
    Returns:
        List of paper metadata dictionaries
    """
    logger.info(f"Fetching papers: query='{query}', max_results={max_results}")
    
    try:
        # Search Arxiv for Quantum Computing papers
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate
        )
        
        papers = []
        for result in search.results():
            paper_data = {
                'arxiv_id': result.entry_id.split('/')[-1],
                'title': result.title,
                'summary': result.summary,
                'published_date': result.published.isoformat(),
                'pdf_url': result.pdf_url,
                'authors': [author.name for author in result.authors],
                'categories': result.categories,
            }
            papers.append(paper_data)
        
        logger.info(f"Fetched {len(papers)} Quantum Computing papers")
        
        return papers
        
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}")
        raise


def check_idempotency_task(papers: List[Dict[str, Any]] = None, **kwargs) -> List[Dict[str, Any]]:
    """
    Filter out papers that already exist in the database
    
    Args:
        papers: List of paper metadata from fetch_metadata_task
        **kwargs: Airflow context
    
    Returns:
        List of new papers that don't exist in database
    """
    if not papers:
        logger.warning("No papers to check")
        return []
    
    logger.info(f"Checking idempotency for {len(papers)} papers")
    
    # Initialize database
    DatabaseSession.initialize()
    
    new_papers = []
    
    with DatabaseSession.session_scope() as session:
        for paper_data in papers:
            arxiv_id = paper_data['arxiv_id']
            
            # Check if paper already exists
            existing_paper = session.query(Paper).filter_by(
                arxiv_id=arxiv_id
            ).first()
            
            if existing_paper:
                logger.debug(f"Paper already exists: {arxiv_id}")
            else:
                logger.debug(f"New paper found: {arxiv_id}")
                new_papers.append(paper_data)
    
    logger.info(
        f"Idempotency check complete: {len(new_papers)} new papers, "
        f"{len(papers) - len(new_papers)} duplicates filtered"
    )
    
    return new_papers


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def download_and_parse_task(paper: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Download PDF and parse with Docling (with retries)
    
    Args:
        paper: Single paper metadata dictionary
        **kwargs: Airflow context
    
    Returns:
        Paper data with parsed content
    """
    arxiv_id = paper['arxiv_id']
    logger.info(f"Processing paper: {arxiv_id}")
    
    # Setup paths
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = data_dir / f"{arxiv_id}.pdf"
    
    try:
        # Step 1: Download PDF if not exists
        if not pdf_path.exists():
            logger.info(f"Downloading PDF: {arxiv_id}")
            
            import requests
            response = requests.get(paper['pdf_url'], timeout=30)
            response.raise_for_status()
            
            pdf_path.write_bytes(response.content)
            logger.info(f"Downloaded: {arxiv_id} ({len(response.content)} bytes)")
        else:
            logger.debug(f"PDF already exists: {arxiv_id}")
        
        # Step 2: Parse PDF with Docling
        logger.info(f"Parsing PDF with Docling: {arxiv_id}")
        parser = DoclingParser()
        parsed_content = parser.parse_pdf(str(pdf_path))
        
        # Step 3: Combine metadata and parsed content
        result = {
            **paper,  # Original metadata
            'pdf_path': str(pdf_path),
            'full_text': parsed_content['full_text'],
            'sections': parsed_content['sections'],
            'parse_metadata': parsed_content['metadata']
        }
        
        logger.info(
            f"Parsed {arxiv_id}: {len(parsed_content['full_text'])} chars, "
            f"{len(parsed_content['sections'])} sections"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing {arxiv_id}: {e}")
        # Return paper with empty content on failure
        return {
            **paper,
            'pdf_path': str(pdf_path) if pdf_path.exists() else None,
            'full_text': '',
            'sections': [],
            'parse_metadata': {'error': str(e)}
        }


def store_to_db_task(data: Dict[str, Any], **kwargs) -> bool:
    """
    Store paper metadata and parsed content to PostgreSQL
    
    Args:
        data: Paper data with parsed content
        **kwargs: Airflow context
    
    Returns:
        True if successful
    """
    arxiv_id = data['arxiv_id']
    logger.info(f"Storing to database: {arxiv_id}")
    
    # Initialize database
    DatabaseSession.initialize()
    DatabaseSession.create_tables()
    
    try:
        with DatabaseSession.session_scope() as session:
            # Check if paper exists
            existing_paper = session.query(Paper).filter_by(
                arxiv_id=arxiv_id
            ).first()
            
            # Convert sections to JSON string
            sections_json = json.dumps(data.get('sections', []))
            
            if existing_paper:
                # Update existing paper
                existing_paper.title = data['title']
                existing_paper.summary = data.get('summary')
                existing_paper.pdf_path = data.get('pdf_path')
                existing_paper.full_text = data.get('full_text')
                existing_paper.sections_json = sections_json
                existing_paper.published_date = datetime.fromisoformat(
                    data['published_date']
                )
                logger.info(f"Updated existing paper: {arxiv_id}")
            else:
                # Insert new paper
                new_paper = Paper(
                    arxiv_id=arxiv_id,
                    title=data['title'],
                    summary=data.get('summary'),
                    pdf_path=data.get('pdf_path'),
                    full_text=data.get('full_text'),
                    sections_json=sections_json,
                    published_date=datetime.fromisoformat(
                        data['published_date']
                    ),
                )
                session.add(new_paper)
                logger.info(f"Inserted new paper: {arxiv_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error storing {arxiv_id}: {e}")
        raise
