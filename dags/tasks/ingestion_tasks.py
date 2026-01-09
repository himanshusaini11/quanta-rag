"""
Airflow tasks for paper ingestion pipeline
"""

import os
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

import arxiv
import aiohttp
import asyncio
from loguru import logger
from sqlalchemy.exc import IntegrityError

from src.database.session import DatabaseSession
from src.database.models import Paper


def fetch_metadata(query: str, max_results: int = 10, **kwargs) -> List[Dict[str, Any]]:
    """
    Fetch paper metadata from Arxiv
    
    Args:
        query: Arxiv search query
        max_results: Maximum number of results
        **kwargs: Airflow context (ti, execution_date, etc.)
    
    Returns:
        List of paper metadata dictionaries
    """
    logger.info(f"Fetching papers: query='{query}', max_results={max_results}")
    
    try:
        # Search Arxiv
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
            }
            papers.append(paper_data)
        
        logger.info(f"Fetched {len(papers)} papers")
        
        # Push to XCom for downstream tasks
        if kwargs.get('ti'):
            kwargs['ti'].xcom_push(key='papers', value=papers)
        
        return papers
        
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}")
        raise


def store_papers(papers: List[Dict[str, Any]] = None, **kwargs) -> int:
    """
    Store paper metadata in PostgreSQL with upsert logic
    
    Args:
        papers: List of paper metadata (optional, can pull from XCom)
        **kwargs: Airflow context
    
    Returns:
        Number of papers stored
    """
    # Pull from XCom if not provided
    if papers is None:
        ti = kwargs.get('ti')
        if ti:
            papers = ti.xcom_pull(key='papers', task_ids='fetch_metadata')
    
    if not papers:
        logger.warning("No papers to store")
        return 0
    
    logger.info(f"Storing {len(papers)} papers in PostgreSQL")
    
    # Initialize database
    DatabaseSession.initialize()
    DatabaseSession.create_tables()
    
    stored_count = 0
    
    with DatabaseSession.session_scope() as session:
        for paper_data in papers:
            try:
                # Check if paper already exists
                existing_paper = session.query(Paper).filter_by(
                    arxiv_id=paper_data['arxiv_id']
                ).first()
                
                if existing_paper:
                    # Update existing paper
                    existing_paper.title = paper_data['title']
                    existing_paper.summary = paper_data['summary']
                    existing_paper.published_date = datetime.fromisoformat(
                        paper_data['published_date']
                    )
                    logger.debug(f"Updated paper: {paper_data['arxiv_id']}")
                else:
                    # Insert new paper
                    new_paper = Paper(
                        arxiv_id=paper_data['arxiv_id'],
                        title=paper_data['title'],
                        summary=paper_data['summary'],
                        published_date=datetime.fromisoformat(
                            paper_data['published_date']
                        ),
                    )
                    session.add(new_paper)
                    logger.debug(f"Inserted paper: {paper_data['arxiv_id']}")
                
                stored_count += 1
                
            except IntegrityError as e:
                logger.warning(f"Integrity error for {paper_data['arxiv_id']}: {e}")
                session.rollback()
            except Exception as e:
                logger.error(f"Error storing paper {paper_data['arxiv_id']}: {e}")
                raise
    
    logger.info(f"Successfully stored {stored_count} papers")
    
    # Push papers to XCom for download task
    if kwargs.get('ti'):
        kwargs['ti'].xcom_push(key='papers', value=papers)
    
    return stored_count


async def _download_single_pdf(
    session: aiohttp.ClientSession,
    paper: Dict[str, Any],
    data_dir: Path,
    semaphore: asyncio.Semaphore
) -> bool:
    """
    Download a single PDF asynchronously
    
    Args:
        session: aiohttp session
        paper: Paper metadata dict
        data_dir: Directory to save PDFs
        semaphore: Concurrency limiter
    
    Returns:
        True if successful
    """
    arxiv_id = paper['arxiv_id']
    pdf_path = data_dir / f"{arxiv_id}.pdf"
    
    # Skip if already exists
    if pdf_path.exists():
        logger.debug(f"PDF already exists: {arxiv_id}")
        return True
    
    async with semaphore:
        try:
            async with session.get(paper['pdf_url'], timeout=30) as response:
                if response.status == 200:
                    content = await response.read()
                    pdf_path.write_bytes(content)
                    logger.info(f"Downloaded PDF: {arxiv_id}")
                    
                    # Update database with PDF path
                    DatabaseSession.initialize()
                    with DatabaseSession.session_scope() as db_session:
                        db_paper = db_session.query(Paper).filter_by(
                            arxiv_id=arxiv_id
                        ).first()
                        if db_paper:
                            db_paper.pdf_path = str(pdf_path)
                    
                    return True
                else:
                    logger.warning(f"Failed to download {arxiv_id}: HTTP {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error downloading {arxiv_id}: {e}")
            return False


def download_pdfs(
    papers: List[Dict[str, Any]] = None,
    max_concurrent: int = 5,
    **kwargs
) -> Dict[str, int]:
    """
    Download PDFs for papers asynchronously
    
    Args:
        papers: List of paper metadata (optional, can pull from XCom)
        max_concurrent: Maximum concurrent downloads
        **kwargs: Airflow context
    
    Returns:
        Dictionary with download statistics
    """
    # Pull from XCom if not provided
    if papers is None:
        ti = kwargs.get('ti')
        if ti:
            papers = ti.xcom_pull(key='papers', task_ids='store_papers')
    
    if not papers:
        logger.warning("No papers to download")
        return {'total': 0, 'successful': 0, 'failed': 0}
    
    logger.info(f"Downloading {len(papers)} PDFs (max_concurrent={max_concurrent})")
    
    # Setup data directory
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Run async downloads
    async def run_downloads():
        semaphore = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            tasks = [
                _download_single_pdf(session, paper, data_dir, semaphore)
                for paper in papers
            ]
            results = await asyncio.gather(*tasks)
        return results
    
    # Execute async downloads
    results = asyncio.run(run_downloads())
    
    stats = {
        'total': len(results),
        'successful': sum(results),
        'failed': len(results) - sum(results)
    }
    
    logger.info(f"Download complete: {stats}")
    
    return stats
