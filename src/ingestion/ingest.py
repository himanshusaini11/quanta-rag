"""
Asynchronous Ingestion Engine for Quanta-RAG
Downloads and indexes Arxiv papers with concurrent execution
"""

import os
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

import arxiv
import aiohttp
from loguru import logger
from tqdm.asyncio import tqdm_asyncio

from src.database.mongo import MongoDBManager


class IngestionEngine:
    """
    Asynchronous paper ingestion engine with rate limiting
    """
    
    def __init__(
        self,
        data_dir: str = "data/raw",
        max_concurrent: int = 5
    ) -> None:
        """
        Initialize ingestion engine
        
        Args:
            data_dir: Directory to save downloaded PDFs
            max_concurrent: Maximum concurrent downloads (semaphore limit)
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        self.db_manager = MongoDBManager()
        
        logger.info(f"Initialized IngestionEngine with max_concurrent={max_concurrent}")
    
    def search_papers(
        self,
        query: str,
        max_results: int = 10,
        sort_by: arxiv.SortCriterion = arxiv.SortCriterion.SubmittedDate
    ) -> List[arxiv.Result]:
        """
        Search Arxiv for papers
        
        Args:
            query: Search query string
            max_results: Maximum number of results
            sort_by: Sort criterion
        
        Returns:
            List of arxiv.Result objects
        """
        logger.info(f"Searching Arxiv: '{query}' (max_results={max_results})")
        
        try:
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=sort_by
            )
            
            results = list(search.results())
            logger.info(f"Found {len(results)} papers")
            return results
            
        except Exception as e:
            logger.error(f"Error searching Arxiv: {e}")
            return []
    
    def _extract_metadata(self, paper: arxiv.Result) -> Dict[str, Any]:
        """
        Extract metadata from arxiv.Result
        
        Args:
            paper: Arxiv paper result
        
        Returns:
            Dictionary of paper metadata
        """
        return {
            'id': paper.entry_id.split('/')[-1],
            'title': paper.title,
            'authors': [author.name for author in paper.authors],
            'summary': paper.summary,
            'published': paper.published.isoformat(),
            'updated': paper.updated.isoformat(),
            'categories': paper.categories,
            'primary_category': paper.primary_category,
            'pdf_url': paper.pdf_url,
            'ingested_at': datetime.utcnow().isoformat(),
        }
    
    async def _download_pdf(
        self,
        session: aiohttp.ClientSession,
        paper: arxiv.Result
    ) -> bool:
        """
        Download PDF for a single paper with semaphore rate limiting
        
        Args:
            session: aiohttp session
            paper: Arxiv paper result
        
        Returns:
            bool: True if download succeeded
        """
        paper_id = paper.entry_id.split('/')[-1]
        pdf_path = self.data_dir / f"{paper_id}.pdf"
        
        # Skip if already downloaded
        if pdf_path.exists():
            logger.debug(f"PDF already exists: {paper_id}")
            return True
        
        async with self.semaphore:
            try:
                logger.debug(f"Downloading PDF: {paper_id}")
                
                async with session.get(paper.pdf_url, timeout=30) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Save PDF
                        pdf_path.write_bytes(content)
                        logger.info(f"Successfully downloaded: {paper_id}")
                        return True
                    else:
                        logger.warning(
                            f"Failed to download {paper_id}: "
                            f"HTTP {response.status}"
                        )
                        return False
                        
            except asyncio.TimeoutError:
                logger.error(f"Timeout downloading {paper_id}")
                return False
            except Exception as e:
                logger.error(f"Error downloading {paper_id}: {e}")
                return False
    
    async def _ingest_paper(
        self,
        session: aiohttp.ClientSession,
        paper: arxiv.Result
    ) -> Dict[str, Any]:
        """
        Ingest a single paper (metadata + PDF)
        
        Args:
            session: aiohttp session
            paper: Arxiv paper result
        
        Returns:
            Result dictionary with status
        """
        paper_id = paper.entry_id.split('/')[-1]
        
        try:
            # Extract and save metadata
            metadata = self._extract_metadata(paper)
            self.db_manager.upsert_paper(metadata)
            
            # Download PDF
            pdf_success = await self._download_pdf(session, paper)
            
            return {
                'paper_id': paper_id,
                'success': True,
                'pdf_downloaded': pdf_success
            }
            
        except Exception as e:
            logger.error(f"Error ingesting paper {paper_id}: {e}")
            return {
                'paper_id': paper_id,
                'success': False,
                'error': str(e)
            }
    
    async def ingest_papers_async(
        self,
        papers: List[arxiv.Result]
    ) -> List[Dict[str, Any]]:
        """
        Ingest multiple papers asynchronously
        
        Args:
            papers: List of arxiv.Result objects
        
        Returns:
            List of result dictionaries
        """
        logger.info(f"Starting async ingestion of {len(papers)} papers")
        
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._ingest_paper(session, paper)
                for paper in papers
            ]
            
            # Execute with progress bar
            results = await tqdm_asyncio.gather(
                *tasks,
                desc="Ingesting papers"
            )
        
        # Log summary
        successful = sum(1 for r in results if r['success'])
        logger.info(
            f"Ingestion complete: {successful}/{len(papers)} successful"
        )
        
        return results
    
    def ingest_papers(
        self,
        query: str,
        max_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for paper ingestion
        
        Args:
            query: Search query
            max_results: Maximum papers to ingest
        
        Returns:
            List of ingestion results
        """
        # Search for papers
        papers = self.search_papers(query, max_results)
        
        if not papers:
            logger.warning("No papers found")
            return []
        
        # Run async ingestion
        results = asyncio.run(self.ingest_papers_async(papers))
        
        return results


if __name__ == "__main__":
    # Example usage
    logger.add("logs/ingestion.log", rotation="1 MB")
    
    engine = IngestionEngine(max_concurrent=5)
    
    # Ingest papers on machine learning
    results = engine.ingest_papers(
        query="machine learning",
        max_results=5
    )
    
    print(f"\nIngestion Summary:")
    for result in results:
        status = "SUCCESS" if result['success'] else "FAILED"
        print(f"  {result['paper_id']}: {status}")
