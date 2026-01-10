"""
OpenSearch client for Quanta-RAG (Production-Grade with Self-Healing)
Handles indexing and searching of research papers with automatic bootstrapping
"""

import os
import time
from typing import List, Dict, Any, Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class QuantaSearchClient:
    """
    Self-healing OpenSearch client for paper search using BM25
    Automatically creates indexes and handles connection retries
    """
    
    def __init__(self, host: str = None, max_retries: int = 3):
        """
        Initialize OpenSearch client with self-healing capabilities
        
        Args:
            host: OpenSearch host URL (defaults to env variable)
            max_retries: Maximum connection retry attempts
        """
        self.host = host or os.getenv('OPENSEARCH_HOST', 'http://localhost:9200')
        self.max_retries = max_retries
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize OpenSearch connection with retries"""
        for attempt in range(1, self.max_retries + 1):
            try:
                from opensearchpy import OpenSearch
                
                # Parse host URL
                if self.host.startswith('http://') or self.host.startswith('https://'):
                    host_parts = self.host.replace('http://', '').replace('https://', '').split(':')
                    host_name = host_parts[0]
                    host_port = int(host_parts[1]) if len(host_parts) > 1 else 9200
                else:
                    host_name = self.host
                    host_port = 9200
                
                self.client = OpenSearch(
                    hosts=[{'host': host_name, 'port': host_port}],
                    http_compress=True,
                    use_ssl=False,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=3,
                    retry_on_timeout=True
                )
                
                # Test connection
                info = self.client.info()
                logger.info(f"Connected to OpenSearch: {info['version']['number']}")
                return
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to OpenSearch after {self.max_retries} attempts")
                    raise
    
    def _ensure_index(self, index_name: str) -> bool:
        """
        SELF-HEALING: Ensure index exists, create if missing
        
        This method is called before ANY upsert or search operation
        to guarantee the index is always available.
        
        Args:
            index_name: Name of the index to check/create
        
        Returns:
            True if index exists or was created successfully
        """
        try:
            # Check if index exists
            if self.client.indices.exists(index=index_name):
                logger.debug(f"Index '{index_name}' exists")
                return True
            
            # Index doesn't exist - create it automatically
            logger.warning(f"Index '{index_name}' not found - auto-creating...")
            
            # Import configuration
            from src.services.opensearch.index_config import get_index_config
            
            index_config = get_index_config()
            
            # Create index
            self.client.indices.create(
                index=index_name,
                body=index_config
            )
            
            logger.info(f"✅ Auto-created index '{index_name}' with production mappings")
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring index '{index_name}': {e}")
            return False
    
    def upsert_paper(self, paper_data: Dict[str, Any], index_name: str = "arxiv-papers") -> bool:
        """
        Upsert a paper to OpenSearch (with auto-bootstrapping)
        
        Args:
            paper_data: Paper metadata and content
            index_name: Target index name
        
        Returns:
            True if successful
        """
        try:
            # SELF-HEALING: Ensure index exists before upserting
            if not self._ensure_index(index_name):
                raise Exception(f"Failed to ensure index '{index_name}' exists")
            
            if 'arxiv_id' not in paper_data:
                raise ValueError("paper_data must contain 'arxiv_id'")
            
            arxiv_id = paper_data['arxiv_id']
            
            # Prepare document for indexing
            document = {
                'arxiv_id': arxiv_id,
                'title': paper_data.get('title', ''),
                'full_text': paper_data.get('full_text', ''),
                'summary': paper_data.get('summary', ''),
                'published_date': paper_data.get('published_date'),
                'created_at': paper_data.get('created_at'),
                'authors': paper_data.get('authors', []),
                'categories': paper_data.get('categories', []),
            }
            
            # Upsert document (use arxiv_id as document ID for idempotency)
            response = self.client.index(
                index=index_name,
                id=arxiv_id,
                body=document,
                refresh=True  # Make immediately searchable
            )
            
            logger.info(f"Upserted paper '{arxiv_id}' to OpenSearch: {response['result']}")
            return True
            
        except Exception as e:
            logger.error(f"Error upserting paper '{paper_data.get('arxiv_id')}': {e}")
            return False
    
    def search(
        self,
        query_text: str,
        index_name: str = "arxiv-papers",
        limit: int = 5,
        fields: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search papers using BM25 algorithm (with auto-bootstrapping)
        
        Args:
            query_text: Search query
            index_name: Index to search
            limit: Maximum results to return
            fields: Fields to search (defaults to ['full_text', 'title'])
        
        Returns:
            List of matching papers with scores
        """
        if fields is None:
            fields = ['full_text', 'title', 'summary']
        
        try:
            # SELF-HEALING: Ensure index exists before searching
            if not self._ensure_index(index_name):
                logger.warning(f"Index '{index_name}' doesn't exist - returning empty results")
                return []
            
            # BM25 multi-match query
            query = {
                "size": limit,
                "query": {
                    "multi_match": {
                        "query": query_text,
                        "fields": fields,
                        "type": "best_fields",  # BM25 scoring
                        "operator": "or"
                    }
                },
                "_source": [
                    "arxiv_id",
                    "title", 
                    "summary",
                    "published_date",
                    "authors"
                ]
            }
            
            response = self.client.search(
                index=index_name,
                body=query
            )
            
            # Extract results
            results = []
            for hit in response['hits']['hits']:
                result = {
                    'arxiv_id': hit['_source']['arxiv_id'],
                    'title': hit['_source']['title'],
                    'summary': hit['_source'].get('summary', ''),
                    'published_date': hit['_source'].get('published_date'),
                    'authors': hit['_source'].get('authors', []),
                    'score': hit['_score']
                }
                results.append(result)
            
            logger.info(f"Search for '{query_text}': {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error searching for '{query_text}': {e}")
            return []
    
    def get_paper_count(self, index_name: str = "arxiv-papers") -> int:
        """
        Get total number of papers in index
        
        Args:
            index_name: Index name
        
        Returns:
            Paper count
        """
        try:
            # Ensure index exists first
            if not self._ensure_index(index_name):
                return 0
            
            response = self.client.count(index=index_name)
            return response['count']
        except Exception as e:
            logger.error(f"Error counting papers: {e}")
            return 0
    
    def close(self):
        """Close OpenSearch connection"""
        if self.client:
            self.client.close()
            logger.info("OpenSearch connection closed")


if __name__ == "__main__":
    # Test the self-healing client
    client = QuantaSearchClient()
    
    # Test paper count (will auto-create index if missing)
    count = client.get_paper_count()
    print(f"\nPapers in index: {count}")
    
    # Test search (will auto-create index if missing)
    results = client.search("quantum computing", limit=5)
    print(f"Found {len(results)} results")
