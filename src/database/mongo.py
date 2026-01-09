"""
MongoDB Connection Manager for Quanta-RAG
Implements Singleton pattern for reliable connection management
"""

import os
from typing import Optional, Dict, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class MongoDBManager:
    """
    Singleton MongoDB connection manager with robust error handling
    """
    
    _instance: Optional['MongoDBManager'] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None
    
    def __new__(cls) -> 'MongoDBManager':
        """Implement Singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize MongoDB connection if not already initialized"""
        if self._client is None:
            self._connect()
    
    def _connect(self) -> None:
        """Establish MongoDB connection with error handling"""
        try:
            mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
            db_name = os.getenv('MONGO_DB', 'quanta_rag')
            
            self._client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            
            # Verify connection
            self._client.admin.command('ping')
            
            self._db = self._client[db_name]
            logger.info(f"Successfully connected to MongoDB: {db_name}")
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            raise
    
    @property
    def db(self) -> Database:
        """Get database instance"""
        if self._db is None:
            raise RuntimeError("MongoDB connection not initialized")
        return self._db
    
    @property
    def papers(self) -> Collection:
        """Get papers collection"""
        return self.db['papers']
    
    def upsert_paper(self, data: Dict[str, Any]) -> bool:
        """
        Upsert paper data to MongoDB with idempotency
        
        Args:
            data: Paper metadata dictionary (must contain 'id' field)
        
        Returns:
            bool: True if operation succeeded
        
        Raises:
            ValueError: If 'id' field is missing from data
        """
        if 'id' not in data:
            raise ValueError("Paper data must contain 'id' field")
        
        try:
            paper_id = data['id']
            
            # Use update_one with upsert=True for idempotency
            result = self.papers.update_one(
                {'id': paper_id},
                {'$set': data},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new paper: {paper_id}")
            elif result.modified_count > 0:
                logger.info(f"Updated existing paper: {paper_id}")
            else:
                logger.debug(f"Paper unchanged: {paper_id}")
            
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate key for paper: {data.get('id')}")
            return False
        except Exception as e:
            logger.error(f"Error upserting paper {data.get('id')}: {e}")
            return False
    
    def get_paper(self, paper_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve paper by ID
        
        Args:
            paper_id: Arxiv paper ID
        
        Returns:
            Paper document or None if not found
        """
        try:
            return self.papers.find_one({'id': paper_id})
        except Exception as e:
            logger.error(f"Error retrieving paper {paper_id}: {e}")
            return None
    
    def close(self) -> None:
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")
            self._client = None
            self._db = None
    
    def __enter__(self) -> 'MongoDBManager':
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()
