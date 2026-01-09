"""
PostgreSQL database session management for Quanta-RAG
"""

import os
from typing import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from loguru import logger
from dotenv import load_dotenv

from src.database.models import Base

load_dotenv()


class DatabaseSession:
    """
    PostgreSQL session manager with connection pooling
    """
    
    _engine = None
    _session_factory = None
    
    @classmethod
    def initialize(cls, database_url: str = None) -> None:
        """
        Initialize database engine and session factory
        
        Args:
            database_url: PostgreSQL connection URL
        """
        if cls._engine is not None:
            logger.warning("Database already initialized")
            return
        
        # Get database URL from environment or parameter
        db_url = database_url or os.getenv(
            'DATABASE_URL',
            'postgresql://quanta:quanta@localhost:5432/quanta_rag'
        )
        
        logger.info(f"Initializing database connection: {db_url.split('@')[-1]}")
        
        try:
            # Create engine with connection pooling
            cls._engine = create_engine(
                db_url,
                pool_pre_ping=True,  # Verify connections before using
                pool_size=10,
                max_overflow=20,
                echo=False  # Set to True for SQL debugging
            )
            
            # Create session factory
            cls._session_factory = sessionmaker(
                bind=cls._engine,
                autocommit=False,
                autoflush=False
            )
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    @classmethod
    def create_tables(cls) -> None:
        """Create all tables defined in models"""
        if cls._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        logger.info("Creating database tables...")
        Base.metadata.create_all(cls._engine)
        logger.info("Tables created successfully")
    
    @classmethod
    def get_session(cls) -> Session:
        """
        Get a new database session
        
        Returns:
            SQLAlchemy Session instance
        """
        if cls._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        return cls._session_factory()
    
    @classmethod
    @contextmanager
    def session_scope(cls) -> Generator[Session, None, None]:
        """
        Provide a transactional scope for database operations
        
        Usage:
            with DatabaseSession.session_scope() as session:
                session.add(paper)
        
        Yields:
            SQLAlchemy Session instance
        """
        session = cls.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            session.close()
    
    @classmethod
    def close(cls) -> None:
        """Close database engine and cleanup connections"""
        if cls._engine:
            cls._engine.dispose()
            cls._engine = None
            cls._session_factory = None
            logger.info("Database connection closed")


# Initialize database on module import (for Airflow)
def init_db() -> None:
    """Initialize database for Airflow tasks"""
    DatabaseSession.initialize()
    DatabaseSession.create_tables()


if __name__ == "__main__":
    # Test database connection
    init_db()
    
    with DatabaseSession.session_scope() as session:
        from src.database.models import Paper
        count = session.query(Paper).count()
        logger.info(f"Current paper count: {count}")
