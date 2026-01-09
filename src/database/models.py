"""
SQLAlchemy models for Quanta-RAG PostgreSQL database
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Paper(Base):
    """
    Paper metadata model
    Stores Arxiv paper information in PostgreSQL
    """
    
    __tablename__ = 'papers'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Arxiv unique identifier
    arxiv_id = Column(String(50), unique=True, nullable=False, index=True)
    
    # Paper metadata
    title = Column(String(500), nullable=False)
    summary = Column(Text, nullable=True)
    
    # File storage
    pdf_path = Column(String(500), nullable=True)
    
    # Temporal metadata
    published_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_arxiv_id', 'arxiv_id'),
        Index('idx_published_date', 'published_date'),
        Index('idx_created_at', 'created_at'),
    )
    
    def __repr__(self) -> str:
        return f"<Paper(arxiv_id='{self.arxiv_id}', title='{self.title[:50]}...')>"
    
    def to_dict(self) -> dict:
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'arxiv_id': self.arxiv_id,
            'title': self.title,
            'summary': self.summary,
            'pdf_path': self.pdf_path,
            'published_date': self.published_date.isoformat() if self.published_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }
