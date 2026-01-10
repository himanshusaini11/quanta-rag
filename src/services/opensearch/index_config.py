"""
OpenSearch index configuration for Quanta-RAG
Defines the mapping for arxiv-papers index
"""

# Index name
ARXIV_PAPERS_INDEX = "arxiv-papers"

# Index mapping configuration
ARXIV_PAPERS_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "default": {
                    "type": "standard"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "arxiv_id": {
                "type": "keyword"  # Exact match for paper IDs
            },
            "title": {
                "type": "text",
                "analyzer": "standard",  # Tokenization and lowercase
                "fields": {
                    "keyword": {
                        "type": "keyword",  # For exact title matching
                        "ignore_above": 256
                    }
                }
            },
            "full_text": {
                "type": "text",
                "analyzer": "standard"  # Main searchable field for BM25
            },
            "summary": {
                "type": "text",
                "analyzer": "standard"
            },
            "published_date": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "created_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "authors": {
                "type": "text",
                "analyzer": "standard"
            },
            "categories": {
                "type": "keyword"  # Array of category tags
            }
        }
    }
}


def get_index_config() -> dict:
    """
    Get the complete index configuration
    
    Returns:
        Dictionary with index settings and mappings
    """
    return ARXIV_PAPERS_MAPPING


def get_index_name() -> str:
    """
    Get the index name
    
    Returns:
        Index name string
    """
    return ARXIV_PAPERS_INDEX
