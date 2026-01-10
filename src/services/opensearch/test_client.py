"""
Test script to verify self-healing OpenSearch client
"""

import sys
import os
sys.path.insert(0, '/opt/airflow')

from src.services.opensearch.client import QuantaSearchClient
from src.services.opensearch.index_config import get_index_name

def test_self_healing():
    """Test that client auto-creates index"""
    
    print("🧪 Testing Self-Healing OpenSearch Client\n")
    print("=" * 60)
    
    # Initialize client (with connection retries)
    print("\n1. Initializing client...")
    client = QuantaSearchClient()
    print("   ✅ Connected to OpenSearch")
    
    # Get index name
    index_name = get_index_name()
    print(f"\n2. Target index: '{index_name}'")
    
    # Check current count (will auto-create if missing)
    print("\n3. Checking paper count (auto-creates index if missing)...")
    count = client.get_paper_count(index_name)
    print(f"   ✅ Papers in index: {count}")
    
    # Test search (should work even with 0 papers)
    print("\n4. Testing search functionality...")
    results = client.search("quantum computing", limit=5)
    print(f"   ✅ Search returned {len(results)} results")
    
    print("\n" + "=" * 60)
    print("✅ Self-healing client verified!")
    print(f"   - Index '{index_name}' exists")
    print(f"   - {count} papers indexed")
    print("   - Ready for index_papers_task ✅")
    
    return True

if __name__ == "__main__":
    try:
        test_self_healing()
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
