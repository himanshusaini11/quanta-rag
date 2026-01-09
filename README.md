# Quanta-RAG

An Autonomous Research Agent that accelerates scientific discovery. Ingests real-time Arxiv papers and provides citation-backed answers using Hybrid Search and LLMs.

---

## Project Status

### Phase 1: Data Ingestion Pipeline - ✅ Completed
- Modular project structure
- Docker-based infrastructure (MongoDB + Qdrant)
- Asynchronous PDF ingestion engine
- MongoDB integration with idempotent operations
- Production-ready error handling and logging

### Phase 2: Text Processing & Embeddings - 🚧 In Progress
- PDF text extraction
- Sentence embedding generation
- Vector storage in Qdrant

### Phase 3: RAG Pipeline - 📋 Planned
- Hybrid search (semantic + keyword)
- LLM integration for answer generation
- Citation tracking

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### 1. Start Infrastructure

Start MongoDB and Qdrant containers:

```bash
docker-compose up -d
```

Verify services are running:

```bash
docker-compose ps
```

### 2. Install Dependencies

Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run Ingestion Script

Ingest papers from Arxiv:

```bash
python -m src.ingestion.ingest
```

Or use it programmatically:

```python
from src.ingestion.ingest import IngestionEngine

engine = IngestionEngine(max_concurrent=5)
results = engine.ingest_papers(
    query="machine learning",
    max_results=10
)
```

### 4. Configuration (Optional)

Create a `.env` file for custom settings:

```env
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=quanta_rag
```

---

## Project Structure

```
quanta-rag/
├── data/
│   └── raw/              # Downloaded PDFs (gitignored)
├── src/
│   ├── database/
│   │   └── mongo.py      # MongoDB connection manager
│   └── ingestion/
│       └── ingest.py     # Async ingestion engine
├── notebooks/            # Jupyter notebooks for experiments
├── tests/                # pytest tests
├── docker-compose.yaml   # Infrastructure setup
├── requirements.txt      # Python dependencies
└── README.md
```

---

## Architecture Highlights

- **Async I/O:** Concurrent PDF downloads using `asyncio` and `aiohttp`
- **Idempotent Ingestion:** Upsert operations prevent duplicate papers
- **Singleton Pattern:** Single MongoDB connection instance
- **Dockerized:** Reproducible environment for development and production
- **Production-Ready:** Type hints, comprehensive error handling, structured logging

For detailed architectural decisions, see `Learning.md` (gitignored).

---

## Development

### Run Tests

```bash
pytest tests/
```

### Stop Infrastructure

```bash
docker-compose down
```

To remove volumes as well:

```bash
docker-compose down -v
```

---

## License

MIT License
