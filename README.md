# Quanta-RAG

An Autonomous Research Agent that accelerates scientific discovery. Ingests real-time Arxiv papers and provides citation-backed answers using Hybrid Search and LLMs.

---

## Project Status

### Phase 1: Data Ingestion Pipeline
- **Architecture:** Apache Airflow + PostgreSQL
- Modular project structure
- Docker-based infrastructure (Airflow, PostgreSQL, Qdrant)
- PostgreSQL with SQLAlchemy ORM
- Airflow DAGs for orchestrated ingestion
- Production-ready with observability and retries

### Phase 2: Text Processing & Embeddings - 📋 Planned
- PDF text extraction
- Sentence embedding generation
- Vector storage in Qdrant

### Phase 3: RAG Pipeline - 📋 Planned
- Hybrid search (semantic + keyword)
- LLM integration for answer generation
- Citation tracking

---

## Architecture

### Current Stack (Airflow + PostgreSQL)

- **Orchestration:** Apache Airflow for workflow management
- **Database:** PostgreSQL for structured metadata storage
- **Vector DB:** Qdrant for semantic search (Phase 2)
- **Async I/O:** aiohttp for concurrent PDF downloads
- **ORM:** SQLAlchemy with Pydantic validation

### DAG Flow

```
Start >> Fetch Metadata >> Store Papers >> Download PDFs >> End
```

**Schedule:** Daily (@daily)  
**Retries:** 3 attempts with 5-minute delays

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.10+

### 1. Start Infrastructure

Start Airflow, PostgreSQL, and Qdrant:

```bash
docker-compose up -d
```

Wait for all services to be healthy (~30 seconds):

```bash
docker-compose ps
```

### 2. Access Airflow UI

Open your browser to: **http://localhost:8080**

- **Username:** `admin`
- **Password:** `admin`

### 3. Trigger the DAG

In the Airflow UI:
1. Find the `ingest_papers_dag`
2. Toggle it to "ON"
3. Click the "Play" button to trigger manually

Or trigger via CLI:

```bash
docker exec quanta-airflow-scheduler airflow dags trigger ingest_papers_dag
```

### 4. Monitor Execution

- View task status in the Airflow UI
- Click on tasks to see logs
- Check the Graph view for task dependencies

### 5. Connect to Database (Optional)

```bash
# PostgreSQL
psql postgresql://quanta:quanta@localhost:5432/quanta_rag

# Or using Python
python -c "from src.database.session import init_db; init_db()"
```

---

## Project Structure

```
quanta-rag/
├── dags/
│   ├── ingest_papers_dag.py       # Daily ingestion DAG
│   └── tasks/
│       └── ingestion_tasks.py     # Task functions
├── src/
│   ├── database/
│   │   ├── models.py              # SQLAlchemy models
│   │   └── session.py             # DB connection
│   └── ingestion/                 # (Deprecated)
├── data/
│   └── raw/                       # Downloaded PDFs
├── notebooks/                     # Jupyter experiments
├── tests/                         # pytest tests
├── docker-compose.yaml            # Infrastructure
└── requirements.txt               # Dependencies
```

---

## Configuration

Create a `.env` file (use `.env.example` as template):

```env
DATABASE_URL=postgresql://quanta:quanta@localhost:5432/quanta_rag
QDRANT_URL=http://localhost:6333
AIRFLOW_UID=50000
LOG_LEVEL=INFO
```

---

## Architecture Highlights

### Why Airflow?
- **Observability:** Rich web UI for monitoring pipelines
- **Retries:** Automatic retry logic for transient failures
- **Backfilling:** Re-run pipelines for historical dates
- **Scheduling:** Built-in CRON-like scheduler
- **Scalability:** Can scale to multiple workers

### Why PostgreSQL?
- **ACID Compliance:** Guaranteed data integrity
- **Structured Data:** Arxiv metadata has consistent schema
- **Integration:** Native Airflow support
- **Analytics:** Better for complex queries and joins
- **Tooling:** Mature ecosystem (pgAdmin, psql)

### Why AsyncIO?
- **I/O-Bound:** PDF downloads are network-bound
- **Concurrency:** Efficient concurrent downloads
- **Rate Limiting:** Semaphore-based control

For detailed architectural decisions, see `Learning.md` (gitignored).

---

## Development

### Stop Infrastructure

```bash
docker-compose down
```

To remove volumes as well:

```bash
docker-compose down -v
```

### Run Tests

```bash
pytest tests/
```

### View Airflow Logs

```bash
docker logs quanta-airflow-scheduler
docker logs quanta-airflow-webserver
```

### Database Migrations

```bash
# Future: Use Alembic for schema migrations
alembic init alembic
alembic revision --autogenerate -m "Description"
alembic upgrade head
```

---

## Troubleshooting

### Airflow UI not accessible
- Ensure port 8080 is not in use
- Check container health: `docker-compose ps`
- View logs: `docker logs quanta-airflow-webserver`

### DAG not appearing
- Check DAG syntax: `docker exec quanta-airflow-scheduler airflow dags list`
- Ensure `/dags` volume is mounted correctly
- Refresh the Airflow UI (may take 30s to detect new DAGs)

### Database connection errors
- Verify PostgreSQL is running: `docker-compose ps postgres`
- Check credentials in `.env` file
- Ensure `DATABASE_URL` matches docker-compose config

---

## License

MIT License
