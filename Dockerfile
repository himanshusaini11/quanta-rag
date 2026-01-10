FROM apache/airflow:2.8.0-python3.10

USER root

# Install system dependencies required for Docling/OCR
RUN apt-get update && apt-get install -y \
    build-essential \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
# We install these directly to bake them into the image
RUN pip install --no-cache-dir \
    arxiv \
    pypdf \
    aiohttp \
    python-dotenv \
    loguru \
    tqdm \
    psycopg2-binary \
    sqlalchemy \
    pydantic \
    docling \
    tenacity \
    requests