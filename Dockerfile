# SitePulse Enterprise Container Image
# Multi-stage image containing Python runtime, OpenJDK for PySpark, and FastAPI server.

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="${JAVA_HOME}/bin:${PATH}" \
    PYTHONPATH=/app

WORKDIR /app

# Install OpenJDK-17 and minimal OS dependencies for PySpark and DuckDB
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application codebase
COPY sitepulse/ ./sitepulse/
COPY sqlserver/ ./sqlserver/
COPY dags/ ./dags/
COPY powerbi/ ./powerbi/
COPY README.md .

# Create lakehouse and serving storage directories
RUN mkdir -p /app/data/metadata /app/data/lakehouse

EXPOSE 8000

# Health check to ensure web API and UI are responsive
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

CMD ["uvicorn", "sitepulse.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
