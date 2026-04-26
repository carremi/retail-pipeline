FROM python:3.12-slim

WORKDIR /app

# System dependencies for psycopg (binary)
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY src/ src/
COPY sql/ sql/
COPY scripts/ scripts/
COPY simulators/ simulators/
COPY data/ data/
COPY docker/entrypoint.sh /entrypoint.sh

# Install the package (editable so scripts can import it)
RUN pip install --no-cache-dir -e ".[dev]"

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["shell"]
