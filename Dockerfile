FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files first (for layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

EXPOSE 8000

CMD ["sh", "-c", "uv run python main_server.py --transport streamable-http --host ${HOST:-0.0.0.0} --port ${PORT:-8000}"]
