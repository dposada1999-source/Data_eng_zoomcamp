FROM python:3.13.11-slim
#Docker image that contains uv:
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/ 

WORKDIR /code
# Add virtual environment to PATH so we can use installed packages and avoid writing uv run on entrypoint
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# install dependencies from uv.lock
RUN uv sync --locked

COPY load_taxi_data_2024.py load_taxi_data_2024.py

ENTRYPOINT ["python","load_taxi_data_2024.py"]