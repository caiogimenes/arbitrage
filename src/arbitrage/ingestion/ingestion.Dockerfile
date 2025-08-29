FROM python:3.12 AS builder

RUN pip install poetry

WORKDIR /ingestion

COPY poetry.lock pyproject.toml ./

RUN poetry install --no-root


FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash appuser

USER appuser

WORKDIR /home/appuser/ingestion

COPY --from=builder /ingestion/.venv ./.venv

ENV PATH="/home/appuser/ingestion/.venv/bin:$PATH"

COPY ./ingestion ./

CMD ["python", "main.py"]