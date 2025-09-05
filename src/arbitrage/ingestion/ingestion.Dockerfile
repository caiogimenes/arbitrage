FROM python:3.12 AS builder
LABEL authors="lucascorrea"
RUN pip install poetry
WORKDIR /app
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
COPY poetry.lock pyproject.toml ./
RUN poetry install --no-root


FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash appuser
USER appuser

WORKDIR /home/appuser/app

ENV PYTHONPATH=.

COPY --from=builder /app/.venv ./.venv
ENV PATH="/home/appuser/app/.venv/bin:$PATH"


COPY ./src ./src


CMD ["python", "-m", "src.arbitrage.ingestion.main"]