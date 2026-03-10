FROM python:3.14-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r /app/requirements.txt


FROM python:3.14-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

COPY --from=builder /wheels /wheels
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r /app/requirements.txt \
    && rm -rf /wheels

COPY . /app

RUN chown -R appuser:appgroup /app
USER appuser

CMD ["python", "main.py"]
