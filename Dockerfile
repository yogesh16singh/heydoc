FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY migrations ./migrations
COPY src ./src
COPY web ./web

EXPOSE 8000
CMD ["python", "-m", "gateway.api"]
