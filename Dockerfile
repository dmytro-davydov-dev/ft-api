# ck-api — Cloud Run container image
# Python 3.12 slim, Gunicorn, port 8080.
#
# Cloud Run config (managed via gcloud / GitHub Actions):
#   region:        europe-west1
#   cpu:           1
#   memory:        512Mi
#   min-instances: 0
#   max-instances: 5
#   port:          8080

FROM python:3.12-slim

# Prevents Python from writing .pyc files and buffers stdout/stderr.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source.
COPY . .

# Cloud Run injects PORT; default to 8080.
ENV PORT=8080
EXPOSE 8080

# Gunicorn: 1 worker per vCPU, sync workers are fine for I/O-bound Flask.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "60", "app:app"]
