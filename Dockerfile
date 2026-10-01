FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir --timeout 120 --retries 5 -r /app/requirements.txt && \
    pip install --no-cache-dir --timeout 120 --retries 5 boto3

COPY . /app

# Default command for scheduled execution in ECS/Fargate
CMD ["python", "daily_report_runner.py"]
