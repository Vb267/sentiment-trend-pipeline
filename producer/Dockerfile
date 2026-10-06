FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt ./producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

COPY common ./common
COPY producer ./producer

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "producer.main"]
