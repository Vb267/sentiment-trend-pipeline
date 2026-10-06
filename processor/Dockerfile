FROM python:3.11-slim

WORKDIR /app

COPY processor/requirements.txt ./processor/requirements.txt
# torch's CPU-only wheel is a fraction of the size of the default (CUDA) build
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
    -r processor/requirements.txt

COPY common ./common
COPY processor ./processor

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "processor.main"]
