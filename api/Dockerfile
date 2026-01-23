FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python deps
COPY api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy API code
COPY api/ /app/api/

EXPOSE 8000

# Note: Configure DB_* env vars at runtime.
CMD ["uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8000"]
