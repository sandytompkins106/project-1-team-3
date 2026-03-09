FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY etl ./etl

# We will replace this pipeline with the unified one
CMD ["python", "-m", "etl.pipelines.openaq"]