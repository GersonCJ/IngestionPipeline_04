FROM python:3.12-slim

WORKDIR /app

COPY quality/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-c", "import great_expectations as gx; print(gx.__version__)"]