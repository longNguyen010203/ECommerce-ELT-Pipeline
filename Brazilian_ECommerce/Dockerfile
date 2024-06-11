FROM python:3.11-slim

WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "Ecommerce_dagster"]