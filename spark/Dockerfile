ARG IMAGE_VARIANT=slim-buster
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM amazoncorretto:8
FROM amazoncorretto:8-alpine

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.4.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN pip --no-cache-dir install elasticsearch
RUN pip --no-cache-dir install numpy
RUN pip --no-cache-dir install beautifulsoup4 requests


COPY app.py .

CMD spark-submit --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 app.py 
