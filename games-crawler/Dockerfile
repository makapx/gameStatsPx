
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM amazoncorretto:8
FROM amazoncorretto:8-alpine

COPY --from=py3 / /

RUN pip --no-cache-dir install requests
RUN pip --no-cache-dir install schedule
RUN pip --no-cache-dir install google-api-python-client

COPY config ./config
COPY games.py .
CMD python games.py


