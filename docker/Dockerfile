########
# Python dependencies builder
#
ARG PYTHON_VERSION=3.6
FROM python:$PYTHON_VERSION-slim AS builder

WORKDIR /app
# Sets utf-8 encoding for Python et al
ENV LANG=C.UTF-8
ENV PYTHONIOENCODING=utf8
# Turns off writing .pyc files; superfluous on an ephemeral container.
ENV PYTHONDONTWRITEBYTECODE=1
# Seems to speed things up
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Ensures that the python and pip executables used
# in the image will be those from our virtualenv.
ENV PATH="/venv/bin:$PATH"

# Setup the virtualenv
RUN python -m venv /venv

# Install OS package dependencies.
RUN apt-get update && apt-get install -y git gcc

# Install Python dependencies and packages not in requirements
ARG GIT_TAG=dev
RUN git clone https://github.com/nestauk/nesta.git --branch $GIT_TAG --depth 1 --single-branch
RUN pip install --no-cache-dir -r nesta/requirements.txt \
 && pip install --no-cache-dir awscli mysql-connector-python python-Levenshtein


########
# app container
#
FROM python:$PYTHON_VERSION-slim AS app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf8
ENV LANG=C.UTF-8
ENV PATH="/venv/bin:$PATH"
ENV PYTHONPATH /app
ENV MYSQLDB /app/nesta/core/config/mysqldb.config
ENV LUIGI_CONFIG_DIR /appdocker
ENV LUIGI_CONFIG_PATH /app/nesta/core/config/luigi.cfg
ENV PATH "$PATH:/app/nesta/core/scripts/"

WORKDIR /app

# Copy in Python environment
COPY --from=builder /venv /venv

# Install OS package dependencies.
RUN apt-get update && apt-get install -y git zip tree

# Use local files and requirements rather than from the repo
# COPY ./ ./
# install from local requirements, maybe useful with the above
# RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

# Comment out the block below if using local files
# this argument has to be re-declared here as it is cleared by the FROM above
ARG GIT_TAG=dev
RUN git clone https://github.com/nestauk/nesta.git --branch $GIT_TAG --depth 1 --single-branch .

# Copy in the rest of the app from local to pick up configs
# TODO: replace when secrets are implemented
COPY nesta/core/config/ nesta/core/config/

RUN mkdir -p /var/tmp/batch && \
    mkdir -p /var/log/luigi && \
    mv docker/run.sh /usr/bin/run.sh && \
    chmod +x /usr/bin/run.sh

ENTRYPOINT ["run.sh"]
