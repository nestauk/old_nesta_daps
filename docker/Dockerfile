########
# Python dependencies builder
#
ARG PYTHON_VERSION=3.6
FROM python:$PYTHON_VERSION-slim AS builder

WORKDIR /app
# Sets utf-8 encoding for Python et al
ENV LANG=C.UTF-8
# Turns off writing .pyc files; superfluous on an ephemeral container.
ENV PYTHONDONTWRITEBYTECODE=1
# Seems to speed things up
ENV PYTHONUNBUFFERED=1

# Ensures that the python and pip executables used
# in the image will be those from our virtualenv.
ENV PATH="/venv/bin:$PATH"

# Install OS package dependencies.
RUN apt-get update && apt-get install -y git

# Setup the virtualenv
RUN python -m venv /venv

# Install Python dependencies
# TODO: Replace with clone from git as below
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# TODO: implement clone from github with arg for branch or tag
# RUN git clone https://github.com/nestauk/nesta.git --branch master --depth 1 --single-branch
# RUN pip install --no-cache-dir -r nesta/requirements.txt

# Install packages not in requirements. 
# TODO: mysql-connector-repackaged doesn't work, investigate switching over.
RUN pip install awscli mysql-connector-python


########
# app container
#
FROM python:$PYTHON_VERSION-slim AS app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PATH="/venv/bin:$PATH"
ENV PYTHONPATH /app
ENV MYSQLDB /app/nesta/production/config/mysqldb.config
ENV LUIGI_CONFIG_DIR /app/nesta/production/config
ENV LUIGI_CONFIG_PATH /app/nesta/production/config/luigi.cfg

WORKDIR /app

# Copy in Python environment
COPY --from=builder /venv /venv

# Copy in the rest of the app from local to pick up configs
# TODO: replace when secrets are implemented
COPY ./ ./

RUN mkdir -p /var/log/luigi && \
    mv docker/run.sh /usr/bin/run.sh && \
    chmod +x /usr/bin/run.sh

ENTRYPOINT ["run.sh"]
