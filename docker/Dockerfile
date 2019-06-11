########
# Python dependencies builder
#
FROM python:3.6-slim-stretch AS builder

WORKDIR /app
# Sets utf-8 encoding for Python et al
ENV LANG=C.UTF-8
# Turns off writing .pyc files; superfluous on an ephemeral container.
ENV PYTHONDONTWRITEBYTECODE=1
# Seems to speed things up (untested)
# ENV PYTHONUNBUFFERED=1

# Ensures that the python and pip executables used
# in the image will be those from our virtualenv.
ENV PATH="/venv/bin:$PATH"

# Install OS package dependencies.
# Do all of this in one RUN to limit final image size.
RUN apt-get update && apt-get install -y git

# Setup the virtualenv
RUN python -m venv /venv
# or "virtualenv /venv" for Python 2

# Install Python deps
# for dev copy locally, or specify a branch
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# implement pull from github for production version (branch or tag can be supplied)
# RUN git clone https://github.com/nestauk/nesta.git --branch master --depth 1 --single-branch
# RUN pip install --no-cache-dir -r nesta/requirements.txt

# HACK! mysql-connector-repackaged doesn't work. Investigate switching over.
RUN pip install mysql-connector-python awscli



########
# app container
#
FROM python:3.6-slim-stretch AS app

# Extra python env
ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PATH="/venv/bin:$PATH"
ENV PYTHONPATH /app
ENV MYSQLDB /app/nesta/production/config/mysqldb.config

WORKDIR /app
# for the luigi central scheduler web interface
# EXPOSE 8082/TCP

ENV LUIGI_CONFIG_DIR /app/nesta/production/config
ENV LUIGI_CONFIG_PATH /app/nesta/production/config/luigi.cfg

# copy in Python environment
COPY --from=builder /venv /venv

# copy in the rest of the app from local
COPY ./ ./

RUN mkdir -p /var/log/luigi && \
    mv docker/run.sh /usr/bin/run.sh && \
    chmod +x /usr/bin/run.sh

ENTRYPOINT ["run.sh"]
