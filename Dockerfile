########
# Python dependencies builder
#
# Full official Debian-based Python image
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
RUN pip install mysql-connector-python



########
# app container
#
# Smaller official Debian-based Python image
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
EXPOSE 8082/TCP

ENV LUIGI_CONFIG_DIR nesta/production/config
ENV LUIGI_CONFIG_PATH nesta/production/config/luigi.cfg
# ENV LUIGI_STATE_DIR /luigi/state
ENV AWS_CONFIG_DIR ~/.aws

# copy in Python environment
COPY --from=builder /venv /venv

# copy in the rest of the app from local
COPY ./ ./

RUN mv nesta/production/scripts/run-luigi-container /usr/bin/run.sh && \
    chmod +x /usr/bin/run.sh && \
    mkdir -p /var/log/luigi
# aws credentials need to be added

CMD ["run.sh"]
