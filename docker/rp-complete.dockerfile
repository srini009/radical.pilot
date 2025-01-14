# This Dockerfile is hosted at https://github.com/radical-cybertools/radical.pilot/tree/devel/docker
# See README.md in this directory for instructions.

FROM mongo:bionic
# Reference https://github.com/docker-library/mongo/blob/master/4.2/Dockerfile

RUN groupadd -r radical && useradd -r -g radical -m rp

RUN apt-get update && \
    apt-get install -y \
        curl \
        dnsutils \
        gcc \
        git \
        iputils-ping \
        libopenmpi-dev \
        locales \
        openmpi-bin \
        python3-dev \
        python3-venv \
        vim \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8

USER rp

RUN (cd ~rp && python3 -m venv rp-venv)

RUN (cd ~rp && \
    rp-venv/bin/pip install --upgrade \
        pip \
        setuptools \
        wheel && \
    rp-venv/bin/pip install --upgrade \
        coverage \
        flake8 \
        'mock==2.0.0' \
        mpi4py \
        netifaces \
        ntplib \
        pylint \
        pymongo \
        pytest \
        python-hostlist \
        setproctitle \
        )

RUN . ~rp/rp-venv/bin/activate && \
    pip install --upgrade \
        'radical.saga>=1.0' \
        'radical.utils>=1.1'

# Install RP from the current local repository working directory.
COPY --chown=rp:radical . /home/rp/radical.pilot
RUN . ~rp/rp-venv/bin/activate && \
    cd ~rp/radical.pilot && \
    pip install .

# Note: if we want the image to target a specific (configrable) branch, use the following instead.
#
## Get repository for example and test files and to simplify RPREF build argument.
## Note that GitHub may have a source directory name suffix that does not exactly
## match the branch or tag name, so we use a glob to try to normalize the name.
#ARG RPREF="v1.5.2"
#RUN cd ~rp && \
#    wget https://github.com/radical-cybertools/radical.pilot/archive/$RPREF.tar.gz && \
#    tar zxvf $RPREF.tar.gz && \
#    mv radical.pilot-* radical.pilot && \
#    rm $RPREF.tar.gz
#
## Install RP from whichever git ref is provided as `--build-arg RPREF=...` (default 1.5.2)
#RUN . ~rp/rp-venv/bin/activate && \
#    cd ~rp/radical.pilot && \
#    pip install .
# OR
## Install official version from PyPI
#RUN . ~rp/rp-venv/bin/activate && \
#    pip install radical.pilot

# Allow RADICAL Pilot to provide more useful behavior during testing,
# such as mocking missing resources from the resource specification.
ENV RADICAL_DEBUG="True"
RUN echo export RADICAL_DEBUG=$RADICAL_DEBUG >> ~rp/.profile

USER root

# Note that the following environment variables have special meaning to the
# `mongo` Docker container entry point script.
ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=password

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD@localhost:27017/admin"
