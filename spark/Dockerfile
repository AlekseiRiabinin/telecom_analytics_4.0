FROM apache/spark:3.5.4

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    libbz2-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN cd /tmp && \
    curl -O https://www.python.org/ftp/python/3.10.14/Python-3.10.14.tgz && \
    tar -xzf Python-3.10.14.tgz && \
    cd Python-3.10.14 && \
    ./configure --enable-optimizations --prefix=/usr/local && \
    make -j $(nproc) altinstall && \
    cd /tmp && \
    rm -rf Python-3.10.14*

# Create symlinks for python3.10
RUN ln -sf /usr/local/bin/python3.10 /usr/bin/python3.10 && \
    ln -sf /usr/local/bin/python3.10 /usr/bin/python3 && \
    ln -sf /usr/local/bin/python3.10 /usr/bin/python

# Install pip for Python 3.10
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

RUN python3 --version

RUN python3.10 -m pip install --no-cache-dir \
    pandas==1.5.3 \
    pyarrow==10.0.1 \
    numpy==1.24.3 \
    boto3==1.26.0 \
    requests==2.28.2

RUN mkdir -p /opt/spark/custom-jars && \
    chown -R 185:185 /opt/spark/custom-jars

USER 185

ENV PYSPARK_PYTHON=python3.10
ENV PYSPARK_DRIVER_PYTHON=python3.10
