FROM public.ecr.aws/lambda/python:3.9

RUN yum update -y && \
  yum install git zip ninja-build make gcc-c++ openssl11-devel cmake3 -y && \
  yum remove cmake -y && \
  ln -s /usr/bin/cmake3 /usr/bin/cmake

RUN mkdir -p /tmp/python /tmp/release /tmp/build

# Install Python dependencies
RUN pip3 install duckdb
RUN pip3 install duckdb --target /tmp/build/python

# Install HTTPFS
COPY install.py ${LAMBDA_TASK_ROOT}
RUN python3 ${LAMBDA_TASK_ROOT}/install.py

RUN cd /tmp/build && \
  zip -q -r /tmp/release/duckdb-layer.zip .