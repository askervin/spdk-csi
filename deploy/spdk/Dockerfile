# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
FROM fedora:33

ARG TAG=v21.04
ARG ARCH=native

WORKDIR /root
RUN dnf install -y git diffutils procps-ng pip
RUN git clone https://github.com/spdk/spdk --branch ${TAG} --depth 1 && \
    cd spdk && git submodule update --init --depth 1 && scripts/pkgdep.sh --rdma

# WORKAROUND: fix grpcio/protobuf versions that generate broken _sb2 files:
# spdk/scripts/sma.py produced error:
# AttributeError: 'NoneType' object has no attribute 'message_types_by_name'
# when using the latest (grpcio-tools-1.49.1 protobuf-4.21.7)
# This should be fixed in spdk/scripts/pkgdep.sh.
RUN pip3 install grpcio-tools==1.14.1 protobuf==3.12.4

RUN cd spdk && \
    ./configure --disable-tests --without-vhost --without-virtio \
                --with-rdma --target-arch=${ARCH} --with-sma && \
    make
