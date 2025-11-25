# Copyright Hewlett Packard Enterprise Development LP.

# Start from official Chapel image
FROM chapel/chapel:2.6.0

# System deps needed for building OTF2 + Arrow
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git wget curl unzip pkg-config \
    python3 python3-pip python3-dev \
    libssl-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install OTF2
# -----------------------------
COPY otf2-3.1.1.tar.gz /opt/
WORKDIR /opt
RUN tar -xzf otf2-3.1.1.tar.gz && \
    cd otf2-3.1.1 && ./configure --prefix=/opt/otf2 && \
    make -j$(nproc) && make install && \
    rm -rf /opt/otf2-3.1.1*
ENV LD_LIBRARY_PATH=/opt/otf2/lib:$LD_LIBRARY_PATH

# # -----------------------------
# # Install Apache Arrow + Parquet C++
# # -----------------------------
# WORKDIR /opt
# RUN git clone --depth=1 https://github.com/apache/arrow.git && \
#     mkdir -p arrow/cpp/build && cd arrow/cpp/build && \
#     cmake -DCMAKE_INSTALL_PREFIX=/usr/local \
#           -DARROW_PARQUET=ON \
#           -DARROW_PYTHON=ON \
#           -DARROW_BUILD_SHARED=ON \
#           -DARROW_WITH_LZ4=ON \
#           -DARROW_WITH_ZSTD=ON \
#           .. && \
#     make -j$(nproc) && make install && \
#     rm -rf /opt/arrow

# -----------------------------
# Default working dir + shell
# -----------------------------
WORKDIR /workspace
CMD ["/bin/bash"]

