# ============================================================================
# AchillesDB Dockerfile
# Multi-stage build for a production-ready vector database container
# ============================================================================

# Stage 1: Build environment with all dependencies
FROM ubuntu:22.04 AS builder

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    pkg-config \
    autoconf \
    automake \
    libtool \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    swig \
    python3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Go 1.24
ARG GO_VERSION=1.24.2
RUN wget -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz \
    && rm go${GO_VERSION}.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"
ENV CGO_ENABLED=1

# Build WiredTiger from source
ARG WIREDTIGER_VERSION=11.3.0
WORKDIR /tmp/wiredtiger
RUN git clone --depth 1 --branch ${WIREDTIGER_VERSION} \
    https://github.com/wiredtiger/wiredtiger.git . \
    && mkdir build && cd build \
    && cmake .. \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DENABLE_SNAPPY=1 \
        -DENABLE_LZ4=1 \
        -DENABLE_ZSTD=1 \
        -DCMAKE_BUILD_TYPE=Release \
    && make -j$(nproc) \
    && make install

# Build FAISS from source
ARG FAISS_VERSION=v1.9.0
WORKDIR /tmp/faiss
RUN git clone --depth 1 --branch ${FAISS_VERSION} \
    https://github.com/facebookresearch/faiss.git . \
    && cmake -B build \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DFAISS_ENABLE_GPU=OFF \
        -DFAISS_ENABLE_PYTHON=OFF \
        -DFAISS_ENABLE_C_API=ON \
        -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j$(nproc) \
    && cmake --install build

# Update library cache
RUN ldconfig

# Build the Go application
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with CGO enabled
RUN CGO_ENABLED=1 \
    CGO_CFLAGS="-I/usr/local/include" \
    CGO_LDFLAGS="-L/usr/local/lib -lwiredtiger -lfaiss_c" \
    go build -ldflags="-s -w" -o achillesdb .

# ============================================================================
# Stage 2: Runtime image
# ============================================================================
FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    libgomp1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled libraries from builder
COPY --from=builder /usr/local/lib/libwiredtiger*.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libfaiss*.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libomp*.so* /usr/local/lib/ 2>/dev/null || true

# Update library cache
RUN ldconfig

# Create non-root user for security
RUN groupadd -r achilles && useradd -r -g achilles achilles

# Create data directories
RUN mkdir -p /data/wiredtiger /data/vectors \
    && chown -R achilles:achilles /data

# Copy the compiled binary
WORKDIR /app
COPY --from=builder /app/achillesdb .

# Set ownership
RUN chown -R achilles:achilles /app

# Switch to non-root user
USER achilles

# Environment variables
ENV WT_HOME=/data/wiredtiger
ENV VECTORS_HOME=/data/vectors

# Expose the HTTP port
EXPOSE 8180

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8180/api/v1/database || exit 1

# Volume for persistent data
VOLUME ["/data"]

# Run the application
CMD ["./achillesdb"]

