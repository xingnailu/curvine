FROM ubuntu:24.04 AS builder

ARG GOPROXY
ARG TARGETARCH

RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    wget \
    tar \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 根据目标架构下载对应的 Go 版本
RUN case ${TARGETARCH} in \
        "amd64") GOARCH=amd64 ;; \
        "arm64") GOARCH=arm64 ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    wget -O go1.23.linux-${GOARCH}.tar.gz https://go.dev/dl/go1.23.4.linux-${GOARCH}.tar.gz && \
    tar -C /usr/local -xzf go1.23.linux-${GOARCH}.tar.gz && \
    rm go1.23.linux-${GOARCH}.tar.gz

ENV PATH=/usr/local/go/bin:$PATH
ENV GOPATH=/go
ENV GOROOT=/usr/local/go
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org}

WORKDIR /workspace

COPY curvine-csi/ ./curvine-csi/

RUN cd curvine-csi && \
    go build -o bin/csi main.go

FROM ubuntu:24.04

RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    fuse3 \
    libfuse3-3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN mkdir -p /opt/curvine/bin /opt/curvine/conf

COPY build/dist/bin/* /opt/curvine/bin/
COPY build/dist/conf/* /opt/curvine/conf/
COPY build/dist/lib/curvine-fuse /opt/curvine/bin/

COPY --from=builder /workspace/curvine-csi/bin/csi /opt/curvine/bin/

RUN chmod u+x /opt/curvine/bin/csi && \
    chmod u+x /opt/curvine/bin/cv && \
    chmod u+x /opt/curvine/bin/curvine-fuse

WORKDIR /opt/curvine