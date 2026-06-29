ARG SING_BOX_VERSION=1.13.13

FROM golang:1.23-bookworm AS builder
WORKDIR /src
COPY src/go.mod ./
RUN go mod download
COPY src/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/proxy-account-router .

FROM debian:bookworm-slim AS singbox
ARG SING_BOX_VERSION
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar gzip \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp
RUN curl -fsSL -o sing-box.tar.gz "https://github.com/SagerNet/sing-box/releases/download/v${SING_BOX_VERSION}/sing-box-${SING_BOX_VERSION}-linux-amd64.tar.gz" \
    && tar -xzf sing-box.tar.gz \
    && cp "sing-box-${SING_BOX_VERSION}-linux-amd64/sing-box" /sing-box \
    && chmod +x /sing-box

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /out/proxy-account-router /app/proxy-account-router
COPY --from=singbox /sing-box /usr/local/bin/sing-box
EXPOSE 19080 19100-19199
CMD ["/app/proxy-account-router", "-config", "/app/config.yaml"]
