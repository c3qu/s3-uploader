# Stage 1: Build
FROM rust:1-alpine AS builder

WORKDIR /app

# Cache dependency layer
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/s3_uploader-*

# Build actual binary
COPY src/ src/
RUN cargo build --release && \
    cp target/release/s3-uploader /s3-uploader

# UPX compress
RUN apk add --no-cache upx && \
    upx --best --lzma /s3-uploader

# Stage 2: Scratch + CA certs from builder (minimal size)
FROM scratch

COPY --from=builder /s3-uploader /s3-uploader
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Ensure /tmp exists for stdin temp files (scratch has no directories)
COPY --from=builder /tmp /tmp

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/s3-uploader"]
