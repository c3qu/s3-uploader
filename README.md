# s3-uploader

Minimal, fast CLI tool for uploading files to Amazon S3 and S3-compatible services (Cloudflare R2, MinIO, etc.). Supports pipe input and progress bars.

## Features

- **Pipe & file input** — `cat data.json | s3-uploader -b bucket -k data.json`
- **Progress bar** — percentage bar for files, spinner for stdin
- **Multi-provider** — works with S3, Cloudflare R2, MinIO, DigitalOcean Spaces
- **Docker image** — ~4 MB distroless image (scratch + UPX compressed)
- **Clean stdout** — prints the object URL, safe to pipe into other tools

## Install

### Cargo

```bash
cargo install s3-uploader
```

### Docker

```bash
docker pull tanxme/s3-uploader:latest
```

### Pre-built binary

```bash
curl -L https://github.com/tanxme/s3-uploader/releases/latest/download/s3-uploader-x86_64-linux -o s3-uploader
chmod +x s3-uploader
```

## Quick start

```bash
# Set credentials
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_REGION="us-east-1"

# Upload a file
s3-uploader -b my-bucket -k path/to/object.txt ./local-file.txt

# Pipe from another command
mysqldump ... | gzip | s3-uploader -b backups -k db/backup.sql.gz

# Pipe into Docker
cat ./large-file.bin | docker run --rm -i \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  s3-uploader:latest \
  -b my-bucket -k large-file.bin
```

## Usage

```
Usage: s3-uploader [OPTIONS] --bucket <BUCKET> --key <KEY> [FILE]

Arguments:
  [FILE]  File to upload. If omitted, reads from stdin.

Options:
  -b, --bucket <BUCKET>              S3 bucket name [env: S3_BUCKET]
  -k, --key <KEY>                    S3 object key (path in bucket)
  -r, --region <REGION>              Region [env: AWS_REGION] [default: us-east-1]
  -e, --endpoint <ENDPOINT>          Custom endpoint for S3-compatible services
  -t, --content-type <TYPE>          Content-Type [default: application/octet-stream]
  -p, --force-path-style <BOOL>      Force path-style addressing [possible values: true, false]
      --no-progress                  Disable progress bar
```

## Providers

### Amazon S3

```bash
s3-uploader -b my-bucket -k file.txt -r us-east-1 ./file.txt
```

### Cloudflare R2

```bash
s3-uploader \
  -b my-bucket -k file.txt \
  -e "https://<account_id>.r2.cloudflarestorage.com" \
  -r auto \
  ./file.txt
```

### MinIO / LocalStack

```bash
s3-uploader \
  -b my-bucket -k file.txt \
  -e http://localhost:9000 \
  ./file.txt
```

## Docker

The Docker image is built from `scratch` with a UPX-compressed binary and bundled CA certificates.

```bash
# Build
docker build -t s3-uploader .

# Run
docker run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  s3-uploader -b my-bucket -k file.txt ./file.txt
```

Image size: **~4 MB**.

## Authentication

Credentials are resolved via the standard AWS SDK chain:

1. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
2. Shared credentials file: `~/.aws/credentials`
3. IAM roles (EC2 / ECS)

For R2, create an API token in the Cloudflare Dashboard under **R2 → Manage R2 API Tokens**.

## Building from source

```bash
cargo build --release
# Binary: ./target/release/s3-uploader (~8 MB, or ~3 MB with UPX)
```

## License

MIT
