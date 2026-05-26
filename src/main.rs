use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Parser)]
#[command(
    name = "s3-uploader",
    version,
    about = "Upload files to Amazon S3 or compatible services (MinIO, Cloudflare R2, etc.)"
)]
struct Cli {
    /// S3 bucket name
    #[arg(short = 'b', long, env = "S3_BUCKET")]
    bucket: String,

    /// S3 object key (path in bucket)
    #[arg(short = 'k', long)]
    key: String,

    /// Region (use "auto" for Cloudflare R2, "us-east-1" for MinIO)
    #[arg(short = 'r', long, default_value = "us-east-1", env = "AWS_REGION")]
    region: String,

    /// Custom S3 endpoint (e.g., for MinIO or Cloudflare R2)
    #[arg(short = 'e', long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// Content-Type of the uploaded object
    #[arg(short = 't', long, default_value = "application/octet-stream")]
    content_type: String,

    /// Force path-style addressing (auto-enabled for custom endpoints)
    #[arg(short = 'p', long)]
    force_path_style: Option<bool>,

    /// Disable progress bar
    #[arg(long)]
    no_progress: bool,

    /// Max retries on transient errors (default: 3)
    #[arg(long, default_value = "3")]
    retries: u32,

    /// File to upload. If omitted, reads from stdin.
    file: Option<PathBuf>,
}

/// Unified input: always backed by a file on disk.
/// For stdin, data is streamed into a temporary file to avoid OOM.
struct UploadInput {
    path: PathBuf,
    size: u64,
    /// Whether this is a temp file that should be cleaned up on drop.
    is_temp: bool,
}

impl Drop for UploadInput {
    fn drop(&mut self) {
        if self.is_temp {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn read_input(file: Option<PathBuf>) -> Result<UploadInput, Box<dyn std::error::Error>> {
    if let Some(path) = file {
        let meta = std::fs::metadata(&path)?;
        Ok(UploadInput {
            size: meta.len(),
            path,
            is_temp: false,
        })
    } else {
        // Stream stdin into a temp file instead of buffering in RAM.
        let tmp_dir = std::env::temp_dir();
        std::fs::create_dir_all(&tmp_dir)?;
        let tmp_path = tmp_dir.join(format!("s3-uploader-{}", std::process::id()));
        let mut tmp_file = std::fs::File::create(&tmp_path)?;
        let mut stdin = std::io::stdin().lock();
        let mut buf = [0u8; 64 * 1024];
        let mut size = 0u64;
        loop {
            let n = stdin.read(&mut buf)?;
            if n == 0 {
                break;
            }
            tmp_file.write_all(&buf[..n])?;
            size += n as u64;
        }
        tmp_file.flush()?;
        Ok(UploadInput {
            path: tmp_path,
            size,
            is_temp: true,
        })
    }
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

fn location(
    endpoint: &Option<String>,
    bucket: &str,
    key: &str,
    region: &str,
    force_path_style: bool,
) -> String {
    match endpoint {
        Some(ref ep) => {
            if force_path_style {
                format!("{}/{}/{}", ep.trim_end_matches('/'), bucket, key)
            } else {
                format!("{}/{}", ep.trim_end_matches('/'), key)
            }
        }
        None => format!("https://{}.s3.{}.amazonaws.com/{}", bucket, region, key),
    }
}

struct Uploader {
    counter: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    pb: Option<ProgressBar>,
}

impl Uploader {
    fn report(&self, n: u64) {
        self.counter.fetch_add(n, Ordering::Relaxed);
        if let Some(ref pb) = self.pb {
            pb.inc(n);
        }
    }

    fn finish(&self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(ref pb) = self.pb {
            pb.finish_and_clear();
        }
    }
}

fn file_feeder(path: &std::path::Path, mut sender: hyper_0_14::body::Sender, uploader: Uploader) {
    let path = path.to_path_buf();
    tokio::spawn(async move {
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                sender.abort();
                eprintln!("Error opening file: {e}");
                uploader.finish();
                return;
            }
        };
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match tokio::io::AsyncReadExt::read(&mut file, &mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Error reading file: {e}");
                    sender.abort();
                    uploader.finish();
                    return;
                }
            };
            let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
            if sender.send_data(chunk).await.is_err() {
                break;
            }
            uploader.report(n as u64);
        }
        uploader.finish();
    });
}

fn make_body(
    input: &UploadInput,
    counter: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    pb: Option<ProgressBar>,
) -> ByteStream {
    let (sender, body) = hyper_0_14::Body::channel();
    let uploader = Uploader { counter, done, pb };
    file_feeder(&input.path, sender, uploader);
    ByteStream::from_body_0_4(body)
}

fn is_retryable(
    e: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> bool {
    match e {
        aws_sdk_s3::error::SdkError::DispatchFailure(_) => true,
        aws_sdk_s3::error::SdkError::TimeoutError(_) => true,
        aws_sdk_s3::error::SdkError::ServiceError(err) => {
            let code = err.raw().status().as_u16();
            code == 429 || code == 500 || code == 502 || code == 503 || code == 504
        }
        _ => false,
    }
}

fn create_progress_bar(size: u64, known_size: bool) -> ProgressBar {
    if known_size {
        let pb = ProgressBar::new(size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({eta})",
                )
                .unwrap()
                .progress_chars("━●─"),
        );
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {bytes} uploaded")
                .unwrap(),
        );
        pb
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let input = read_input(cli.file)?;

    let size = input.size;
    // We always know the size now — stdin is written to a temp file first.
    let known_size = true;

    if size == 0 {
        eprintln!("Warning: input is empty, uploading 0 bytes");
    }

    let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(cli.region.clone()));
    if let Some(ref endpoint) = cli.endpoint {
        config_builder = config_builder.endpoint_url(endpoint);
    }
    let sdk_config = config_builder.load().await;
    let force_path_style = cli.force_path_style.unwrap_or(cli.endpoint.is_some());
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(force_path_style)
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);

    let is_tty = std::io::stderr().is_terminal() && !cli.no_progress;

    // Retry loop
    let max_retries = cli.retries;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if attempt > 1 {
            eprintln!("  Retrying ({attempt}/{max_retries})...");
        }

        let result = if size == 0 {
            client
                .put_object()
                .bucket(&cli.bucket)
                .key(&cli.key)
                .body(ByteStream::from(Vec::new()))
                .content_type(&cli.content_type)
                .send()
                .await
                .map(|_| ())
        } else if !is_tty {
            let (counter, done) = spawn_logger(size, known_size, attempt);
            let body = make_body(&input, counter, done, None);
            send_upload(
                &client,
                &cli.bucket,
                &cli.key,
                &cli.content_type,
                size,
                body,
            )
            .await
        } else {
            let pb = create_progress_bar(size, known_size);
            let counter = Arc::new(AtomicU64::new(0));
            let done = Arc::new(AtomicBool::new(false));
            let body = make_body(&input, counter, done, Some(pb));
            send_upload(
                &client,
                &cli.bucket,
                &cli.key,
                &cli.content_type,
                size,
                body,
            )
            .await
        };

        match result {
            Ok(_) => break,
            Err(e) if attempt < max_retries && is_retryable(&e) => {
                let delay = 2u64.pow(attempt);
                eprintln!("  {e}");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    let loc = location(
        &cli.endpoint,
        &cli.bucket,
        &cli.key,
        &cli.region,
        force_path_style,
    );
    eprintln!("Done  {size} bytes → {loc}");
    println!("{loc}");
    Ok(())
}

async fn send_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    content_type: &str,
    size: u64,
    body: ByteStream,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .content_type(content_type)
        .content_length(size as i64)
        .send()
        .await
        .map(|_| ())
}

fn spawn_logger(size: u64, known_size: bool, attempt: u32) -> (Arc<AtomicU64>, Arc<AtomicBool>) {
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let c = counter.clone();
    let d = done.clone();
    let total = size;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.tick().await;
        loop {
            interval.tick().await;
            if d.load(Ordering::Relaxed) {
                break;
            }
            let n = c.load(Ordering::Relaxed);
            if attempt > 1 {
                eprintln!(
                    "  Attempt {attempt}: {}/{}",
                    human_bytes(n),
                    human_bytes(total)
                );
            } else if known_size {
                let pct = if total > 0 {
                    (n as f64 / total as f64) * 100.0
                } else {
                    0.0
                };
                eprintln!("  {}/{} ({pct:.0}%)", human_bytes(n), human_bytes(total));
            } else {
                eprintln!("  {} uploaded", human_bytes(n));
            }
        }
    });

    (counter, done)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::error::{ConnectorError, ErrorMetadata, SdkError};
    use aws_sdk_s3::operation::put_object::PutObjectError;
    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime_api::http::{Response, StatusCode};

    // -----------------------------------------------------------------------
    // human_bytes
    // -----------------------------------------------------------------------
    #[test]
    fn human_bytes_exact_bytes() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1), "1 B");
        assert_eq!(human_bytes(1023), "1023 B");
    }

    #[test]
    fn human_bytes_kib() {
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(1024 * 1024 - 1), "1024.0 KiB");
    }

    #[test]
    fn human_bytes_mib() {
        assert_eq!(human_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(human_bytes(100 * 1024 * 1024), "100.0 MiB");
    }

    #[test]
    fn human_bytes_gib() {
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn human_bytes_tib() {
        assert_eq!(human_bytes(1024 * 1024 * 1024 * 1024), "1.0 TiB");
    }

    // -----------------------------------------------------------------------
    // location
    // -----------------------------------------------------------------------
    #[test]
    fn location_aws_default() {
        let loc = location(&None, "my-bucket", "path/to/obj", "us-east-1", false);
        assert_eq!(
            loc,
            "https://my-bucket.s3.us-east-1.amazonaws.com/path/to/obj"
        );
    }

    #[test]
    fn location_custom_endpoint_path_style() {
        let loc = location(
            &Some("https://minio.example.com".into()),
            "my-bucket",
            "data.json",
            "us-east-1",
            true,
        );
        assert_eq!(loc, "https://minio.example.com/my-bucket/data.json");
    }

    #[test]
    fn location_custom_endpoint_virtual_hosted() {
        let loc = location(
            &Some("https://r2.example.com".into()),
            "my-bucket",
            "data.json",
            "auto",
            false,
        );
        assert_eq!(loc, "https://r2.example.com/data.json");
    }

    #[test]
    fn location_custom_endpoint_trailing_slash() {
        let loc = location(
            &Some("https://minio.example.com/".into()),
            "my-bucket",
            "data.json",
            "us-east-1",
            true,
        );
        assert_eq!(loc, "https://minio.example.com/my-bucket/data.json");
    }

    // -----------------------------------------------------------------------
    // is_retryable
    // -----------------------------------------------------------------------
    fn make_service_error(status: u16) -> SdkError<PutObjectError> {
        let meta = ErrorMetadata::builder()
            .code("TestError")
            .message("test")
            .build();
        let err = PutObjectError::generic(meta);
        let raw = Response::new(
            StatusCode::try_from(status).unwrap(),
            SdkBody::from(""),
        );
        SdkError::service_error(err, raw)
    }

    #[test]
    fn is_retryable_dispatch_failure() {
        let err = SdkError::dispatch_failure(ConnectorError::timeout(
            "connection timed out".into(),
        ));
        assert!(is_retryable(&err));
    }

    #[test]
    fn is_retryable_timeout_error() {
        let err = SdkError::timeout_error("request timed out");
        assert!(is_retryable(&err));
    }

    #[test]
    fn is_retryable_service_429() {
        assert!(is_retryable(&make_service_error(429)));
    }

    #[test]
    fn is_retryable_service_500() {
        assert!(is_retryable(&make_service_error(500)));
    }

    #[test]
    fn is_retryable_service_502() {
        assert!(is_retryable(&make_service_error(502)));
    }

    #[test]
    fn is_retryable_service_503() {
        assert!(is_retryable(&make_service_error(503)));
    }

    #[test]
    fn is_retryable_service_504() {
        assert!(is_retryable(&make_service_error(504)));
    }

    #[test]
    fn is_not_retryable_service_400() {
        assert!(!is_retryable(&make_service_error(400)));
    }

    #[test]
    fn is_not_retryable_service_403() {
        assert!(!is_retryable(&make_service_error(403)));
    }

    #[test]
    fn is_not_retryable_service_404() {
        assert!(!is_retryable(&make_service_error(404)));
    }

    #[test]
    fn is_not_retryable_construction_failure() {
        let err = SdkError::construction_failure("bad config");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn is_not_retryable_response_error() {
        let raw = Response::new(StatusCode::try_from(200).unwrap(), SdkBody::from(""));
        let err = SdkError::response_error("parse failure", raw);
        assert!(!is_retryable(&err));
    }

    // -----------------------------------------------------------------------
    // read_input — file path
    // -----------------------------------------------------------------------
    #[test]
    fn read_input_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"hello world").unwrap();

        let input = read_input(Some(file_path.clone())).unwrap();
        assert_eq!(input.size, 11);
        assert_eq!(input.path, file_path);
        assert!(!input.is_temp);
    }

    #[test]
    fn read_input_nonexistent_file() {
        let result = read_input(Some(PathBuf::from("/nonexistent/path/foo.bar")));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // UploadInput Drop — temp file cleanup
    // -----------------------------------------------------------------------
    #[test]
    fn upload_input_drop_removes_temp() {
        let tmp_path;
        {
            let input = UploadInput {
                path: {
                    let p = std::env::temp_dir().join("s3-uploader-test-drop");
                    tmp_path = p.clone();
                    p
                },
                size: 42,
                is_temp: true,
            };
            // create a real file so remove_file can succeed
            std::fs::write(&input.path, b"test").unwrap();
            assert!(input.path.exists());
        } // Drop runs here
        assert!(!tmp_path.exists());
    }

    #[test]
    fn upload_input_drop_keeps_non_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keep-me.txt");
        std::fs::write(&path, b"data").unwrap();
        {
            let _input = UploadInput {
                path: path.clone(),
                size: 4,
                is_temp: false,
            };
        }
        // File must still exist — is_temp = false means no cleanup.
        assert!(path.exists());
    }

    // -----------------------------------------------------------------------
    // create_progress_bar
    // -----------------------------------------------------------------------
    #[test]
    fn progress_bar_known_size() {
        let pb = create_progress_bar(1024, true);
        // Bar with known size should have a non-zero length.
        assert!(pb.length().unwrap() > 0);
    }

    #[test]
    fn progress_bar_unknown_size() {
        let pb = create_progress_bar(0, false);
        // Spinner mode — no length.
        assert!(pb.length().is_none());
    }
}
