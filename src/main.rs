use std::io::{IsTerminal, Read};
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

enum Input {
    File { path: PathBuf, size: u64 },
    Stdin(Arc<Vec<u8>>),
}

fn read_input(file: Option<PathBuf>) -> Result<Input, Box<dyn std::error::Error>> {
    if let Some(path) = file {
        let meta = std::fs::metadata(&path)?;
        Ok(Input::File {
            size: meta.len(),
            path,
        })
    } else {
        let mut data = Vec::new();
        std::io::stdin().read_to_end(&mut data)?;
        Ok(Input::Stdin(Arc::new(data)))
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

fn memory_feeder(data: Arc<Vec<u8>>, mut sender: hyper_0_14::body::Sender, uploader: Uploader) {
    tokio::spawn(async move {
        let chunk_sz: usize = 64 * 1024;
        let mut offset = 0;
        while offset < data.len() {
            let end = std::cmp::min(offset + chunk_sz, data.len());
            let len = end - offset;
            let chunk = bytes::Bytes::copy_from_slice(&data[offset..end]);
            if sender.send_data(chunk).await.is_err() {
                break;
            }
            offset = end;
            uploader.report(len as u64);
        }
        uploader.finish();
    });
}

fn make_body(
    input: &Input,
    counter: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    pb: Option<ProgressBar>,
) -> ByteStream {
    let (sender, body) = hyper_0_14::Body::channel();
    let uploader = Uploader { counter, done, pb };

    match input {
        Input::File { path, .. } => file_feeder(path, sender, uploader),
        Input::Stdin(data) => memory_feeder(Arc::clone(data), sender, uploader),
    }

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

    let (size, known_size) = match &input {
        Input::File { size, .. } => (*size, true),
        Input::Stdin(data) => (data.len() as u64, false),
    };

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
