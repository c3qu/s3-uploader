use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::pin::Pin;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use clap::Parser;

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MiB minimum for S3 multipart
const CHUNK_SIZE: usize = 64 * 1024;

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

    /// Content-Type (auto-detected from key extension if not set)
    #[arg(short = 't', long)]
    content_type: Option<String>,

    /// Force path-style addressing (auto-enabled for custom endpoints)
    #[arg(short = 'p', long)]
    force_path_style: Option<bool>,

    /// Disable progress bar
    #[arg(long)]
    no_progress: bool,

    /// Max retries on transient errors (file upload only, default: 3)
    #[arg(long, default_value = "3")]
    retries: u32,

    /// File to upload. If omitted, reads from stdin (multipart).
    file: Option<PathBuf>,
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

fn detect_content_type(key: &str) -> &str {
    std::path::Path::new(key)
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| mime_guess::from_ext(ext).first_raw())
        .unwrap_or("application/octet-stream")
}

struct Uploader {
    counter: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
}

impl Uploader {
    fn report(&self, n: u64) {
        self.counter.fetch_add(n, Ordering::Relaxed);
    }

    fn count(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn finish(&self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

// ── 用于线性统计网络发送的包装流 ──
struct TrackingReader<R> {
    inner: R,
    counter: Arc<AtomicU64>,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for TrackingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let after = buf.filled().len();
            let written = after - before;
            if written > 0 {
                // 网络每读走一点数据，进度条计数器就立刻实时增加
                self.counter.fetch_add(written as u64, Ordering::Relaxed);
            }
        }
        result
    }
}

// ── File upload (single put_object with known size) ──

fn file_feeder(path: &std::path::Path, mut sender: hyper_0_14::body::Sender, uploader: Uploader, show_progress: bool) {
    let path = path.to_path_buf();
    tokio::spawn(async move {
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                sender.abort();
                eprintln!("Error: {e}");
                uploader.finish();
                return;
            }
        };
        let display_start = std::time::Instant::now();
        let mut last_display = display_start;
        if show_progress {
            eprint!("\r[00:00] 0 B uploaded    ");
            std::io::stderr().flush().ok();
        }
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let n = match tokio::io::AsyncReadExt::read(&mut file, &mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Error: {e}");
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

            if show_progress {
                let now = std::time::Instant::now();
                let since_ms = now.duration_since(last_display).as_millis();
                if since_ms >= 1000 {
                    let total = uploader.count();
                    let s = now.duration_since(display_start).as_secs();
                    eprint!(
                        "\r[{:02}:{:02}] {} uploaded    ",
                        s / 60,
                        s % 60,
                        human_bytes(total)
                    );
                    std::io::stderr().flush().ok();
                    last_display = now;
                }
            }
        }
        uploader.finish();
    });
}

fn file_body(
    path: &std::path::Path,
    counter: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    show_progress: bool,
) -> ByteStream {
    let (sender, body) = hyper_0_14::Body::channel();
    file_feeder(path, sender, Uploader { counter, done }, show_progress);
    ByteStream::from_body_0_4(body)
}

// ── Stdin multipart upload ──

async fn upload_stdin(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    content_type: &str,
    uploader: &Uploader,
    max_retries: u32,
    _read_counter: Arc<AtomicU64>, 
    done_flag: Arc<AtomicBool>,
    show_progress: bool,
) -> Result<u64, Box<dyn std::error::Error>> {
    eprintln!("Uploading...");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let rd = done_flag;

    // 1-second 定时刷新进度条线程
    let display_done = Arc::new(AtomicBool::new(false));
    if show_progress {
        let dc = uploader.counter.clone(); 
        let dd = display_done.clone();
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.tick().await; 
            loop {
                let elapsed = start.elapsed().as_secs();
                let n = dc.load(Ordering::Relaxed);
                eprint!(
                    "\r[{:02}:{:02}] {} uploaded    ",
                    elapsed / 60,
                    elapsed % 60,
                    human_bytes(n)
                );
                std::io::stderr().flush().ok();
                if dd.load(Ordering::Relaxed) {
                    break;
                }
                interval.tick().await;
            }
        });
    }

    let read_handle = tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let mut read_buf = [0u8; CHUNK_SIZE];
        let mut buffer: Vec<u8> = Vec::with_capacity(PART_SIZE);

        loop {
            let n = match handle.read(&mut read_buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Error reading stdin: {e}");
                    rd.store(true, Ordering::Relaxed);
                    return;
                }
            };
            buffer.extend_from_slice(&read_buf[..n]);

            while buffer.len() >= PART_SIZE {
                let part = buffer[..PART_SIZE].to_vec();
                buffer.drain(..PART_SIZE);
                if tx.blocking_send(part).is_err() {
                    rd.store(true, Ordering::Relaxed);
                    return;
                }
            }
        }

        if !buffer.is_empty() {
            tx.blocking_send(buffer).ok();
        }
        rd.store(true, Ordering::Relaxed);
    });

    let create_fut = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type(content_type)
        .send();

    let (create, first_part) = tokio::join!(create_fut, rx.recv());

    let create = create?;
    let upload_id = create.upload_id().ok_or("missing upload_id")?.to_string();

    let mut part_number: i32 = 1;
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let mut total_uploaded: u64 = 0;

    let mut pending = first_part;
    'outer: loop {
        let data = if let Some(d) = pending.take() {
            d
        } else {
            loop {
                match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
                    Ok(Some(d)) => break d,
                    Ok(None) => break 'outer,
                    Err(_) => continue,
                }
            }
        };
        let size = data.len() as u64;
        
        // 【核心改进】：传入计数器引用，使得上传时底层网络读到哪里，进度条就能涨到哪里
        let etag = upload_part_with_retry(
            client,
            bucket,
            key,
            &upload_id,
            part_number,
            data,
            max_retries,
            uploader.counter.clone(),
        )
        .await?;

        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(etag)
                .build(),
        );
        total_uploaded += size;
        part_number += 1;
    }

    read_handle.await?;

    if completed_parts.is_empty() {
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        display_done.store(true, Ordering::Relaxed);
        uploader.finish();
        return Ok(0);
    }

    let result = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await;

    display_done.store(true, Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    match result {
        Ok(_) => {
            uploader.finish();
            Ok(total_uploaded)
        }
        Err(e) => {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(e.into())
        }
    }
}

async fn upload_part_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    data: Vec<u8>,
    max_retries: u32,
    counter: Arc<AtomicU64>, // 接收计数器
) -> Result<String, Box<dyn std::error::Error>> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        
        // 【核心改进】：利用 TrackingReader 包装底层数据输入流，将其转换为 ByteStream
        let cursor = std::io::Cursor::new(data.clone());
        let tracking_reader = TrackingReader {
            inner: cursor,
            counter: counter.clone(),
        };
        let body_stream = ByteStream::from_body_0_4(hyper_0_14::Body::wrap_stream(
            tokio_util::io::ReaderStream::new(tracking_reader),
        ));

        // 如果这是重试，需要把上一次错误增加的数据对冲减掉，保证计数精确
        if attempt > 1 {
            // 重试时，让底层流重新计数
            eprintln!("  Part {part_number} retrying, attempt {attempt}...");
        }

        let start_count = counter.load(Ordering::Relaxed);
		let known_size = data.len();
        match client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
			.content_length(known_size as i64)
            .body(body_stream)
            .send()
            .await
        {
            Ok(output) => {
                return Ok(output.e_tag().unwrap_or_default().to_string());
            }
            Err(e) if attempt < max_retries && is_retryable(&e) => {
                // 如果失败了，要把刚才这个分片已经错误加上的网络字节数扣除（回滚），防止数字虚高
                let end_count = counter.load(Ordering::Relaxed);
                let uploaded_in_this_failed_attempt = end_count.saturating_sub(start_count);
                counter.fetch_sub(uploaded_in_this_failed_attempt, Ordering::Relaxed);

                let delay = 2u64.pow(attempt);
                eprintln!("  Part {part_number} failed, retrying in {delay}s: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

// ── Shared ──

fn is_retryable<E: std::fmt::Debug>(e: &aws_sdk_s3::error::SdkError<E>) -> bool {
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

// ── Main ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let content_type = cli
        .content_type
        .as_deref()
        .unwrap_or_else(|| detect_content_type(&cli.key));

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

    if let Some(ref file_path) = cli.file {
        // ── File upload ──
        let meta = std::fs::metadata(file_path)?;
        let size = meta.len();

        if size == 0 {
            eprintln!("Warning: input is empty, uploading 0 bytes");
        }

        let max_retries = cli.retries;
        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if attempt > 1 {
                eprintln!("  Retrying ({attempt}/{max_retries})...");
            }

            let body = file_body(file_path, counter.clone(), done.clone(), !cli.no_progress);
            let result = client
                .put_object()
                .bucket(&cli.bucket)
                .key(&cli.key)
                .body(body)
                .content_type(content_type)
                .content_length(size as i64)
                .send()
                .await;

            match result {
                Ok(_) => break,
                Err(e) if attempt <= max_retries && is_retryable(&e) => {
                    let delay = 2u64.pow(attempt);
                    eprintln!("  {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }

        done.store(true, Ordering::Relaxed);
        eprintln!("\nDone  {}", human_bytes(size));
    } else {
        // ── Stdin multipart upload ──
        let counter = Arc::new(AtomicU64::new(0));
        let read_counter = counter.clone();
        let done = Arc::new(AtomicBool::new(false));
        let uploader = Uploader { counter, done: done.clone() };

        let total = upload_stdin(
            &client,
            &cli.bucket,
            &cli.key,
            content_type,
            &uploader,
            cli.retries,
            read_counter,
            done,
            !cli.no_progress,
        )
        .await?;

        uploader.finish();
        eprintln!("\nDone  {}", human_bytes(total));
    }

    Ok(())
}

// ── Tests ──
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_basic() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.0 MiB");
    }

    #[test]
    fn detect_content_type_works() {
        assert_eq!(detect_content_type("a.jpg"), "image/jpeg");
        assert_eq!(detect_content_type("a.html"), "text/html");
        assert_eq!(detect_content_type("a.json"), "application/json");
        assert_eq!(
            detect_content_type("a.nosuchext"),
            "application/octet-stream"
        );
    }
}