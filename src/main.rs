use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use clap::Parser;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MiB 最小分片大小
const CHUNK_SIZE: usize = 64 * 1024;     // 64 KiB 缓冲区大小
const MAX_CONCURRENT_UPLOADS: usize = 4; // 并发上传分片数

#[derive(Parser)]
#[command(
    name = "s3-uploader",
    version,
    about = "Upload files to Amazon S3 or compatible services (MinIO, Cloudflare R2, etc.)",
    after_help = "Environment variables:\n  \
                  AWS_ACCESS_KEY_ID       AWS access key (required)\n  \
                  AWS_SECRET_ACCESS_KEY   AWS secret key (required)\n  \
                  AWS_REGION              AWS region [env: AWS_REGION]\n  \
                  S3_BUCKET               S3 bucket name [env: S3_BUCKET]\n  \
                  AWS_ENDPOINT_URL        Custom S3 endpoint [env: AWS_ENDPOINT_URL]"
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
    #[arg(short = 'e', long, env = "AWS_ENDPOINT_URL")]
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

    /// Max retries on transient errors (default: 3)
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
    Path::new(key)
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

// ── 1. 单文件单次 PUT 上传 ──

fn file_feeder(path: PathBuf, mut sender: hyper_0_14::body::Sender, uploader: Uploader, show_progress: bool) {
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

        let display_start = Instant::now();
        let mut last_display = display_start;
        if show_progress {
            eprint!("\r[00:00] 0 B uploaded    ");
            std::io::stderr().flush().ok();
        }

        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut loop_counter = 0u64;

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

            // 优化点：降低高频调用 Instant::now() 带来的 CPU 损耗
            loop_counter += 1;
            if show_progress && loop_counter % 32 == 0 {
                let now = Instant::now();
                if now.duration_since(last_display).as_millis() >= 1000 {
                    let total = uploader.count();
                    let s = now.duration_since(display_start).as_secs();
                    eprint!("\r[{:02}:{:02}] {} uploaded    ", s / 60, s % 60, human_bytes(total));
                    std::io::stderr().flush().ok();
                    last_display = now;
                }
            }
        }
        uploader.finish();
    });
}

fn file_body(path: &Path, counter: Arc<AtomicU64>, done: Arc<AtomicBool>, show_progress: bool) -> ByteStream {
    let (sender, body) = hyper_0_14::Body::channel();
    file_feeder(path.to_path_buf(), sender, Uploader { counter, done }, show_progress);
    ByteStream::from_body_0_4(body)
}

// ── 2. 流式数据多段并发上传 (支持 Stdin 或大文件) ──

async fn upload_multipart_stream<R>(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    content_type: &str,
    mut reader: R,
    uploader: &Uploader,
    max_retries: u32,
    show_progress: bool,
) -> Result<u64, Box<dyn std::error::Error>> 
where
    R: Read + Send + 'static,
{
    eprintln!("Uploading (Multipart)...");

    // 创建标准大小通道：控制背压，防止内存中积压过多未上传的 Part 块
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(MAX_CONCURRENT_UPLOADS);
    let done_flag = uploader.done.clone();

    // 独立的可控进度条异步线程
    let display_done = Arc::new(AtomicBool::new(false));
    if show_progress {
        let dc = uploader.counter.clone();
        let dd = display_done.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await;
            loop {
                let elapsed = start.elapsed().as_secs();
                let n = dc.load(Ordering::Relaxed);
                eprint!("\r[{:02}:{:02}] {} uploaded    ", elapsed / 60, elapsed % 60, human_bytes(n));
                std::io::stderr().flush().ok();
                if dd.load(Ordering::Relaxed) {
                    break;
                }
                interval.tick().await;
            }
        });
    }

    // 生产者线程：阻塞式地读取流并划分固定大小的块发送给 Channel
    let read_handle = tokio::task::spawn_blocking(move || {
        let mut read_buf = [0u8; CHUNK_SIZE];
        let mut buffer: Vec<u8> = Vec::with_capacity(PART_SIZE);

        // 利用作用域确保异常或正常退出时，_tx_guard 自动释放，Rx 端能正确接收到 None
        let _tx_guard = tx; 

        loop {
            match reader.read(&mut read_buf) {
                Ok(0) => break,
                Ok(n) => {
                    buffer.extend_from_slice(&read_buf[..n]);
                    while buffer.len() >= PART_SIZE {
                        let part = buffer[..PART_SIZE].to_vec();
                        buffer.drain(..PART_SIZE);
                        if _tx_guard.blocking_send(part).is_err() {
                            done_flag.store(true, Ordering::Relaxed);
                            return;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading stream: {e}");
                    done_flag.store(true, Ordering::Relaxed);
                    return;
                }
            }
        }

        if !buffer.is_empty() {
            let _ = _tx_guard.blocking_send(buffer);
        }
    });

    // 初始化 Multipart 上传任务
    let create_upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type(content_type)
        .send()
        .await?;

    let upload_id = create_upload.upload_id().ok_or("Missing upload_id")?.to_string();

    // 核心改进：引入并发控制，利用 JoinSet 并发上传不同 Part
    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));
    let client_arc = Arc::new(client.clone());
    let bucket_owned = bucket.to_string();
    let key_owned = key.to_string();
    let upload_id_owned = upload_id.clone();
    let counter_arc = uploader.counter.clone();

    let mut part_number: i32 = 1;
    let mut total_uploaded: u64 = 0;

    while let Some(data) = rx.recv().await {
        let size = data.len() as u64;
        total_uploaded += size;

        let client = Arc::clone(&client_arc);
        let bucket = bucket_owned.clone();
        let key = key_owned.clone();
        let upload_id = upload_id_owned.clone();
        let counter = Arc::clone(&counter_arc);
        let permit = semaphore.clone().acquire_owned().await?;

        // 异步派发上传任务
        join_set.spawn(async move {
            let _permit = permit; // 离开作用域时释放信号量
            let etag = upload_part_with_retry(
                &client,
                &bucket,
                &key,
                &upload_id,
                part_number,
                data,
                max_retries,
                counter,
            )
            .await?;
            Ok::<CompletedPart, Box<dyn std::error::Error + Send + Sync>>(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            )
        });

        part_number += 1;
    }

    // 收集所有并发 Part 的上传结果
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let mut upload_failed = false;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(completed_part)) => completed_parts.push(completed_part),
            _ => upload_failed = true,
        }
    }

    // 确保阻塞生产者线程安全退出
    let _ = read_handle.await;
    display_done.store(true, Ordering::Relaxed);

    // 排序 Part 列表（S3 协议严格要求 PartNumber 必须按升序排列）
    completed_parts.sort_by_key(|p| p.part_number());

    if upload_failed || completed_parts.is_empty() {
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        uploader.finish();
        return Err("Multipart upload failed or was empty".into());
    }

    // 闭合 Multipart 上传
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

    // 给进度条线程预留一点最后的输出冲刷时间
    tokio::time::sleep(Duration::from_millis(20)).await;

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
    counter: Arc<AtomicU64>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut attempt = 0u32;
    let known_size = data.len();

    loop {
        attempt += 1;
        let body_stream = ByteStream::from(data.clone());

        if attempt > 1 {
            eprintln!("  Part {part_number} retrying, attempt {attempt}...");
        }

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
                counter.fetch_add(known_size as u64, Ordering::Relaxed);
                return Ok(output.e_tag().unwrap_or_default().to_string());
            }
            Err(e) if attempt <= max_retries && is_retryable(&e) => {
                let delay = 2u64.pow(attempt);
                eprintln!("  Part {part_number} failed, retrying in {delay}s: {e}");
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            Err(e) => return Err(Box::new(e)),
        }
    }
}

// ── 3. 工具类公共函数 ──

fn is_retryable<E: std::fmt::Debug>(e: &aws_sdk_s3::error::SdkError<E>) -> bool {
    match e {
        aws_sdk_s3::error::SdkError::DispatchFailure(_) => true,
        aws_sdk_s3::error::SdkError::TimeoutError(_) => true,
        aws_sdk_s3::error::SdkError::ServiceError(err) => {
            let code = err.raw().status().as_u16();
            matches!(code, 429 | 500 | 502 | 503 | 504)
        }
        _ => false,
    }
}

// ── 4. Main 入口 ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let content_type = cli
        .content_type
        .as_deref()
        .unwrap_or_else(|| detect_content_type(&cli.key));

    // 构建 AWS SDK 配置
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

    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let uploader = Uploader { counter: counter.clone(), done: done.clone() };

    let total_bytes = if let Some(ref file_path) = cli.file {
        let meta = std::fs::metadata(file_path)?;
        let size = meta.len();

        // 优化点：如果文件大小超过限制(这里界定为大于 PART_SIZE)，则自动降级为多段流式上传
        if size > PART_SIZE as u64 {
            let file = std::fs::File::open(file_path)?;
            upload_multipart_stream(&client, &cli.bucket, &cli.key, content_type, file, &uploader, cli.retries, !cli.no_progress).await?
        } else {
            if size == 0 {
                eprintln!("Warning: input is empty, uploading 0 bytes");
            }
            let max_retries = cli.retries;
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
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            size
        }
    } else {
        // 从标准输入读取流式多段上传
        let stdin = std::io::stdin();
        upload_multipart_stream(&client, &cli.bucket, &cli.key, content_type, stdin, &uploader, cli.retries, !cli.no_progress).await?
    };

    uploader.finish();
    eprintln!("\nDone  {}", human_bytes(total_bytes));

    Ok(())
}

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
        assert_eq!(detect_content_type("a.nosuchext"), "application/octet-stream");
    }
}