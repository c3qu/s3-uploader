use std::io::{IsTerminal, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Parser)]
#[command(name = "s3-uploader")]
#[command(about = "Upload files to Amazon S3 or compatible services (MinIO, Cloudflare R2, etc.)")]
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

    /// File to upload. If omitted, reads from stdin.
    file: Option<PathBuf>,
}

fn read_input(file: Option<PathBuf>) -> Result<(Vec<u8>, bool), Box<dyn std::error::Error>> {
    if let Some(path) = file {
        let data = std::fs::read(&path)?;
        Ok((data, true))
    } else {
        let mut data = Vec::new();
        std::io::stdin().read_to_end(&mut data)?;
        Ok((data, false))
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

fn location(endpoint: &Option<String>, bucket: &str, key: &str, region: &str, force_path_style: bool) -> String {
    match endpoint {
        Some(ref ep) => {
            if force_path_style {
                format!("{}/{}/{}", ep.trim_end_matches('/'), bucket, key)
            } else {
                format!("{}/{}", ep.trim_end_matches('/'), key)
            }
        }
        None => format!(
            "https://{}.s3.{}.amazonaws.com/{}",
            bucket, region, key
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let (data, known_size) = read_input(cli.file)?;
    let size = data.len();

    if size == 0 {
        eprintln!("Warning: input is empty, uploading 0 bytes");
    }

    // Build S3 client
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

    // Upload
    let is_tty = std::io::stderr().is_terminal() && !cli.no_progress;

    if size == 0 || cli.no_progress || !is_tty {
        // No progress: direct upload (or non-TTY with periodic logging)
        let done_flag = if size > 0 && !cli.no_progress {
            let counter = Arc::new(AtomicU64::new(0));
            let done = Arc::new(AtomicBool::new(false));

            let c = counter.clone();
            let d = done.clone();
            let total = size as u64;
            let known = known_size;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if d.load(Ordering::Relaxed) {
                        break;
                    }
                    let n = c.load(Ordering::Relaxed);
                    if known {
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

            // Use streaming body so we can increment the counter
            let (mut sender, body) = hyper_0_14::Body::channel();
            let c2 = counter.clone();
            let chunk_sz: usize = 64 * 1024;
            tokio::spawn(async move {
                let mut offset = 0;
                while offset < data.len() {
                    let end = std::cmp::min(offset + chunk_sz, data.len());
                    let chunk = bytes::Bytes::copy_from_slice(&data[offset..end]);
                    if sender.send_data(chunk).await.is_err() {
                        break;
                    }
                    offset = end;
                    c2.store(offset as u64, Ordering::Relaxed);
                }
            });

            let byte_stream = ByteStream::from_body_0_4(body);
            client
                .put_object()
                .bucket(&cli.bucket)
                .key(&cli.key)
                .body(byte_stream)
                .content_type(&cli.content_type)
                .content_length(size as i64)
                .send()
                .await?;

            done.store(true, Ordering::Relaxed);
            None
        } else {
            client
                .put_object()
                .bucket(&cli.bucket)
                .key(&cli.key)
                .body(ByteStream::from(data))
                .content_type(&cli.content_type)
                .send()
                .await?;
            None::<Arc<AtomicBool>>
        };
        drop(done_flag);
    } else {
        // TTY: show animated progress bar
        let pb = if known_size {
            let pb = ProgressBar::new(size as u64);
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
        };

        let (mut sender, body) = hyper_0_14::Body::channel();
        let pb2 = pb.clone();
        let chunk_sz: usize = 64 * 1024;
        tokio::spawn(async move {
            let mut offset = 0;
            while offset < data.len() {
                let end = std::cmp::min(offset + chunk_sz, data.len());
                let chunk = bytes::Bytes::copy_from_slice(&data[offset..end]);
                let len = chunk.len() as u64;
                if sender.send_data(chunk).await.is_err() {
                    break;
                }
                offset = end;
                pb2.inc(len);
            }
            pb2.finish_and_clear();
        });

        let byte_stream = ByteStream::from_body_0_4(body);
        client
            .put_object()
            .bucket(&cli.bucket)
            .key(&cli.key)
            .body(byte_stream)
            .content_type(&cli.content_type)
            .content_length(size as i64)
            .send()
            .await?;
    }

    let loc = location(&cli.endpoint, &cli.bucket, &cli.key, &cli.region, force_path_style);
    eprintln!("Done  {size} bytes → {loc}");
    println!("{loc}");
    Ok(())
}
