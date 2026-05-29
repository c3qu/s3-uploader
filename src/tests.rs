use super::*;

// ── human_bytes ──

#[test]
fn human_bytes_zero() {
    assert_eq!(human_bytes(0), "0 B");
}

#[test]
fn human_bytes_bytes() {
    assert_eq!(human_bytes(1), "1 B");
    assert_eq!(human_bytes(512), "512 B");
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
    assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    assert_eq!(human_bytes(100 * 1024 * 1024), "100.0 MiB");
}

#[test]
fn human_bytes_gib() {
    assert_eq!(human_bytes(1024 * 1024 * 1024), "1.0 GiB");
    assert_eq!(human_bytes(2 * 1024 * 1024 * 1024), "2.0 GiB");
}

#[test]
fn human_bytes_tib() {
    assert_eq!(human_bytes(1024 * 1024 * 1024 * 1024), "1.0 TiB");
    assert_eq!(human_bytes(3 * 1024 * 1024 * 1024 * 1024), "3.0 TiB");
}

#[test]
fn human_bytes_large_value() {
    // 10 TiB — should stay at TiB, not overflow
    let ten_tib = 10u64 * 1024 * 1024 * 1024 * 1024;
    assert_eq!(human_bytes(ten_tib), "10.0 TiB");
}

// ── detect_content_type ──

#[test]
fn detect_content_type_images() {
    assert_eq!(detect_content_type("photo.jpg"), "image/jpeg");
    assert_eq!(detect_content_type("photo.jpeg"), "image/jpeg");
    assert_eq!(detect_content_type("icon.png"), "image/png");
    assert_eq!(detect_content_type("logo.gif"), "image/gif");
    assert_eq!(detect_content_type("img.svg"), "image/svg+xml");
    assert_eq!(detect_content_type("img.webp"), "image/webp");
}

#[test]
fn detect_content_type_text() {
    assert_eq!(detect_content_type("page.html"), "text/html");
    assert_eq!(detect_content_type("page.htm"), "text/html");
    assert_eq!(detect_content_type("style.css"), "text/css");
    assert_eq!(detect_content_type("notes.txt"), "text/plain");
    assert_eq!(detect_content_type("readme.md"), "text/markdown");
}

#[test]
fn detect_content_type_data() {
    assert_eq!(detect_content_type("data.json"), "application/json");
    assert_eq!(detect_content_type("data.xml"), "text/xml");
    assert_eq!(detect_content_type("file.pdf"), "application/pdf");
    assert_eq!(detect_content_type("file.zip"), "application/zip");
    assert_eq!(detect_content_type("file.tar.gz"), "application/gzip"); // extension is "gz"
}

#[test]
fn detect_content_type_audio_video() {
    assert_eq!(detect_content_type("song.mp3"), "audio/mpeg");
    assert_eq!(detect_content_type("audio.wav"), "audio/wav");
    assert_eq!(detect_content_type("video.mp4"), "video/mp4");
}

#[test]
fn detect_content_type_no_extension() {
    assert_eq!(detect_content_type("Makefile"), "application/octet-stream");
    assert_eq!(detect_content_type("noext"), "application/octet-stream");
}

#[test]
fn detect_content_type_unknown_extension() {
    assert_eq!(
        detect_content_type("file.unknown_ext"),
        "application/octet-stream"
    );
}

#[test]
fn detect_content_type_case_insensitive() {
    // Path::extension() returns the extension as-is;
    // mime_guess::from_ext is case-insensitive
    assert_eq!(detect_content_type("photo.JPG"), "image/jpeg");
    assert_eq!(detect_content_type("data.JSON"), "application/json");
}

#[test]
fn detect_content_type_hidden_file() {
    // Files starting with dot but with an extension
    assert_eq!(
        detect_content_type(".gitignore"),
        "application/octet-stream"
    ); // no extension
    assert_eq!(detect_content_type(".hidden.json"), "application/json");
}

#[test]
fn detect_content_type_multiple_dots() {
    assert_eq!(detect_content_type("archive.tar.gz"), "application/gzip");
    assert_eq!(detect_content_type("file.min.css"), "text/css");
    assert_eq!(detect_content_type("file.min.js"), "text/javascript");
}

// ── Uploader ──

#[test]
fn uploader_report_and_count() {
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let uploader = Uploader {
        counter: counter.clone(),
        done: done.clone(),
    };

    assert_eq!(uploader.count(), 0);

    uploader.report(42);
    assert_eq!(uploader.count(), 42);

    uploader.report(100);
    assert_eq!(uploader.count(), 142);
}

#[test]
fn uploader_finish() {
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let uploader = Uploader {
        counter,
        done: done.clone(),
    };

    assert!(!done.load(Ordering::Relaxed));
    uploader.finish();
    assert!(done.load(Ordering::Relaxed));
}

#[test]
fn uploader_shared_counter_across_clones() {
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let u1 = Uploader {
        counter: counter.clone(),
        done: done.clone(),
    };
    let u2 = Uploader {
        counter: counter.clone(),
        done,
    };

    u1.report(10);
    u2.report(20);
    assert_eq!(u1.count(), 30);
    assert_eq!(u2.count(), 30);
}

// ── Constants ──

#[test]
fn constants_have_expected_values() {
    assert_eq!(PART_SIZE, 5 * 1024 * 1024);
    assert_eq!(CHUNK_SIZE, 64 * 1024);
    assert_eq!(MAX_CONCURRENT_UPLOADS, 4);
}

// ── is_retryable ──

#[test]
fn is_retryable_timeout() {
    let err = aws_sdk_s3::error::SdkError::<()>::timeout_error("timeout");
    assert!(is_retryable(&err));
}

#[test]
fn is_retryable_dispatch_failure() {
    let err = aws_sdk_s3::error::SdkError::<()>::dispatch_failure(
        aws_sdk_s3::error::ConnectorError::other("connector error".into(), None),
    );
    assert!(is_retryable(&err));
}

// ── CLI argument parsing ──

#[test]
fn cli_parses_minimal_args() {
    use clap::Parser;
    let args = Cli::try_parse_from(["s3-uploader", "-b", "mybucket", "-k", "mykey", "file.txt"]);
    let cli = args.unwrap();
    assert_eq!(cli.bucket, "mybucket");
    assert_eq!(cli.key, "mykey");
    assert_eq!(cli.file, Some(PathBuf::from("file.txt")));
    assert_eq!(cli.region, "us-east-1");
    assert_eq!(cli.retries, 3);
    assert!(!cli.no_progress);
}

#[test]
fn cli_parses_region_endpoint() {
    use clap::Parser;
    let args = Cli::try_parse_from([
        "s3-uploader",
        "-b",
        "bkt",
        "-k",
        "k",
        "-r",
        "auto",
        "-e",
        "https://example.r2.cloudflarestorage.com",
        "data.bin",
    ]);
    let cli = args.unwrap();
    assert_eq!(cli.region, "auto");
    assert_eq!(
        cli.endpoint,
        Some("https://example.r2.cloudflarestorage.com".to_string())
    );
}

#[test]
fn cli_parses_content_type_and_no_progress() {
    use clap::Parser;
    let args = Cli::try_parse_from([
        "s3-uploader",
        "-b",
        "bkt",
        "-k",
        "k",
        "-t",
        "text/plain",
        "--no-progress",
        "-p",
        "true",
        "file.txt",
    ]);
    let cli = args.unwrap();
    assert_eq!(cli.content_type, Some("text/plain".to_string()));
    assert!(cli.no_progress);
    assert_eq!(cli.force_path_style, Some(true));
}

#[test]
fn cli_parses_stdin_mode() {
    use clap::Parser;
    let args = Cli::try_parse_from(["s3-uploader", "-b", "bkt", "-k", "k"]);
    let cli = args.unwrap();
    assert!(cli.file.is_none());
}

#[test]
fn cli_parses_retries_override() {
    use clap::Parser;
    let args = Cli::try_parse_from([
        "s3-uploader",
        "-b",
        "bkt",
        "-k",
        "k",
        "--retries",
        "5",
        "file.txt",
    ]);
    let cli = args.unwrap();
    assert_eq!(cli.retries, 5);
}

// ═══════════════════════════════════════════════════════════
// Integration tests — require .env credentials & network
// Run with: cargo test -- --ignored
// ═══════════════════════════════════════════════════════════

mod s3_integration {
    use super::*;
    use std::io::{Cursor, Write};
    use std::path::Path;

    /// Load env vars from `.env` in the project root (tests run from crate root).
    fn load_dotenv() {
        let env_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
        if !env_path.exists() {
            eprintln!("  ⚠ .env not found — skipping integration setup");
            return;
        }
        let content = std::fs::read_to_string(&env_path).unwrap();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("####") {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();
                // Don't overwrite already-set env vars (CI etc.)
                if std::env::var(key).is_err() {
                    std::env::set_var(key, value);
                }
            }
        }
    }

    async fn build_client() -> (aws_sdk_s3::Client, String) {
        load_dotenv();

        let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET must be set in .env");
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();

        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region));
        if let Some(ref ep) = endpoint {
            config_builder = config_builder.endpoint_url(ep);
        }
        let sdk_config = config_builder.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(endpoint.is_some())
            .build();
        (aws_sdk_s3::Client::from_conf(s3_config), bucket)
    }

    /// Delete an uploaded test object so we don't leave trash in the bucket.
    async fn cleanup(client: &aws_sdk_s3::Client, bucket: &str, key: &str) {
        let _ = client.delete_object().bucket(bucket).key(key).send().await;
    }

    // ── helpers to build temp files ──

    fn temp_file(name: &str, size: usize) -> (PathBuf, std::fs::File) {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("s3-uploader-test-{name}"));
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(size as u64).unwrap();
        (path, file)
    }

    fn temp_file_with_content(name: &str, content: &[u8]) -> (PathBuf, std::fs::File) {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("s3-uploader-test-{name}"));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content).unwrap();
        file.sync_all().unwrap();
        (path, file)
    }

    fn drop_temp(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
    }

    // ── tests ──

    /// Upload a tiny file (one chunk) via the single-PUT path and verify it exists.
    #[ignore]
    #[tokio::test]
    async fn put_small_file_upload_verify_exists() {
        let (client, bucket) = build_client().await;
        let key = format!("_test/small-file-{}.bin", uuid_v4());

        let content = b"hello s3-uploader integration test!\n";
        let (path, _file) = temp_file_with_content("small.bin", content);
        let meta = std::fs::metadata(&path).unwrap();
        let expected_size = meta.len();
        assert!(
            expected_size <= PART_SIZE as u64,
            "test file must be <5 MiB"
        );

        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));

        // Use the single-PUT body mechanism
        let body = file_body(&path, counter.clone(), done.clone(), false);
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(body)
            .content_type("application/octet-stream")
            .content_length(expected_size as i64)
            .send()
            .await
            .expect("PUT should succeed");

        // Verify via HeadObject
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("head_object should succeed");
        assert_eq!(head.content_length(), Some(expected_size as i64));

        cleanup(&client, &bucket, &key).await;
        drop_temp(&path);
    }

    /// Upload a file larger than PART_SIZE → automatically goes through multipart streaming.
    #[ignore]
    #[tokio::test]
    async fn multipart_large_file_upload_verify() {
        let (client, bucket) = build_client().await;
        let key = format!("_test/large-file-{}.bin", uuid_v4());

        // Create a file just over PART_SIZE to trigger the multipart path
        let file_size = PART_SIZE + 1024 * 1024; // 6 MiB
        let (path, _file) = temp_file("large.bin", file_size);

        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let uploader = Uploader {
            counter: counter.clone(),
            done: done.clone(),
        };

        let file = std::fs::File::open(&path).unwrap();
        let total = upload_multipart_stream(
            &client,
            &bucket,
            &key,
            "application/octet-stream",
            file,
            &uploader,
            3,
            false,
        )
        .await
        .expect("multipart upload should succeed");

        assert_eq!(total, file_size as u64);

        // Verify
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("head_object should succeed");
        assert_eq!(head.content_length(), Some(file_size as i64));

        cleanup(&client, &bucket, &key).await;
        drop_temp(&path);
    }

    /// Upload from an in-memory cursor (simulating stdin) via the multipart streaming path.
    #[ignore]
    #[tokio::test]
    async fn stream_from_memory_upload_verify() {
        let (client, bucket) = build_client().await;
        let key = format!("_test/stream-{}.bin", uuid_v4());

        let data = vec![b'A'; PART_SIZE + CHUNK_SIZE]; // just over 1 part
        let reader = Cursor::new(data.clone());

        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let uploader = Uploader {
            counter: counter.clone(),
            done: done.clone(),
        };

        let total = upload_multipart_stream(
            &client,
            &bucket,
            &key,
            "application/octet-stream",
            reader,
            &uploader,
            3,
            false,
        )
        .await
        .expect("stream upload should succeed");

        assert_eq!(total, data.len() as u64);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("head_object should succeed");
        assert_eq!(head.content_length(), Some(data.len() as i64));

        cleanup(&client, &bucket, &key).await;
    }

    /// Verify that content_type is propagated through a multipart upload.
    #[ignore]
    #[tokio::test]
    async fn multipart_preserves_content_type() {
        let (client, bucket) = build_client().await;
        let key = format!("_test/photo-{}.jpg", uuid_v4());

        let data = vec![b'X'; PART_SIZE + 1];
        let reader = Cursor::new(data);

        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let uploader = Uploader {
            counter: counter.clone(),
            done: done.clone(),
        };

        upload_multipart_stream(
            &client,
            &bucket,
            &key,
            "image/jpeg",
            reader,
            &uploader,
            3,
            false,
        )
        .await
        .expect("multipart upload should succeed");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("head_object should succeed");
        assert_eq!(head.content_type(), Some("image/jpeg"));

        cleanup(&client, &bucket, &key).await;
    }

    /// Upload exactly PART_SIZE bytes → still triggers multipart (one part).
    #[ignore]
    #[tokio::test]
    async fn multipart_exact_one_part() {
        let (client, bucket) = build_client().await;
        let key = format!("_test/one-part-{}.bin", uuid_v4());

        let data = vec![b'Z'; PART_SIZE];
        let reader = Cursor::new(data.clone());

        let counter = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let uploader = Uploader {
            counter: counter.clone(),
            done: done.clone(),
        };

        let total = upload_multipart_stream(
            &client,
            &bucket,
            &key,
            "application/octet-stream",
            reader,
            &uploader,
            3,
            false,
        )
        .await
        .expect("single-part multipart upload should succeed");

        assert_eq!(total, PART_SIZE as u64);

        cleanup(&client, &bucket, &key).await;
    }

    // ── simple uuid v4 for test key uniqueness ──

    fn uuid_v4() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!(
            "{:08x}-{:04x}-{:04x}",
            (ts >> 32) as u32,
            (ts >> 16) as u16,
            ts as u16,
        )
    }
}
