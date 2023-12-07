use clap::*;
use log::debug;
use std::collections::HashMap;
use tokio::{fs::File, io::AsyncReadExt};

use root_s3::RootS3Client;

#[derive(Parser)] // requires `derive` feature
#[command(name = "cargo")]
#[command(bin_name = "cargo")]
pub enum S3Cli {
    // Buckets
    CreateBucket(CreateBucketArgs),
    DeleteBucket(DeleteBucketArgs),
    ListBuckets(ListBucketsArgs),
    // Objects
    PutObject(PutObjectArgs),
    GetObject(GetObjectArgs),
    CopyObject(CopyObjectArgs),
    DeleteObject(DeleteObjectArgs),
    ListObjects(ListObjectArgs),
    GetHeadObject(GetHeadObject),
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    debug!("cli started");

    let api_key = std::env::var("API_KEY").expect("API_KEY not set");
    log::debug!("api key: {}", api_key);

    match S3Cli::parse() {
        S3Cli::CreateBucket(CreateBucketArgs { url, name, project }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.create_bucket(&name, project).await;
            match res {
                Ok(_) => println!("Bucket created: {:?}", name),
                Err(e) => eprintln!("Error creating bucket: {:?}", e),
            }
        }
        S3Cli::DeleteBucket(DeleteBucketArgs { url, name, project }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.delete_bucket(&name, project).await;
            match res {
                Ok(_) => println!("Bucket deleted: {:?}", name),
                Err(e) => eprintln!("Error deleting bucket: {:?}", e),
            }
        }
        S3Cli::ListBuckets(ListBucketsArgs { url, project }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.list_buckets(project).await.unwrap();

            debug!("result {:?}", res);

            if let Some(buckets) = res.buckets {
                println!("\nBuckets:\n");
                for bucket in buckets {
                    println!(
                        "- Bucket:\n\tname: {:?}\n\tcreated at: {:?}",
                        bucket.name.unwrap(),
                        bucket.creation_date.unwrap().secs()
                    );
                }
                println!("\n");
            } else {
                println!("No buckets");
            }
        }
        S3Cli::PutObject(PutObjectArgs {
            url,
            bucket,
            key,
            file_path,
            project,
            metadata,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();

            let mut file = File::open(file_path).await?;

            // Create a buffer to store the file contents
            let mut buffer = Vec::new();

            let metadata_map = if let Some(metadata) = metadata {
                let mut map = HashMap::new();
                metadata.split(',').for_each(|m| {
                    let mut split = m.split('=');
                    let key = split.next().unwrap();
                    let value = split.next().unwrap();
                    map.insert(key.to_string(), value.to_string());
                });
                Some(map)
            } else {
                None
            };

            // Read the entire file into the buffer
            file.read_to_end(&mut buffer).await?;
            log::debug!("buffer size: {}", buffer.len());

            let res = client
                .put_object(&bucket, &key, buffer.into(), project, metadata_map)
                .await;

            match res {
                Ok(r) => println!(
                    "Object created: {:?} in bucket {:?}",
                    r.e_tag.unwrap(),
                    bucket
                ),
                Err(e) => eprintln!("Error creating object: {:?}", e),
            }
        }
        S3Cli::GetObject(GetObjectArgs {
            url,
            bucket,
            key,
            output,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.get_object(&bucket, &key, project).await;

            match res {
                Ok(res) => {
                    // Write content to output file
                    let mut body = res.body.into_async_read();
                    let mut file = File::create(&output).await?;
                    tokio::io::copy(&mut body, &mut file).await?;

                    println!(
                        "Object with id '{}' downloaded to {}, size: {} bytes",
                        key,
                        output,
                        res.content_length.unwrap()
                    );
                }
                Err(e) => eprintln!("Error getting object: {:?}", e),
            }
        }
        S3Cli::CopyObject(CopyObjectArgs {
            url,
            bucket,
            key,
            source_bucket,
            source_key,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client
                .copy_object(&bucket, &key, &source_bucket, &source_key, project)
                .await;

            match res {
                Ok(res) => {
                    println!("{:?}", res);
                    println!("Object copied: {:?} to bucket {:?}", key, bucket);
                }
                Err(e) => eprintln!("Error copying object: {:?}", e),
            }
        }
        S3Cli::DeleteObject(DeleteObjectArgs {
            url,
            bucket,
            key,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();

            let res = client.delete_object(&bucket, &key, project).await;
            match res {
                Ok(_) => println!("Object with id '{}' deleted", key),
                Err(e) => eprintln!("Error deleting object: {:?}", e),
            }
        }
        S3Cli::ListObjects(ListObjectArgs {
            url,
            bucket,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.list_objects(&bucket, project).await.unwrap();

            if let Some(contents) = res.contents {
                println!("Objects in bucket '{}'\n", bucket);
                for c in contents {
                    println!(
                        "- Object:\n\tkey: {:?}\n\tupdated at: {:?}\n\tsize: {} bytes",
                        c.key.unwrap(),
                        c.last_modified.unwrap().secs(),
                        c.size.unwrap(),
                    );
                }
                println!("\n");
            } else {
                println!("No objects in bucket '{}'", bucket);
            }
        }
        S3Cli::GetHeadObject(GetHeadObject {
            url,
            bucket,
            key,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client.head_object(&bucket, &key, project).await.unwrap();

            println!("Object with id '{}' in bucket '{}'\n", key, bucket);
            if let Some(meta) = res.metadata {
                println!("Metadata:");
                for (k, v) in meta {
                    println!("\t{}: {}", k, v);
                }
            }
            println!(
                "- Object:\n\tkey: {:?}\n\tupdated at: {:?}\n\tsize: {} bytes",
                key,
                res.last_modified,
                res.content_length.unwrap_or_default(),
            );
            println!("\n");
        }
    }

    Ok(())
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct CreateBucketArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct DeleteBucketArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct ListBucketsArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct PutObjectArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub file_path: String,

    #[arg(long)]
    pub metadata: Option<String>,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct GetObjectArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub output: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct CopyObjectArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub source_bucket: String,

    #[arg(long)]
    pub source_key: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct DeleteObjectArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct ListObjectArgs {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub project: i32,
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct GetHeadObject {
    #[arg(long, short)]
    pub url: String,

    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub project: i32,
}
