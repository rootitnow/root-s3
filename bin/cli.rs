use bytes::BytesMut;
use clap::*;
use log::debug;
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
    DeleteObject(DeleteObjectArgs),
    ListObjects(ListObjectArgs),
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
            let _ = client.create_bucket(&name, project).await.unwrap();
            println!("Bucket created: {:?}", name);
        }
        S3Cli::DeleteBucket(DeleteBucketArgs { url, name, project }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let _ = client.delete_bucket(&name, project).await.unwrap();
            println!("Bucket deleted: {:?}", name);
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
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();

            let mut file = File::open(file_path).await?;
            let mut buffer = BytesMut::new();
            // read the whole file
            file.read_buf(&mut buffer).await?;

            log::debug!("buffer size: {}", buffer.len());

            let res = client
                .put_object(&bucket.clone(), &key, buffer.freeze(), project)
                .await
                .unwrap();

            println!(
                "Object with id '{}' created in bucket {}",
                res.e_tag.unwrap(),
                bucket
            );
        }
        S3Cli::GetObject(GetObjectArgs {
            url,
            bucket,
            key,
            output,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();
            let res = client
                .get_object(&bucket, &key.clone(), project)
                .await
                .unwrap();

            // Write content to output file
            let mut body = res.body.into_async_read();
            let mut file = File::create(&output).await?;
            tokio::io::copy(&mut body, &mut file).await?;

            println!(
                "Object with id '{}' downloaded to {}, size: {} bytes",
                key, output, res.content_length
            );
        }
        S3Cli::DeleteObject(DeleteObjectArgs {
            url,
            bucket,
            key,
            project,
        }) => {
            let client = RootS3Client::new(url.as_ref(), api_key).unwrap();

            let _ = client.delete_object(&bucket, &key, project).await.unwrap();

            println!("Object with id '{}' deleted", key);
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
                        c.size,
                    );
                }
                println!("\n");
            } else {
                println!("No objects in bucket '{}'", bucket);
            }
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
