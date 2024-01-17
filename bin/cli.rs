use anyhow::Result;
use clap::*;
use log::debug;
use std::collections::HashMap;
use tokio::{fs::File, io::AsyncReadExt};

#[derive(Parser, Debug)]
#[clap(name = "Root S3 cli", version = "0.1", about = "S3 cli")]
pub struct S3Cli {
    #[clap(subcommand)]
    command: SubCommand,

    #[clap(
        long,
        value_name = "URL",
        required = false,
        default_value = "http://localhost:9000",
        short = 'u'
    )]
    url: String,

    #[clap(long, short, required = false)]
    org_id: Option<i32>,

    #[clap(long, short, required = false)]
    project_id: Option<i32>,

    #[clap(long, required = false)]
    api_key: Option<String>,

    #[clap(long, required = false)]
    access_key: Option<String>,

    #[clap(long, short, required = false)]
    secret_key: Option<String>,
}

#[derive(Parser, Debug)] // requires `derive` feature
pub enum SubCommand {
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
    let args: S3Cli = S3Cli::parse();

    let client = get_client(&args).await.expect("Error creating client");

    match args.command {
        SubCommand::CreateBucket(CreateBucketArgs { name }) => {
            let res = client.create_bucket(&name, args.project_id).await;
            match res {
                Ok(_) => println!("Bucket created: {:?}", name),
                Err(e) => eprintln!("Error creating bucket: {:?}", e),
            }
        }
        SubCommand::DeleteBucket(DeleteBucketArgs { name }) => {
            let res = client.delete_bucket(&name, args.project_id).await;
            match res {
                Ok(_) => println!("Bucket deleted: {:?}", name),
                Err(e) => eprintln!("Error deleting bucket: {:?}", e),
            }
        }
        SubCommand::ListBuckets(ListBucketsArgs {}) => {
            let res = client.list_buckets(args.project_id).await.unwrap();

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
        SubCommand::PutObject(PutObjectArgs {
            bucket,
            key,
            file_path,
            metadata,
        }) => {
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
                .put_object(&bucket, &key, buffer.into(), args.project_id, metadata_map)
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
        SubCommand::GetObject(GetObjectArgs {
            bucket,
            key,
            output,
        }) => {
            let res = client.get_object(&bucket, &key, args.project_id).await;

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
        SubCommand::CopyObject(CopyObjectArgs {
            bucket,
            key,
            source_bucket,
            source_key,
        }) => {
            let res = client
                .copy_object(&bucket, &key, &source_bucket, &source_key, args.project_id)
                .await;

            match res {
                Ok(res) => {
                    println!("{:?}", res);
                    println!("Object copied: {:?} to bucket {:?}", key, bucket);
                }
                Err(e) => eprintln!("Error copying object: {:?}", e),
            }
        }
        SubCommand::DeleteObject(DeleteObjectArgs { bucket, key }) => {
            let res = client.delete_object(&bucket, &key, args.project_id).await;
            match res {
                Ok(_) => println!("Object with id '{}' deleted", key),
                Err(e) => eprintln!("Error deleting object: {:?}", e),
            }
        }
        SubCommand::ListObjects(ListObjectArgs { bucket }) => {
            let res = client.list_objects(&bucket, args.project_id).await.unwrap();

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
        SubCommand::GetHeadObject(GetHeadObject { bucket, key }) => {
            let res = client
                .head_object(&bucket, &key, args.project_id)
                .await
                .unwrap();

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

async fn get_client(args: &S3Cli) -> Result<root_s3::Client> {
    if let Some(api_key) = &args.api_key {
        Ok(root_s3::Client::new(
            args.url.clone(),
            api_key,
            args.org_id.unwrap_or(0),
        )?)
    } else {
        let cred = root_s3::S3Credentials {
            access_key_id: args.access_key.clone().unwrap(),
            secret_access_key: args.secret_key.clone().unwrap(),
            session_token: None,
            expiration: None,
            region: "eu".to_string(),
        };

        Ok(root_s3::Client::new_from_s3_credentials(
            args.url.clone(),
            cred,
        )?)
    }
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CreateBucketArgs {
    #[arg(long)]
    pub name: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct DeleteBucketArgs {
    #[arg(long)]
    pub name: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct ListBucketsArgs {}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct PutObjectArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub file_path: String,

    #[arg(long)]
    pub metadata: Option<String>,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct GetObjectArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub output: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CopyObjectArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub source_bucket: String,

    #[arg(long)]
    pub source_key: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct DeleteObjectArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct ListObjectArgs {
    #[arg(long)]
    pub bucket: String,
}

#[derive(clap::Args, Debug)]
#[command(author, version, about, long_about = None)]
pub struct GetHeadObject {
    #[arg(long)]
    pub bucket: String,

    #[arg(long)]
    pub key: String,
}
