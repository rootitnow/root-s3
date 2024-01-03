use std::collections::HashMap;

use anyhow::Result;
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::{
    operation::{
        copy_object::{CopyObjectError, CopyObjectOutput},
        create_bucket::{CreateBucketError, CreateBucketOutput},
        delete_bucket::{DeleteBucketError, DeleteBucketOutput},
        delete_object::{DeleteObjectError, DeleteObjectOutput},
        get_object::{GetObjectError, GetObjectOutput},
        head_object::{HeadObjectError, HeadObjectOutput},
        list_buckets::{ListBucketsError, ListBucketsOutput},
        list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output},
        put_object::{PutObjectError, PutObjectOutput},
    },
    types::BucketLocationConstraint,
    types::CreateBucketConfiguration,
    Client,
};
use aws_smithy_runtime_api::http::Request;
use aws_types::{region::Region, sdk_config::SdkConfig};
use thiserror::Error;

/// RootS3Client struct represents a client for interacting with the S3 service of root.
#[derive(Debug, Clone)]
pub struct RootS3Client {
    /// API key for authentication.
    pub api_key: String,
    pub org_id: i32,
    /// S3 client from AWS SDK.
    pub s3_client: Client,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid url")]
    InvalidUrl,
    #[error("Failed to create bucket: {0}")]
    ErrCreateBucket(Box<CreateBucketError>),
    #[error("Failed to delete bucket: {0}")]
    ErrDeleteBucket(Box<DeleteBucketError>),
    #[error("Failed to list buckets: {0}")]
    ErrListBuckets(Box<ListBucketsError>),
    #[error("Failed to put object: {0}")]
    ErrPutObject(Box<PutObjectError>),
    #[error("Failed to copy object: {0}")]
    ErrCopyObject(Box<CopyObjectError>),
    #[error("Failed to get object: {0}")]
    ErrGetObject(Box<GetObjectError>),
    #[error("Failed to get head object: {0}")]
    ErrGetHeadObject(Box<HeadObjectError>),
    #[error("Failed to delete object: {0}")]
    ErrDeleteObject(Box<DeleteObjectError>),
    #[error("Failed to list objects: {0}")]
    ErrListObjects(Box<ListObjectsV2Error>),
}

impl<'a> RootS3Client {
    /// Creates a new `RootS3Client`.
    ///
    /// # Arguments
    ///
    /// * `url` - The base URL for the S3 service.
    /// * `api_key` - The API key for authentication.
    /// * `project_id` - The project ID for identifying the project.
    ///
    /// # Returns
    ///
    /// A Result containing the initialized `RootS3Client` or an `Error` if the URL is invalid.
    pub fn new(url: impl Into<&'a str>, api_key: String, org_id: i32) -> Result<Self, Error> {
        let s3_client = get_s3_client(url.into()).map_err(|_| Error::InvalidUrl)?;
        Ok(Self {
            api_key,
            s3_client,
            org_id,
        })
    }
}

pub fn get_s3_client(url: &str) -> Result<Client> {
    let cred = Credentials::new("", "", None, None, "");
    let scred = SharedCredentialsProvider::new(cred);

    let client = Client::new(
        &SdkConfig::builder()
            .endpoint_url(url)
            .region(Region::new("eu-central-1"))
            .credentials_provider(scred)
            .build(),
    );

    Ok(client)
}

impl RootS3Client {
    pub async fn create_bucket(
        &self,
        bucket: &str,
        project_id: i32,
    ) -> Result<CreateBucketOutput, Error> {
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from("eu-central-2"))
            .build();

        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrCreateBucket(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn delete_bucket(
        &self,
        bucket: &str,
        project_id: i32,
    ) -> Result<DeleteBucketOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrDeleteBucket(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn list_buckets(&self, project_id: i32) -> Result<ListBucketsOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .list_buckets()
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrListBuckets(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        project_id: i32,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .put_object()
            .key(key)
            .body(data.into())
            .bucket(bucket)
            .set_metadata(metadata)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrPutObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        target_bucket: &str,
        target_key: &str,
        project_id: i32,
    ) -> Result<CopyObjectOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .copy_object()
            .key(key)
            .copy_source(format!("{}/{}", target_bucket, target_key))
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrCopyObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: i32,
    ) -> Result<GetObjectOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .get_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrGetObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: i32,
    ) -> Result<DeleteObjectOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .delete_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrDeleteObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        project_id: i32,
    ) -> Result<ListObjectsV2Output, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .list_objects_v2()
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrListObjects(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: i32,
    ) -> Result<HeadObjectOutput, Error> {
        let api_key = self.api_key.clone();
        let org_id = self.org_id.clone();

        let res = self
            .s3_client
            .head_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, api_key.clone(), project_id, org_id))
            .send()
            .await
            .map_err(|e| Error::ErrGetHeadObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }
}

// Add the api key to the headers and the project id to the query
fn add_root_auth(req: &mut Request, api_key: String, org_id: i32, project_id: i32) {
    // Add the api key to the headers
    req.headers_mut().append("x-api-key", api_key);

    // Add the project id to the query
    let mut path = req.uri().to_owned();
    log::debug!("path: {}", path);
    // let (path, query) = path.split_once("/").unwrap();
    // log::debug!("path: {}", path);
    // log::debug!("query: {}", query);
    // let mut path = path.to_owned();
    path += &format!("api/v1/organisations/{org_id}/projects/{project_id}/s3");
    log::debug!("path: {}", path);
    // Set the new uri
    let _ = req.set_uri(path);
    log::debug!("req: {:?}", req);
}
