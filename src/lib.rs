use anyhow::Result;
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::{
    error::ErrorMetadata,
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
};
use aws_smithy_runtime_api::http::Request;
use aws_types::{region::Region, sdk_config::SdkConfig};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::Semaphore;
pub const MAX_CONCURRENT: usize = 20;

/// `RootS3Client` struct represents a client for interacting with the S3 service of root.
#[derive(Debug, Clone)]
pub struct Client {
    /// S3 client from AWS SDK.
    pub s3_client: aws_sdk_s3::Client,

    /// Optional root config.
    pub config: Option<RootConfig>,

    /// Limit concurrent requests to S3.
    pub semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct RootConfig {
    pub api_key: String,
    pub org_id: i32,
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
    #[error("Failed to acquire semaphore: {0}")]
    SemaphoreError(#[from] tokio::sync::AcquireError),
}

pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expiration: Option<String>,
    pub region: String,
}

impl Client {
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
    pub fn new(
        url: impl Into<String> + Clone,
        api_key: impl Into<String>,
        org_id: i32,
    ) -> Result<Self, Error> {
        let s3_client = get_s3_client(&url.into(), None).map_err(|_| Error::InvalidUrl)?;

        Ok(Self {
            config: Some(RootConfig {
                api_key: api_key.into(),
                org_id,
            }),
            s3_client,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        })
    }

    pub fn new_from_s3_credentials(
        url: impl Into<String> + Clone,
        credentials: S3Credentials,
    ) -> Result<Self, Error> {
        let s3_client =
            get_s3_client(&url.into(), Some(credentials)).map_err(|_| Error::InvalidUrl)?;

        Ok(Self {
            config: None,
            s3_client,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        })
    }
}

pub fn get_s3_client(url: &str, credentials: Option<S3Credentials>) -> Result<aws_sdk_s3::Client> {
    let cred = match credentials {
        Some(cred) => Credentials::new(cred.access_key_id, cred.secret_access_key, None, None, ""),
        None => Credentials::new("", "", None, None, ""),
    };

    let client = aws_sdk_s3::Client::new(
        &SdkConfig::builder()
            .endpoint_url(url)
            .region(Region::new("weur"))
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .build(),
    );

    Ok(client)
}

impl Client {
    pub async fn create_bucket(
        &self,
        bucket: &str,
        project_id: Option<i32>,
    ) -> Result<CreateBucketOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .create_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrCreateBucket(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn delete_bucket(
        &self,
        bucket: &str,
        project_id: Option<i32>,
    ) -> Result<DeleteBucketOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrDeleteBucket(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn list_buckets(&self, project_id: Option<i32>) -> Result<ListBucketsOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .list_buckets()
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
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
        project_id: Option<i32>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .put_object()
            .key(key)
            .body(data.into())
            .bucket(bucket)
            .set_metadata(metadata)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
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
        project_id: Option<i32>,
    ) -> Result<CopyObjectOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .copy_object()
            .key(target_key)
            .copy_source(format!("{bucket}/{key}"))
            .bucket(target_bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrCopyObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: Option<i32>,
    ) -> Result<GetObjectOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .get_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrGetObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: Option<i32>,
    ) -> Result<DeleteObjectOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .delete_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrDeleteObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        project_id: Option<i32>,
    ) -> Result<ListObjectsV2Output, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrListObjects(Box::new(e.into_service_error())))?;

        Ok(res)
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        project_id: Option<i32>,
    ) -> Result<HeadObjectOutput, Error> {
        let config = self.config.clone();

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError(e))?;

        let res = self
            .s3_client
            .head_object()
            .key(key)
            .bucket(bucket)
            .customize()
            .mutate_request(move |req| add_root_auth(req, &config, project_id))
            .send()
            .await
            .map_err(|e| Error::ErrGetHeadObject(Box::new(e.into_service_error())))?;

        Ok(res)
    }
}

// Add the api key to the headers and the project id to the query
// Only do this if an api key is set
fn add_root_auth(req: &mut Request, config: &Option<RootConfig>, project_id: Option<i32>) {
    if config.is_none() {
        return;
    };

    if project_id.is_none() {
        return;
    };

    let config = config.clone().unwrap();

    // Add the api key to the headers
    req.headers_mut().append("x-api-key", config.api_key);

    let req_uri = req.uri().to_string();
    log::debug!("uri: {:?}", req_uri);
    let parts = req_uri.split('?').collect::<Vec<_>>();
    // From splitted req uri, get the base url
    let base_url = parts[0].to_owned();
    let (url, _) = base_url.rsplit_once('/').unwrap();
    log::debug!("url: {:?}", url);

    let uri_mut = req.uri_mut();
    let original_path = uri_mut.path().to_owned();
    log::debug!("uri_mut path: {:?}", uri_mut.path());

    // Construct the path
    let mut path = format!(
        "/api/v1/organisations/{}/projects/{}/s3",
        config.org_id,
        project_id.unwrap()
    );

    // If the original path contains more than just a slash, add it to the path
    if original_path != *"/" {
        path += &original_path;
    }

    // Construct the new uri with the path and original url (url can contain bucketname)
    let mut new_uri = format!("{url}{path}");

    // Put back query if there was one
    if let Some(query) = req.uri_mut().query() {
        new_uri += &format!("?{query}");
    }

    let _ = req.set_uri(new_uri);

    log::debug!("req: {:?}", req);
}
