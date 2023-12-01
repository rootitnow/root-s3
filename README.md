# S3 Cli

![build status](https://github.com/rootitnow/root-s3/actions/workflows/main.yaml/badge.svg)

## Requirements

- Rust
- Running Root instance
- API Key for the root instance
- Project ID on the root instance

See [docs](../backend/readme.md) for more information on how to run the backend and how to get an API key.

## Implemented methods

- Put Bucket
- Delete bucket
- List buckets
- Put object
- Get object
- Delete object
- List objects in bucket

## Usage

### Library

```rust
use root_s3::RootS3Client;

let project_id = 1;
let client = RootS3Client::new("http:://localhost:9000", String::from("api_key"), project_id).expect("client created");
let _ = client.create_bucket(name.clone()).await.unwrap();
println!("Bucket created: {:?}", name);
```

### CLI

```rust
cargo build
../target/debug/s3-cli --help
```

Make sure to export the API key before running the commands: `export API_KEY="some_api_key"`

## Examples

### Create a bucket

```bash
cargo run --bin s3-cli create-bucket --name testbucket --project 1 --url http://localhost:9000
```

### Delete bucket

```bash
cargo run --bin s3-cli delete-bucket --name testbucket --project 1 --url http://localhost:9000
```

### Listing buckets

```bash
cargo run --bin s3-cli list-buckets --project 1 --url http://localhost:9000
```

### Put object

```bash
cargo run --bin s3-cli put-object --bucket testbucket --key a1 --file-path ./Cargo.toml --project 1 --url http://localhost:9000
```

### Get object

```bash
cargo run --bin s3-cli get-object --bucket testbucket --key a1 --output Cargo.toml.download --project 1 --url http://localhost:9000
```

### Delete object

```bash
cargo run --bin s3-cli delete-object --bucket testbucket --key a1 --project 1 --url http://localhost:9000
```

### Listing objects

```bash
cargo run --bin s3-cli list-objects --project 1 --bucket "testbucket" --url http://localhost:9000
```

### Getting the head for an object

```bash
cargo run --bin s3-cli head-object --project 1 --bucket "testbucket" --key "a1" --url http://localhost:9000
```
