#![allow(warnings)]
use aws_config::from_env;
use aws_sdk_s3 as s3;
use std::error::Error;
use aws_sdk_s3::config::{Builder, Credentials};
use aws_config::Region;

use std::{fs::File, io::Write, path::PathBuf, process::exit};
use std::path::Path;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Bucket, CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ReplicationStatus::Failed;
use clap::Parser;
use parquet::file::reader::Length;
use tracing::trace;





pub async fn create_directory(client: &Client, bucket: &str, directory: &str) -> Result<(), Box<dyn std::error::Error>> {
    let directory_key = format!("{}/", directory);

    let put_request = client.put_object()
        .bucket(bucket)
        .key(directory_key)
        .body(Vec::new().into());

    put_request.send().await?;

    Ok(())
}
pub async fn object_exists(client: &Client, bucket: &str, object: &str) -> Result<bool, Box<dyn std::error::Error>> {
    match client.head_object().bucket(bucket).key(object).send().await {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

pub async fn is_bucket_accessible(client: &Client, bucket_name: String) -> Result<bool, anyhow::Error>{
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => Ok(true),
        Err(e) => Err(e.into()),
    }
}

pub async fn get_bucket_list(client: &Client)-> Result<(Vec<String>), Box<dyn Error>> {
    let resp = client.list_buckets().send().await?;
    let mut res = Vec::new();
    for bucket in resp.buckets.unwrap_or_default() {
        res.push(bucket.name.unwrap_or_default());
    }
    Ok(res)
}

pub async fn initialize_bucket_directories(client: &Client) -> Result<(), Box<dyn Error>>{
    let temp = get_bucket_list(client).await?;
    if !temp.contains(&"mrl-lite".to_string()){
        client.create_bucket().bucket("mrl-lite").send().await?;
    }
    // if (object_exists(client, "mrl-lite", "/input/").await? ==  false){
    //     create_directory(client, "mrl-lite", "/input/").await?;
    // }
    // if (object_exists(client, "mrl-lite", "/output/").await? == false){
    //     create_directory(client, "mrl-lite", "/output/").await?;
    // }
    if (object_exists(client, "mrl-lite", "/temp/").await? == false){
        create_directory(client, "mrl-lite", "/temp/").await?;
    }
    Ok(())

}

pub async fn get_local_minio_client() -> aws_sdk_s3::Client {
    // Create credentials for local MinIO
    let credentials = Credentials::new(
        "ROOTNAME",
        "CHANGEME123",
        None,
        None,
        "minio",
    );

    // Create the S3 config
    let config = Builder::new()
        .region(Region::new("us-east-1"))
        .endpoint_url("http://[::1]:9000") // localhost
        .credentials_provider(credentials)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();

    // Create the S3 client
    let client = aws_sdk_s3::Client::from_conf(config);

    return client;
}

// USEFUL STUFF THAT YOU GUYS PROBABLY USE
pub async fn get_min_io_client(base_url: String, access_id: String, access_key: String) -> Result<Client, Box<dyn Error>> {
    // MinIO Server config
    // let base_url = "http://localhost:9000";
    // let access_key_id = "ROOTNAME";
    // let secret_access_key = "CHANGEME123";

    let region = Region::new("us-east-1");
    let credentials =
        Credentials::new(
            access_id,
            access_key,
            None,
            None,
            "minio");

    let config_loader = from_env()
        .region(region)
        .credentials_provider(credentials)
        .endpoint_url(base_url)
        .behavior_version(s3::config::BehaviorVersion::latest())
        .load().await;

    // Create an S3 client
    let s3_client = Client::new(&config_loader);
    Ok(s3_client)
}

pub async fn upload_string(client: &Client, bucket: &str, file_name: &str, content: &str) -> Result<(), Box<dyn std::error::Error> > {
    let put_request = client.put_object()
        .bucket(bucket)
        .key(file_name)
        .body(content.as_bytes().to_vec().into());
    put_request.send().await?;
    Ok(())
}

// Get object as String for now for test purposes
pub async fn get_object(client: &Client, bucket: &str, object: &str) -> Result<String, anyhow::Error> {
    //Bucket is the name of the bucket, object is the name of the object
    trace!("bucket:      {}", bucket);
    trace!("object:      {}", object);
    // trace!("destination: {}", opt.destination.display());
    let mut object = client
        .get_object()
        .bucket(bucket)
        .key(object)
        .send()
        .await?;

    let mut content = Vec::new();

    while let Some(bytes) = object.body.try_next().await? {
        content.extend_from_slice(&bytes);
    }
    let content_str = String::from_utf8(content)?;
    Ok(content_str)
}

//If wanna use this in main just
// let bucket_name = "rust-s3";
// let object_name = "/input/text2.txt";
// match minio::get_object(s3_client, bucket_name, object_name).await {
// Ok(content) => println!("{:?}", content),
// Err(e) => eprintln!("Failed to get object: {:?}", e),
// }
pub async fn delete_object(client: &Client, bucket: &str, object: &str) -> Result<(), Box<dyn std::error::Error>> {
    let delete_request = client.delete_object()
        .bucket(bucket)
        .key(object);
    delete_request.send().await?;
    Ok(())
}

//havent check yet
pub async fn list_files_with_prefix(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut objects = Vec::new();
    let resp = client.list_objects_v2().bucket(bucket).prefix(prefix).send().await?;
    for object in resp.contents.unwrap_or_default() {
        objects.push(object.key.unwrap_or_default());
    }
    Ok(objects)
}

//filename has to be the same name as upload to s3
pub async fn upload_parts(client: &Client, bucket: &str, filename: &str)-> Result<(), Box<dyn std::error::Error>> {
    //Split into 5MB chunks
    const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
    const MAX_CHUNKS: u64 = 10000;

    let destination_filename = filename;
    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(destination_filename)
        .send()
        .await
        .unwrap();

    println!("Created multipart upload with ID: {}", multipart_upload_res.upload_id.as_ref().unwrap());


    let upload_id = multipart_upload_res.upload_id.unwrap();

    let path = Path::new(filename);
    let file_size = tokio::fs::metadata(path)
        .await
        .expect("it exists I swear")
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        panic!("Bad file size.");
    }
    if chunk_count > MAX_CHUNKS {
        panic!("Too many chunks! Try increasing your chunk size.")
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();

    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };
        let stream = ByteStream::read_from()
            .path(path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(aws_smithy_types::byte_stream::Length::Exact(this_chunk))
            .build()
            .await
            .unwrap();
        //Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;
        // snippet-start:[rust.example_code.s3.upload_part]
        let upload_part_res = client
            .upload_part()
            .key(filename)
            .bucket(bucket)
            .upload_id(upload_id.clone())
            .body(stream)
            .part_number(part_number)
            .send()
            .await;



        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.unwrap().e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
        // snippet-end:[rust.example_code.s3.upload_part]
    }


    // snippet-start:[rust.example_code.s3.upload_part.CompletedMultipartUpload]
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();
    // snippet-end:[rust.example_code.s3.upload_part.CompletedMultipartUpload]

    // snippet-start:[rust.example_code.s3.complete_multipart_upload]
    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(filename)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id.clone())
        .send()
        .await
        .unwrap();
    // snippet-end:[rust.example_code.s3.complete_multipart_upload]

    Ok(())
}





// Example of listing file buckets
//
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     let s3_client = get_min_io_client("http://localhost:9000".to_string()).await?;
//     // List all buckets
//     let resp = s3_client.list_buckets().send().await?;
//     println!("Buckets:");
//     for bucket in resp.buckets.unwrap_or_default() {
//         println!("{}", bucket.name.unwrap_or_default());
//     }
//     Ok(())
// }