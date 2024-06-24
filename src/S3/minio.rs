#![allow(warnings)]
use aws_config::from_env;
use aws_sdk_s3 as s3;
use std::error::Error;
use aws_sdk_s3::config::Credentials;
use aws_config::Region;

use std::{fs::File, io::Write, path::PathBuf, process::exit};

use aws_sdk_s3::Client;
use aws_sdk_s3::types::Bucket;
use aws_sdk_s3::types::ReplicationStatus::Failed;
use clap::Parser;
use tracing::trace;




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
            "loaded-from-custom-env");

    let config_loader = from_env()
        .region(region)
        .credentials_provider(credentials)
        .endpoint_url("http://127.0.0.1:9000")
        .behavior_version(s3::config::BehaviorVersion::latest())
        .load().await;


    // Create an S3 client
    let s3_client = Client::new(&config_loader);
    Ok(s3_client)
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
    if (object_exists(client, "mrl-lite", "/input/").await? ==  false){
        create_directory(client, "mrl-lite", "/input/").await?;
    }
    if (object_exists(client, "mrl-lite", "/output/").await? == false){
        create_directory(client, "mrl-lite", "/output/").await?;
    }
    if (object_exists(client, "mrl-lite", "/temp/").await? == false){
        create_directory(client, "mrl-lite", "/temp/").await?;
    }
    Ok(())

}

pub async fn upload_string(client: &Client, bucket: &str, file_name: &str, content: &str) -> Result<(), Box<dyn std::error::Error> > {
    let put_request = client.put_object()
        .bucket(bucket)
        .key(file_name)
        .body(content.as_bytes().to_vec().into());
    put_request.send().await?;
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