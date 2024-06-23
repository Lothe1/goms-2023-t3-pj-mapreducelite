use aws_config::from_env;
use aws_sdk_s3 as s3;
use s3::Client;
use std::error::Error;
use aws_sdk_s3::config::Credentials;
use aws_config::Region;

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
        .endpoint_url(base_url)
        .load().await;


    // Create an S3 client
    let s3_client = Client::new(&config_loader);
    Ok(s3_client)
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