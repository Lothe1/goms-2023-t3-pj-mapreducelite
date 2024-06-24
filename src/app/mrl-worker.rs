// use anyhow::*;
// use bytes::Bytes;
#![ allow(warnings)]
use mrlite::*;
use clap::Parser;
use cmd::worker::Args;
use tokio::time::sleep;
use tonic::{Request, Response};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use aws_sdk_s3::Client;
use S3::minio;

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::{WorkerRegistration, WorkerRequest, Task, JobRequest};
use mapreduce::coordinator_client::CoordinatorClient;

#[tokio::main]
async fn main() {
    print!("Hello worker!\n");
    let args = Args::parse();
    let ip = args.join;
    print!("IP to join: {}", ip);
    let mut client = CoordinatorClient::connect(format!("http://{}", ip)).await.unwrap();

    let join_resp = client.register_worker(Request::new(WorkerRegistration {})).await.unwrap();
    println!("{:?}", join_resp);
    let s3_args = join_resp.into_inner();
    let s3_ip = format!("http://{}", s3_args.args.get("ip").unwrap().clone());
    let s3_user = s3_args.args.get("user").unwrap().clone();
    let s3_pw = s3_args.args.get("pw").unwrap().clone();
    // println!("s3: {} {} {}\n", s3_ip, s3_user, s3_pw);
    let s3_client = minio::get_min_io_client(s3_ip, s3_user, s3_pw).await.unwrap();
    let resp = s3_client.list_buckets().send().await.unwrap();

    // println!("bucket accessible: {}", minio::is_bucket_accessible(s3_client.clone(), bucket_name.to_string()).await.unwrap());

    let bucket_name = "rust-s3";
    let object_name = "/input/text2.txt";
    match minio::get_object(s3_client, bucket_name, object_name).await {
        Ok(content) => println!("{:?}", content),
        Err(e) => eprintln!("Failed to get object: {:?}", e),
    }

    loop {
        let resp = client.get_task(Request::new(WorkerRequest {  })).await;
        if resp.is_err() {
            sleep(Duration::from_secs(1)).await;
        }
    }
}

