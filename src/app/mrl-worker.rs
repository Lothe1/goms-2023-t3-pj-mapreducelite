use anyhow::Ok;
// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::worker::Args;
use tokio::time::sleep;
use tonic::{Request, Response};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use S3::minio;

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::{WorkerRegistration, WorkerResponse, WorkerRequest, Task, JobRequest, JobResponse, Empty, Status as SystemStatus, Worker};
use mapreduce::coordinator_client::CoordinatorClient;

#[tokio::main]
async fn main() {
    print!("Hello worker!\n");
    let args = Args::parse();
    let ip = args.join;
    print!("IP to join: {}", ip);
    let mut client = CoordinatorClient::connect(format!("http://{}", ip)).await.unwrap();

    let join_resp = client.register_worker(Request::new(WorkerRegistration { address: format!("") })).await.unwrap();
    // println!("{:?}", join_resp);
    let s3_client = minio::get_min_io_client(join_resp.into_inner().message).await.unwrap();

    loop {
        let resp = client.get_task(Request::new(WorkerRequest {  })).await;
        if resp.is_err() {
            sleep(Duration::from_secs(1)).await;
        }
    }
}
