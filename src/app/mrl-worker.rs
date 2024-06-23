use anyhow::Ok;
// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::worker::Args;
use tonic::Request;
use std::sync::{Arc, Mutex};

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

    let request: Request<WorkerRegistration> = Request::new(WorkerRegistration { address: format!("") });
    let response = client.register_worker(request).await.unwrap();
    println!("{:?}", response)
}
