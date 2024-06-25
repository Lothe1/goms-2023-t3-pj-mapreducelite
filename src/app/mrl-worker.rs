// use anyhow::*;
#![ allow(warnings)]
use itertools::Itertools;
use mrlite::*;
use bytes::Bytes;
use clap::Parser;
use cmd::worker::Args;
use tokio::time::sleep;
use tonic::{Request, Response};
use std::sync::{Arc, Mutex};
use std::time::Duration;
// use std::io::{BufReader, Read};
// use aws_sdk_s3::config::{Credentials, Config, Region, endpoint};
use aws_sdk_s3::Client;
use S3::minio;
use standalone::Job;

// #[derive(Parser)]
// struct WorkerArgs {
//     #[clap(long)]
//     join: String,
//     #[clap(long)]
//     s3_endpoint: String,
//     #[clap(long)]
//     s3_bucket: String,
//     #[clap(long)]
//     s3_access_key: String,
//     #[clap(long)]
//     s3_secret_key: String,
// }

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::{WorkerRegistration, WorkerRequest, Task, JobRequest};
use mapreduce::coordinator_client::CoordinatorClient;

async fn map(
    client: &Client,
    job: &Job
) {
    let engine: Workload = workload::try_named(&job.workload.clone()).expect("Error");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);
    match minio::get_object(&client, bucket_name, object_name).await {
        Ok(content) => println!("{:?}", object_name),
        Err(e) => eprintln!("Failed to get object: {:?}", e),
    }
}


// pub fn perform_map(
//     job: &Job,
//     engine: &Workload,
//     serialized_args: &Bytes,
//     num_reduce_worker: u32,
// ) -> Result<Buckets> {
//     // Iterator going through all files in the input file path, precisely, input/*
//     let input_files = glob(&job.input)?;
//     let buckets: Buckets = Buckets::new();
//     for pathspec in input_files.flatten() {
//         let mut buf = Vec::new();
//         {
//             // a scope so that the file is closed right after reading
//             let mut file = File::open(&pathspec)?;
//             // Reads the input file completely and stores in buf
//             file.read_to_end(&mut buf)?;
//         }
//         // Converts to Bytes
//         let buf = Bytes::from(buf);
//         let filename = pathspec.to_str().unwrap_or("unknown").to_string();
//         // Stores the data read from each file as <Filename, All data in file>
//         let input_kv = KeyValue {
//             key: Bytes::from(filename),
//             value: buf,
//         };
//         let map_func = engine.map_fn;
//         // For each <key, value> object that has been mapped by the map function,
//         // create a KeyValue object, and insert the KeyValue object into a bucket
//         // according to the hashed value (mod # workers)
//         for item in map_func(input_kv, serialized_args.clone())? {
//             let KeyValue { key, value } = item?;
//             let bucket_no = ihash(&key) % num_reduce_worker;

//             #[allow(clippy::unwrap_or_default)]
//             buckets
//                 .entry(bucket_no)
//                 .or_insert(Vec::new())
//                 .push(KeyValue { key, value });
//         }
//     }

//     Ok(buckets)
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    print!("Hello worker!\n");

    let args = Args::parse();
    let ip = args.join; 
    print!("IP to join: {}", ip);
    let mut client = CoordinatorClient::connect(format!("http://{}", ip)).await.unwrap();

    // Register with coordinator
    let request = Request::new(WorkerRegistration { });
    let response = client.register_worker(request).await?;
    println!("Worker registered: {:?} \n", response);

    // Get S3 args
    let s3_args = response.into_inner();
    let s3_ip = format!("http://{}", s3_args.args.get("ip").unwrap().clone());
    let s3_user = s3_args.args.get("user").unwrap().clone();
    let s3_pw = s3_args.args.get("pw").unwrap().clone();
    println!("s3: {} {} {}\n", s3_ip, s3_user, s3_pw);

    // Initialize S3 client
    let s3_client = minio::get_min_io_client(s3_ip.clone(), s3_user.clone(), s3_pw.clone()).await?;  

    // Listen for tasks
    loop {
        let resp = client.get_task(Request::new(WorkerRequest {  })).await;
        match resp {
            Ok(t) => {
                let task = t.into_inner();
                let job = Job {
                    input: task.input.clone(),
                    workload: task.workload.clone(),
                    output: task.output.clone(),
                    args: Vec::new()
                };
                sleep(Duration::from_secs(1)).await;

                map(&s3_client, &job).await;
            }
            Err(e) => {
                // no task
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/* 
S3 Client examples


    // Lists the buckets 
    let resp = match s3_client.list_buckets().send().await {
        Ok(resp) =>  println!("{:?}", resp),
        Err(e) => eprintln!("Failed to list buckets: {:?}", e),
    };  
    // println!("bucket accessible: {}", minio::is_bucket_accessible(s3_client.clone(), bucket_name.to_string()).await.unwrap());

    // Get object
    let bucket_name = "mrl-lite";
    let object_name = "/testcases/graph-edges/00.txt";
    match minio::get_object(&s3_client, bucket_name, object_name).await {
        Ok(content) => println!("{:?}", content),
        Err(e) => eprintln!("Failed to get object: {:?}", e),
    }

    match minio::list_files_with_prefix(&s3_client, bucket_name, "testcases").await {
        Ok(content) => println!("{:?}", content),
        Err(e) => eprintln!("Failed!!! {:?}", e),
    }

    // //Write object
    match minio::upload_string(&s3_client, bucket_name, "write_test.txt", "Mother wouldst Thou truly Lordship sanction\n in one so bereft of light?").await {
        Ok(_) => println!("Uploaded"),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }

    //Delete object s3 doesnt return error for some reason lol
    match minio::delete_object(&s3_client, bucket_name, "write_test.txt").await {
        Ok(_) => println!("Deleted"),
        Err(e) => eprintln!("Failed to delete: {:?}", e),
    }

*/