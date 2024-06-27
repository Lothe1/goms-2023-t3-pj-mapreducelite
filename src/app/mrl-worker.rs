// use anyhow::*;
#![ allow(warnings)]
use itertools::Itertools;
use mrlite::*;
use bytes::Bytes;
use clap::Parser;
use cmd::worker::Args;
use tokio::time::sleep;
use tonic::{Request, Response};
use tonic::{transport::Server, Status};
use tonic::transport::Channel;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write, BufRead};
use std::sync::{Arc, Mutex};
use std::time::Duration;
// use std::io::{BufReader, Read};
// use aws_sdk_s3::config::{Credentials, Config, Region, endpoint};
use aws_sdk_s3::Client;
use S3::minio;
use standalone::Job;

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::{JobRequest, Task, WorkerRegistration, WorkerReport, WorkerRequest, WorkerResponse};
use mapreduce::coordinator_client::CoordinatorClient;

async fn map(client: &Client, job: &Job) -> Result<(), anyhow::Error> {
    let engine = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    let content = minio::get_object(&client, bucket_name, object_name).await?;
    let input_kv = KeyValue {
        key: Bytes::from(object_name.clone()),
        value: Bytes::from(content),
    };

    let serialized_args = Bytes::from(job.args.join(" "));
    let map_func = engine.map_fn;

    let mut intermediate_data = Vec::new();
    for item in map_func(input_kv, serialized_args.clone())? {
        let kv = item?;
        intermediate_data.push(kv);
    }

    // Store intermediate data back to S3 or a temporary location
    let temp_path = format!("/temp/{}", job.output);
    let mut file = File::create(&temp_path)?;
    for kv in &intermediate_data {
        writeln!(file, "{}\t{}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value))?;
    }

    Ok(())
}

async fn reduce(client: &Client, job: &Job) -> Result<(), anyhow::Error> {
    let engine: Workload = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    let temp_path = format!("/temp/{}", object_name);
    let file = File::open(&temp_path)?;
    let reader = io::BufReader::new(file);

    let mut intermediate_data = HashMap::new();
    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 2 {
            let key = parts[0].to_string();
            let value = parts[1].to_string();
            intermediate_data.entry(key).or_insert_with(Vec::new).push(value);
        }
    }

    let mut output_data = Vec::new();
    let serialized_args = Bytes::from(job.args.join(" "));
    let reduce_func = engine.reduce_fn;
    for (key, values) in intermediate_data {        
        let kv = KeyValue {
            key: Bytes::from(key.clone()),
            value: Bytes::from(values.join(",")),
        };
        
        let value_iter = Box::new(values.into_iter().map(Bytes::from));
        let reduced_value = reduce_func(Bytes::from(key.clone()), value_iter, serialized_args.clone())?;
        output_data.push(KeyValue { key: Bytes::from(key.clone()), value: reduced_value });
    }

    // Store the reduced data back to S3 or final output location
    let output_path = format!("/output/{}", job.output);
    let mut file = File::create(&output_path)?;
    for kv in &output_data {
        writeln!(file, "{}\t{}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value))?;
    }
    Ok(())
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!("Hello worker!\n");

    // Parse command line arguments
    let args = Args::parse();
    let ip = args.join; 
    print!("IP to join: {}", ip);

    // Connect to coordinator
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

    // Main loop to receive and process tasks
    loop {
        println!("Waiting for tasks...");
        match client.get_task(Request::new(WorkerRequest {})).await {
            Ok(response) => {
                let task = response.into_inner();
                println!("Received task: {:?}", task);

                let job = Job {
                    input: task.input.clone(),
                    workload: task.workload.clone(),
                    output: task.output.clone(),
                    args: task.args.clone().split_whitespace().map(String::from).collect(),
                };

                // Process task based on its type
                match task.status.as_str() {
                    "Map" => {
                        if let Err(err) = map(&s3_client, &job).await {
                            eprintln!("Error during map task: {:?}", err);
                        }
                    }
                    "Reduce" => {
                        if let Err(err) = reduce(&s3_client, &job).await {
                            eprintln!("Error during reduce task: {:?}", err);
                        }
                    }
                    _ => {
                        eprintln!("Invalid task status received: {}", task.status);
                    }
                }

                // Report task completion to coordinator
                let report = Request::new(WorkerReport {
                    task: task.status,
                    input: task.input,
                    output: task.output,
                });
                if let Err(err) = client.report_task(report).await {
                    eprintln!("Error reporting task completion: {:?}", err);
                }
            }
            Err(status) => {
                eprintln!("Error receiving task: {:?}", status);
                sleep(Duration::from_secs(1)).await; // Sleep before retrying
            }
        }

        // Sleep for a short period before checking for the next task
        sleep(Duration::from_secs(1)).await;
    }

    // // Listen for tasks
    // loop {
    //     println!("Sending a request for a job!");
    //     let resp = client.get_task(Request::new(WorkerRequest {  })).await;
    //     match resp {
    //         Ok(t) => {
    //             let task = t.into_inner();
    //             let job = Job {
    //                 input: task.input.clone(),
    //                 workload: task.workload.clone(),
    //                 output: task.output.clone(),
    //                 args: Vec::new()
    //             };
    //             let _ = sleep(Duration::from_secs(1)).await;
    //             // if it is a mapPhase -> call map
    //             // if it is a ReducePhase -> call reduce
    //             let task_complete = match task.status.clone() {
    //                 s if s==format!("Map") => {
    //                     map(&s3_client, &job).await
    //                 }
    //                 s if s==format!("Reduce") => {
    //                     reduce(&s3_client, &job).await
    //                 }
    //                 _ => {
    //                     // should not reach here
    //                     eprintln!("Invalid task assigned!");
    //                     map(&s3_client, &job).await
    //                 }
    //             };
    //             match task_complete {
    //                 Ok(_) => {
    //                     client.report_task(Request::new(WorkerReport { 
    //                         task: task.status.clone(),
    //                         input: task.input.clone(),
    //                         output: task.output.clone(),
    //                     })).await;
    //                 }
    //                 Err(err) => {
    //                     // should not occur.... just saying :/
    //                     eprintln!("{:?}", err)
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             // no task
    //             let _ = sleep(Duration::from_secs(1)).await;
    //         }
    //     }
    // }
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