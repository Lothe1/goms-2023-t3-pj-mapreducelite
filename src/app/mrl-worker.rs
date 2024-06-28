// use anyhow::*;
#![ allow(warnings)]
use cmd::coordinator::now;
use itertools::Itertools;
use mrlite::*;
use bytes::Bytes;
use clap::Parser;
use cmd::worker::Args;
use prost::Name;
use tokio::time::sleep;
use tonic::{Request, Response};
use tonic::{transport::Server, Status};
use tonic::transport::Channel;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
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
use mrlite::Encode::encode_decode::{append_parquet, KeyValueList_to_KeyListandValueList, make_writer};
use mrlite::S3::minio::upload_parts;

async fn map(client: &Client, job: &Job) -> Result<String, anyhow::Error> {
    let engine = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    let content = minio::get_object(&client, bucket_name, object_name).await?;
    let input_kv = KeyValue {
        key: Bytes::from(object_name.clone()),
        value: Bytes::from(content),
    };
    // println!("{:?}", input_kv.key);
    let serialized_args = Bytes::from(job.args.join(" "));
    let map_func = engine.map_fn;

    let mut intermediate_data = Vec::new();
    for item in map_func(input_kv, serialized_args.clone())? {
        let kv = item?;
        intermediate_data.push(kv);
    }
    // println!("{:?}", intermediate_data); // works here

    // Store intermediate data back to S3 or a temporary location
    let _ = fs::create_dir_all("./_temp")?;
    let filename = now();
    let temp_path = format!("./_temp/{}", filename);
    // println!("{:?}", temp_path);
    let mut file_res = OpenOptions::new().write(true).create(true).open(&temp_path); //File::create(&temp_path)?;
    // println!("{:?}", file_res);
    let mut file = file_res.unwrap();
    // println!("File created!");
    let mut content = format!(""); 
    for kv in &intermediate_data {
        // println!("{:?}", &kv.value);
        writeln!(file, "{}\t{}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value))?;
        content = format!("{content}\n{}\t{}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value));
    }

    match minio::upload_string(&client, bucket_name, &format!("{}{}", job.output, filename), &content).await {
        Ok(_) => println!("Uploaded"),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }

    Ok(filename.to_string())
}


async fn reduce(client: &Client, job: &Job) -> Result<String, anyhow::Error> {
    let engine: Workload = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    // Fetch intermediate data from MinIO
    let content = minio::get_object(&client, bucket_name, object_name).await?;
    let reader = io::BufReader::new(content.as_bytes());

    // Intermediate data storage
    let mut intermediate_data = HashMap::new();

    // Read and parse intermediate data
    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 2 {
            let key = parts[0].to_string();
            let value = parts[1].to_string();
            intermediate_data.entry(key).or_insert_with(Vec::new).push(value);
        }
    }

    // Sort intermediate data by key
    let mut sorted_intermediate_data: Vec<(String, Vec<String>)> = intermediate_data.into_iter().collect();
    sorted_intermediate_data.sort_by(|a, b| a.0.cmp(&b.0));

    // Reduce intermediate data
    let mut output_data = Vec::new();
    let serialized_args = Bytes::from(job.args.join(" "));
    let reduce_func = engine.reduce_fn;

    for (key, values) in sorted_intermediate_data {
        let kv = KeyValue {
            key: Bytes::from(key.clone()),
            value: Bytes::from(values.join(",")),
        };

        let value_iter = Box::new(values.into_iter().map(Bytes::from));
        let reduced_value = reduce_func(Bytes::from(key.clone()), value_iter, serialized_args.clone())?;
        output_data.push(KeyValue { key: Bytes::from(key.clone()), value: reduced_value });
    }

    // Prepare and upload the final output
    let filename = now();
    let mut content = String::new();

    for kv in &output_data {
        // println!("k: {:?} || v: {}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value));
        content.push_str(&format!("{}\t{}\n", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value)));
    }

    match minio::upload_string(&client, bucket_name, &format!("{}{}", job.output, filename), &content).await {
        Ok(_) => println!("Uploaded to {}{}", job.output, filename),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }

    Ok(filename.to_string())
}

async fn map2(client: &Client, job: &Job) -> Result<String, anyhow::Error> {
    let engine = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    let content = minio::get_object(&client, bucket_name, object_name).await?;
    let input_kv = KeyValue {
        key: Bytes::from(object_name.clone()),
        value: Bytes::from(content),
    };
    // println!("{:?}", input_kv.key);
    let serialized_args = Bytes::from(job.args.join(" "));
    let map_func = engine.map_fn;

    let mut intermediate_data = Vec::new();
    for item in map_func(input_kv, serialized_args.clone())? {
        let kv = item?;
        intermediate_data.push(kv);
    }
    // println!("{:?}", intermediate_data); // works here

    // Store intermediate data back to S3 or a temporary location
    let _ = fs::create_dir_all("./_temp")?;
    let filename = now();
    let temp_path = format!("/_temp/{}", filename);
    // println!("{:?}", temp_path);
    let mut file_res = OpenOptions::new().write(true).create(true).open(&format!(".{temp_path}")); //File::create(&temp_path)?;
    // println!("{:?}", file_res);
    let mut file = file_res.unwrap();
    // println!("File created!");

    let mut writer = make_writer(&mut file);

    let (keys, values) = KeyValueList_to_KeyListandValueList(intermediate_data);
    append_parquet(&file, &mut writer, keys, values);

    writer.close().unwrap();

    upload_parts(&client, bucket_name, &temp_path).await.unwrap();

    Ok(filename.to_string())
}

async fn reduce2(client: &Client, job: &Job) -> Result<String, anyhow::Error> {
    let engine: Workload = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let object_name = &job.input;
    println!("{:?}", object_name);

    // Fetch intermediate data from MinIO
    let content = minio::get_object(&client, bucket_name, object_name).await?;

    let reader = io::BufReader::new(content.as_bytes());

    // Intermediate data storage
    let mut intermediate_data = HashMap::new();

    // Read and parse intermediate data
    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 2 {
            let key = parts[0].to_string();
            let value = parts[1].to_string();
            intermediate_data.entry(key).or_insert_with(Vec::new).push(value);
        }
    }

    // Sort intermediate data by key
    let mut sorted_intermediate_data: Vec<(String, Vec<String>)> = intermediate_data.into_iter().collect();
    sorted_intermediate_data.sort_by(|a, b| a.0.cmp(&b.0));

    // Reduce intermediate data
    let mut output_data = Vec::new();
    let serialized_args = Bytes::from(job.args.join(" "));
    let reduce_func = engine.reduce_fn;

    for (key, values) in sorted_intermediate_data {
        let kv = KeyValue {
            key: Bytes::from(key.clone()),
            value: Bytes::from(values.join(",")),
        };

        let value_iter = Box::new(values.into_iter().map(Bytes::from));
        let reduced_value = reduce_func(Bytes::from(key.clone()), value_iter, serialized_args.clone())?;
        output_data.push(KeyValue { key: Bytes::from(key.clone()), value: reduced_value });
    }

    // Prepare and upload the final output
    let filename = now();
    let mut content = String::new();

    for kv in &output_data {
        // println!("k: {:?} || v: {}", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value));
        content.push_str(&format!("{}\t{}\n", String::from_utf8_lossy(&kv.key), String::from_utf8_lossy(&kv.value)));
    }

    match minio::upload_string(&client, bucket_name, &format!("{}{}", job.output, filename), &content).await {
        Ok(_) => println!("Uploaded to {}{}", job.output, filename),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }

    Ok(filename.to_string())
}




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
                let out_fn = match task.status.as_str() {
                    "Map" => {
                        match map2(&s3_client, &job).await {
                            Ok(name) => Some(name),
                            Err(err) => {
                                eprintln!("Error during map task: {:?}", err);
                                None
                            }
                        }
                    }
                    "Reduce" => {
                        match reduce2(&s3_client, &job).await {
                            Ok(name) => Some(name),
                            Err(err) => {
                                eprintln!("Error during reduce task: {:?}", err);
                                None
                            }
                        }
                    }
                    _ => {
                        eprintln!("Invalid task status received: {}", task.status);
                        None
                    }
                };

                if out_fn.is_some() {
                    // Report task completion to coordinator
                    let report = Request::new(WorkerReport {
                        task: task.status,
                        input: task.input,
                        output: format!("{}{}", task.output, out_fn.unwrap()),
                    });
                    if let Err(err) = client.report_task(report).await {
                        eprintln!("Error reporting task completion: {:?}", err);
                    }
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