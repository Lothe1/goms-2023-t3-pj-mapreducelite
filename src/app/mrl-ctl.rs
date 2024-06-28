#![ allow(warnings)]
use clap::Parser;
use tonic::transport::Channel;
use tonic::Request;
use mrlite::cmd::ctl::{Args, Commands};

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_client::CoordinatorClient;
use mapreduce::{JobRequest, JobListRequest, Empty, Status as SystemStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let host = match args.host {
        Some(h) => h,
        None => format!("127.0.0.1:50051") // If the host is not specified, assume read from config file na kub (future)
    };

    let mut client = CoordinatorClient::connect(format!("http://{}", host)).await?;

    match args.command {
        Commands::Submit { input, workload, output , args} => {
            let request = Request::new(JobRequest {
                input,
                workload,
                output,
                args: "".to_string(),
            });

            let response = client.submit_job(request).await?;
            println!("Submitted job: {:?}", response);
        },
        Commands::Jobs {show} => {
            let show_req = match show {
                Some(s) => match s {
                    s if s == format!("all") || s == format!("a") => s,
                    s if s == format!("complete") || s == format!("c") => s,
                    s if s == format!("default") || s == format!("d") => s,
                    _ => format!("default")
                }
                None => format!("default")
            };
            println!("{}", show_req);
            let response = client.list_jobs(Request::new(JobListRequest {show: show_req})).await?;
            println!("Job list: {:?}", response); 
        },
        Commands::Status {} => {
            let response = client.system_status(Request::new(Empty {})).await?;
            println!("System status: {:?}", response);
            
            let system_status: SystemStatus = response.into_inner();
            println!("Worker Count: {}", system_status.worker_count);
            for worker in system_status.workers {
                println!("Worker Address: {}, State: {}", worker.address, worker.state);
            }
            println!("Jobs: {:?}", system_status.jobs);
        },
    }

    Ok(())
}
