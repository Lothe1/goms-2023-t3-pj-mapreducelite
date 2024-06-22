use clap::{Parser, Subcommand};
use tonic::transport::Channel;
use tonic::Request;

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_client::CoordinatorClient;
use mapreduce::{JobRequest, Empty, Status as SystemStatus};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
    #[clap(long)]
    coordinator: String,
}

#[derive(Subcommand)]
enum Commands {
    Submit {
        input: String,
        workload: String,
        output: String,
    },
    Jobs,
    Status,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let coordinator_address = args.coordinator;

    let mut client = CoordinatorClient::connect(format!("http://{}", coordinator_address)).await?;

    match args.command {
        Commands::Submit { input, workload, output } => {
            let request = Request::new(JobRequest {
                input,
                workload,
                output,
                args: "".to_string(),
            });

            let response = client.submit_job(request).await?;
            println!("Submitted job: {:?}", response);
        },
        Commands::Jobs => {
            let response = client.list_jobs(Request::new(Empty {})).await?;
            println!("Job list: {:?}", response);
        },
        Commands::Status => {
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
