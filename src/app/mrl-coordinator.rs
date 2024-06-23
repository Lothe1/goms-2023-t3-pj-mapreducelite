use std::collections::VecDeque;
use std::net::SocketAddr;
// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::Args;
// use tokio::net::TcpListener;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_server::{Coordinator, CoordinatorServer};
use mapreduce::{WorkerRegistration, WorkerResponse, WorkerRequest, Task, JobRequest, JobResponse, Empty, JobList, Status as SystemStatus, Worker};


/* 
    Only one coordinator !!
*/

// Only one job should be in either of the following states: `{MapPhase, Shuffle, ShufflePhase}``; 
// all other jobs should either be `Pending` or `Completed`.
#[derive(Debug)]
enum JobStatus {
    Pending,
    MapPhase,
    Shuffle,
    ShufflePhase,
    Completed
} 

// Struct for a Job, which holds the status of a job
// and the assigned `standalone::Job`.
struct Job {
    status: JobStatus,
    job: standalone::Job,
}

// The default state for a worker node is `Idle`, meaning no work is assigned but the worker is alive.
// 
// A worker node is `busy` if it is currently operating a task, and `dead` if it has not responded
// to a status check (heartbeat).
#[derive(Debug)]
enum WorkerState {
    Idle,
    Busy,
    Dead
}

// Struct for a worker, which holds the state of a worker
// and the IP address of the worker to send RPC for communication.
struct WorkerNode {
    state: WorkerState,
    addr: SocketAddr,
}

// Implement the default trait for the CoordinatorService struct
impl Default for CoordinatorService {
    fn default() -> Self {
        Self {
            job_queue: Arc::new(Mutex::new(VecDeque::new())),
            workers: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

// Struct for the coordinator, which holds the job queue and the worker list.
pub struct CoordinatorService {
    job_queue: Arc<Mutex<VecDeque<Job>>>,
    workers: Arc<Mutex<Vec<WorkerNode>>>
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {

    // Register a worker with the coordinator
    // This function is called when a worker node wants to register itself with the coordinator. 
    // When a worker starts up, it should send a registration request to the coordinator to announce its availability for task assignments
    async fn register_worker(&self, request: Request<WorkerRegistration>) -> Result<Response<WorkerResponse>, Status> {
        let worker = WorkerNode {
            state: WorkerState::Idle,
            addr: request.remote_addr().unwrap(),
        };
        println!("New worker joined at {:?}", worker.addr.to_string());
        self.workers.lock().unwrap().push(worker);
        Ok(Response::new(WorkerResponse {
            success: true,
            message: "Worker registered".into(),
        }))
    }

    // Get a task from the job queue
    // and return the task to the worker
    // This function is called by a worker node when it requests a task from the coordinator. 
    // Once a worker is registered and ready to perform work, it will periodically request tasks from the coordinator to execute
    async fn get_task(&self, _request: Request<WorkerRequest>) -> Result<Response<Task>, Status> {
        let job_option = self.job_queue.lock().unwrap().pop_front();
        match job_option {
            Some(job) => {
                let task = Task {
                    input: job.job.input,
                    workload: job.job.workload,
                    output: job.job.output,
                    args: job.job.args.join(" ") // Convert vector of strings to a single string
                };
                Ok(Response::new(task))
            },
            None => Err(Status::not_found("No job available")),
        }
    }

    // Submit a job to the job queue
    // and return a response to the client
    async fn submit_job(&self, request: Request<JobRequest>) -> Result<Response<JobResponse>, Status> {
        let standalone_job = standalone::Job {
            input: request.get_ref().input.clone(),
            workload: request.get_ref().workload.clone(),
            output: request.get_ref().output.clone(),
            args: request.get_ref().args.clone().split_whitespace().map(String::from).collect() // Change this to a vector of strings
        };

        let job = Job {
            status: JobStatus::Pending,
            job: standalone_job, 
        };

        self.job_queue.lock().unwrap().push_back(job);

        Ok(Response::new(JobResponse {
            success: true,
            message: "Job submitted".into(),
        }))
    }   

    // List all jobs in the job queue
    // and return the list to the client
    async fn list_jobs(&self, _request: Request<Empty>) -> Result<Response<JobList>, Status> {
        let jobs = self.job_queue.lock().unwrap();
        // Define a standalone function to convert from Job to Task
        fn job_to_task(job: mrlite::standalone::Job) -> Task {
            Task {
                input: job.input,
                workload: job.workload,
                output: job.output,
                args: job.args.join(" ")
            }
        }
        // Use the job_to_task function inside the map function
        let tasks: Vec<Task> = jobs.iter().map(|job| job_to_task(job.job.clone())).collect();
        Ok(Response::new(JobList { jobs: tasks }))
    }
    
    // Get the system status
    async fn system_status(&self, _request: Request<Empty>) -> Result<Response<SystemStatus>, Status> {
        let workers = self.workers.lock().unwrap();
        let worker_list: Vec<Worker> = workers.iter().map(|worker| Worker {
            address: worker.addr.to_string().clone(),
            state: format!("{:?}", worker.state),
        }).collect();

        Ok(Response::new(SystemStatus {
            worker_count: workers.len() as i32,
            workers: worker_list,
            jobs: Vec::new(), // Add job details here if needed
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!("Hello coordinator!\n");
    let args = Args::parse();
    let port: u128 = match args.port {
        Some(p) => p,
        None => 50051,
    };
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let coordinator = CoordinatorService::default();

    println!("Coordinator listening on {}", addr);

    // Start a new the gRPC server
    // and add the coordinator service to the server
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}

// #[tokio::main]
// async fn main() {
//     print!("Hello coordinator!\n");
//     let args = Args::parse();
//     let port: Option<u128> = args.port;
//     // Check for port !!
//     print!("Port to listen to: {:?}\n", port);
//     // There are no required arguments (yet)

//     let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
//     println!("Listening!");
//     let jobQueue: VecDeque<Job> = VecDeque::new();

// }