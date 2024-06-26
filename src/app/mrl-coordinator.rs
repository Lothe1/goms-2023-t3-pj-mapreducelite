use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder, Credentials, Region};
use itertools::Itertools;
// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::{now, Args};
// use tokio::net::TcpListener;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_server::{Coordinator, CoordinatorServer};
use mapreduce::{WorkerRegistration, WorkerResponse, WorkerRequest, Task, JobRequest, JobResponse, WorkerReport, Empty, JobList, Status as SystemStatus, Worker};
use mrlite::S3::minio::*;

/* 
    Only one coordinator !!
*/

// Only one job should be in either of the following states: `{MapPhase, Shuffle, ShufflePhase}``; 
// all other jobs should either be `Pending` or `Completed`.
#[derive(Debug, Clone, PartialEq)]
enum JobStatus {
    Pending,
    MapPhase,
    Shuffle,
    ReducePhase,
    Completed
} 

#[derive(Debug, Clone)]
struct FileStatus {
    status: JobStatus,
    elapsed: u128,
}

// Struct for a Job, which holds the status of a job
// and the assigned `standalone::Job`.
#[derive(Clone, Debug)]
struct Job {
    id: String,
    status: JobStatus,
    job: standalone::Job,
    files: Vec<String>,
    file_status: Arc<Mutex<HashMap<String, FileStatus>>>,
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

// Creates a new Coordinator Service with the supplied arguments
impl CoordinatorService {
    fn new(ip: impl ToString, user: impl ToString, pw: impl ToString, client: Client) -> Self {
        Self {
            job_queue: Arc::new(Mutex::new(VecDeque::new())),
            workers: Arc::new(Mutex::new(Vec::new())),
            os_ip: ip.to_string(),
            os_user: user.to_string(),
            os_pw: pw.to_string(),
            s3_client: client,
        }
    }
}

// Struct for the coordinator, which holds the job queue and the worker list.
pub struct CoordinatorService {
    job_queue: Arc<Mutex<VecDeque<Job>>>,
    workers: Arc<Mutex<Vec<WorkerNode>>>,
    os_ip: String,
    os_user: String,
    os_pw: String,
    s3_client: Client
}

fn get_next_file(files: &Vec<String>, file_status: &HashMap<String, FileStatus>) -> String {
    for file in files {
        let this_file_status = file_status.get(file).unwrap();
        match this_file_status.status {
            JobStatus::Pending => {
                return file.clone();
            }
            JobStatus::MapPhase => {
                if (now() - this_file_status.elapsed) > Duration::from_secs(15).as_nanos() {
                    return file.clone();
                }
            }
            JobStatus::Shuffle => {

            }
            JobStatus::ReducePhase => {

            }
            JobStatus::Completed => {

            }
        }
    }
    return files.get(0).unwrap().clone();
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
        let mut args: HashMap<String, String> = HashMap::new();
        args.insert("ip".into(), self.os_ip.clone());
        args.insert("user".into(), self.os_user.clone());
        args.insert("pw".into(), self.os_pw.clone());
        self.workers.lock().unwrap().push(worker);
        Ok(Response::new(WorkerResponse {
            success: true,
            message: "Worker registered".into(),
            args: args,
        }))
    }

    // Get a task from the job queue
    // and return the task to the worker
    // This function is called by a worker node when it requests a task from the coordinator. 
    // Once a worker is registered and ready to perform work, it will periodically request tasks from the coordinator to execute
    async fn get_task(&self, _request: Request<WorkerRequest>) -> Result<Response<Task>, Status> {
        let mut job_q = self.job_queue.lock().unwrap();
        // let job_option = self.job_queue.lock().unwrap().pop_front();
        match job_q.pop_front() {
            Some(mut job) => {
                match job.status {
                    JobStatus::Pending => {
                        // pick first file -> change job's status to MapPhase
                        // println!("{:?}", job.files);
                        let mut input_file = job.file_status.lock().unwrap();
                        // println!("{:?}", input_file);
                        // println!("{:?}", job.files);
                        let new_status = FileStatus {
                            status: JobStatus::MapPhase,
                            elapsed: now(),
                        };
                        input_file.insert( job.files.get(0).unwrap().to_string().clone(), new_status.clone());

                        let task = Task {
                            input: job.files.get(0).unwrap().to_string().clone(), 
                            workload: job.job.workload.clone(),
                            output: "/temp/".into(), //tmp file
                            args: job.job.args.join(" "),
                            status: "Map".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::MapPhase,
                            job: job.job.clone(),
                            files: job.files.clone(),
                            file_status: Arc::new(Mutex::new(input_file.clone())),
                        };
                        job_q.push_front(modified_job);
                        // job.status = JobStatus::MapPhase;
                        println!("Gave a pending task");
                        return Ok(Response::new(task))
                    }
                    JobStatus::MapPhase => {
                        let mut input_file = job.file_status.lock().unwrap();
                        let file = get_next_file(&job.files, &input_file);
                        // println!("{:?}", input_file);
                        // println!("{:?}", job.files);
                        let new_status = FileStatus {
                            status: JobStatus::MapPhase,
                            elapsed: now(),
                        };
                        input_file.insert( file.clone(), new_status.clone());

                        let task = Task {
                            input: file.clone(), 
                            workload: job.job.workload.clone(),
                            output: "/temp/".into(), //tmp file
                            args: job.job.args.join(" "),
                            status: "Map".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::MapPhase,
                            job: job.job.clone(),
                            files: job.files.clone(),
                            file_status: Arc::new(Mutex::new(input_file.clone())),
                        };
                        job_q.push_front(modified_job);
                        println!("Gave a pending task or lagging task");
                        return Ok(Response::new(task))
                    }
                    JobStatus::Shuffle => {
                        
                        job_q.push_front(job);
                        println!("Gave a pending task");

                        return Err(Status::not_found("Not implemented"))

                    }
                    JobStatus::ReducePhase => {
                        
                        job_q.push_front(job);
                        println!("Gave a pending task or lagging task");

                        return Err(Status::not_found("Not implemented"))

                    }
                    JobStatus::Completed => {
                        job_q.push_front(job);
                        println!("Should not occur yet");

                        return Err(Status::not_found("No job available"))
                    }
                }
            },
            None => Err(Status::not_found("No job available")),
        }
    }

    // Submit a job to the job queue
    // and return a response to the client
    async fn submit_job(&self, request: Request<JobRequest>) -> Result<Response<JobResponse>, Status> {
        let _ = match workload::try_named(&request.get_ref().workload.clone()) {
            Some(engine) => engine,
            None => {
                return Ok(Response::new(JobResponse {
                    success: false,
                    message: "Invalid workload".into(),
                })
                )
            }
        };

        let standalone_job = standalone::Job {
            input: request.get_ref().input.clone(),
            workload: request.get_ref().workload.clone(),
            output: request.get_ref().output.clone(),
            args: request.get_ref().args.clone().split_whitespace().map(String::from).collect() // Change this to a vector of strings
        };
        
        let list_input_files = list_files_with_prefix(&self.s3_client, "mrl-lite", &standalone_job.input).await.unwrap();
        println!("Num files submitted: {}", list_input_files.len());

        let mut input_files: HashMap<String, FileStatus> = HashMap::new();
        let _ = list_input_files.clone().into_iter().for_each(|f| {input_files.insert(f, FileStatus { status: JobStatus::Pending, elapsed: 0 });});
        // Creates the output directory 
        let _ = create_directory(&self.s3_client, "mrl-lite", &standalone_job.output).await;
        println!("Output dir {} created", &standalone_job.output);

        // Generates the job id for the job
        // let job_id = calculate_hash(&standalone_job).to_string();
        // let _ = minio::create_directory(&self.s3_client, "mrl-lite", &format!("/temp/temp-{}", job_id)).await;

        let job_id = format!("{:?}", SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_nanos());
        println!("{job_id}");

        println!("{:?}", input_files);

        let job = Job {
            id: job_id,
            status: JobStatus::Pending,
            job: standalone_job, 
            // engine: wl,
            files: list_input_files.clone(),
            file_status: Arc::new(Mutex::new(input_files)),
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
        fn job_to_task(job: Job) -> Task {
            Task {
                input: job.job.input,
                workload: job.job.workload,
                output: job.job.output,
                args: job.job.args.join(" "),
                status: format!("{:?}", job.status),
            }
        }
        // Use the job_to_task function inside the map function
        println!("{}", format!("--------------\n{:?}\n-------------", jobs));

        let tasks: Vec<Task> = jobs.iter().map(|job| job_to_task(job.clone())).collect();
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

    async fn report_task(&self, request: Request<WorkerReport>) -> Result<Response<WorkerResponse>, Status> {
        Ok(Response::new(WorkerResponse { success: true, message: "".into(), args: HashMap::new()}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!("Hello coordinator!\n");
    let args = Args::parse();
    let port: u128 = args.port.unwrap_or(50051);
    let os_ip: String = args.os.unwrap_or_else(|| "127.0.0.1:9000".into());
    let os_user: String = args.user.unwrap_or_else(|| "ROOTNAME".into());
    let os_pw: String = args.pw.unwrap_or_else(|| "CHANGEME123".into());
    // Port to listen to
    let addr = format!("127.0.0.1:{port}").parse().unwrap();

    // If having trouble connecting to minio vvvvvvvvv
    // let s3_client = get_local_minio_client().await; 
    let s3_client = get_min_io_client(format!("http://{}",os_ip.clone()), os_user.clone(), os_pw.clone()).await.unwrap();
    let coordinator = CoordinatorService::new(os_ip.clone(), os_user.clone(), os_pw.clone(), s3_client.clone());

    println!("Coordinator listening on {}", addr);
    // Create a bucket for the coordinator, and the subdirectores if not exist
    initialize_bucket_directories(&coordinator.s3_client).await?;

    // Start a new the gRPC server
    // and add the coordinator service to the server
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}