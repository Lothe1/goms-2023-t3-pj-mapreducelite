use std::collections::VecDeque;

// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::Args;
use tokio::net::TcpListener;

/* 
    Only one coordinator !!
*/

/// Only one job should be in either of the following states: `{MapPhase, Shuffle, ShufflePhase}``; 
/// all other jobs should either be `Pending` or `Completed`.
enum JobStatus {
    Pending,
    MapPhase,
    Shuffle,
    ShufflePhase,
    Completed
} 

/// Struct for a Job, which holds the status of a job
/// and the assigned `standalone::Job`.
struct Job {
    status: JobStatus,
    job: standalone::Job,
}

/// The default state for a worker node is `Idle`, meaning no work is assigned but the worker is alive.
/// 
/// A worker node is `busy` if it is currently operating a task, and `dead` if it has not responded
/// to a status check (heartbeat).
enum WorkerState {
    Idle,
    Busy,
    Dead
}

/// Struct for a worker, which holds the state of a worker
/// and the IP address of the worker to send RPC for communication.
struct Worker {
    state: WorkerState,
    addr: String
}

#[tokio::main]
async fn main() {
    print!("Hello coordinator!\n");
    let args = Args::parse();
    let port: Option<u128> = args.port;
    // Check for port !!
    print!("Port to listen to: {:?}\n", port);
    // There are no required arguments (yet)

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening!");
    let jobQueue: VecDeque<Job> = VecDeque::new();

}
