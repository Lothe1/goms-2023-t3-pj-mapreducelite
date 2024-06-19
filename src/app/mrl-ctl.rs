// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::ctl::{Args, Commands};
use standalone::Job;

fn main() {
    print!("Hello!");
    let args = Args::parse();
    match args.command {
        Commands::Submit {
            input,
            workload,
            output,
            args,
        } => {
            let job = Job {
                input,
                workload,
                output,
                args, 
            };
            let engine = workload::try_named(&job.workload).expect("what?");
            print!("Submitted {} to coordinator!", job.workload);
        },
        Commands::Jobs { } => {
            print!("Checking list of jobs! :D");
        },
        Commands::Status {  } => {
            print!("Checking status of jobs! :D");
        }
    }
}
