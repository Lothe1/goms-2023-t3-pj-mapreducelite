// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::Args;

/* 
    Only one coordinator !!
*/

// Job Queue


fn main() {
    print!("Hello coordinator!\n");
    let args = Args::parse();
    let port: Option<u128> = args.port;
    // Check for port !!
    print!("{:?}", port);
    // There are no required arguments (yet)
}
