// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::worker::Args;

fn main() {
    print!("Hello worker!\n");
    let args = Args::parse();
    let ip = args.join;
    print!("IP to join: {}", ip)
}
