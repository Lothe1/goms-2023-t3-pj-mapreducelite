// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::worker::Args;

fn main() {
    print!("Hello worker!");
    let args = Args::parse();
    let ip = args.join;
    print!("{}", ip)
}
