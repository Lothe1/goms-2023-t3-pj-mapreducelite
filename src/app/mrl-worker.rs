// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::worker::{Args, Commands};

fn main() {
    print!("Hello worker!");
    let args = Args::parse();
    match args.command {
        Commands::Join {
            addr
        } => {
            // host the coordinatorrr
            print!("Joining with coordinator @ {}!", addr);
        }
    }
}
