// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::{Args, Commands};

fn main() {
    print!("Hello coordinator!");
    let args = Args::parse();
    match args.command {
        Commands::Port {
            port,
            args,
        } => {
            // host the coordinatorrr
            print!("Listening to port {}!", port.unwrap());
        }
    }
}
