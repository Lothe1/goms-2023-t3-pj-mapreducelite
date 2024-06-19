use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Connect to a coordinator given an IP address and port
    Join {
        /// Glob spec for the coordinator address
        #[arg(short, long)]
        addr: String,
    }
}
