use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Connect to a coordinator at the given IP address and port
    #[clap(short, long)]
    pub join: String
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
