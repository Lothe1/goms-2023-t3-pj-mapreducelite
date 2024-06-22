use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // #[clap(subcommand)]
    // pub command: Commands,
    /// [OPT] Specified port for coordinator to listen to
    #[clap(short, long, default_value = None, short = 'P')]
    pub port: Option<u128>
}
