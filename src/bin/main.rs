use clap::{Parser, Subcommand};
use protohacker;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Echo,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Echo => protohacker::echo::main()?,
    }

    Ok(())
}
