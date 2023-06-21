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
    Prime,
    Mean,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Echo => protohacker::echo::main()?,
        Command::Prime => protohacker::prime::main()?,
        Command::Mean => protohacker::mean::main()?,
    }

    Ok(())
}
