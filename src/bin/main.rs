use clap::{Parser, Subcommand};
use protohacker;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, default_value_t = 6789)]
    port: u16,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Echo,
    Prime,
    Mean,
    Chat,
    KVDB,
    MITM,
    Speed,
    Reversal,
    ReversalClient,
    ISL,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let addr = ([0, 0, 0, 0], cli.port).into();

    match cli.command {
        Command::Echo => protohacker::echo::main(addr).await?,
        Command::Prime => protohacker::prime::main(addr).await?,
        Command::Mean => protohacker::mean::main(addr).await?,
        Command::Chat => protohacker::chat::main(addr).await?,
        Command::KVDB => protohacker::kvdb::main(addr).await?,
        Command::MITM => protohacker::mitm::main(addr).await?,
        Command::Speed => protohacker::speed::main(addr).await?,
        Command::Reversal => protohacker::reversal::main(addr).await?,
        Command::ReversalClient => protohacker::reversal::test_main(addr).await?,
        Command::ISL => protohacker::isl::main(addr).await?,
    }

    Ok(())
}
