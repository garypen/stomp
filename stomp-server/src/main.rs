use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use clap::Parser;
use stomp::BoxedServerProcessor;
use stomp::Server;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use stompserver::ServerProcessor;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Server Address
    #[arg(short, long)]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_line_number(true)
        .with_file(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let transactions = Arc::new(RwLock::new(HashMap::new()));
    let channels = Arc::new(RwLock::new(HashMap::new()));
    let processor = move || {
        Box::new(ServerProcessor::new(transactions.clone(), channels.clone()))
            as BoxedServerProcessor
    };
    let mut service = Server::new(processor);

    tracing::info!(address=%&cli.address, "ready to serve at");

    let listener = TcpListener::bind(&cli.address).await?;

    Ok(service.serve_tcp(listener).await?)
}
