use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use stomp::BoxedClientProcessor;
use stomp::BoxedServerProcessor;
use stomp::Client;
use stomp::ClientCommandProcessor;
use stomp::Frame;
use stomp::Header;
use stomp::Server;
use stomp::ServerCommand;
use stomp::StompError;
use stomp::StompState;
use stompserver::ServerProcessor;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

async fn spawn_server() -> (JoinHandle<Result<(), std::io::Error>>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(StompError::IO)
        .expect("it created listener");

    let addr = listener.local_addr().expect("server is running");

    let jh: JoinHandle<Result<(), std::io::Error>> = tokio::spawn(async move {
        let transactions = Arc::new(RwLock::new(HashMap::new()));
        let channels = Arc::new(RwLock::new(HashMap::new()));
        let processor = move || {
            Box::new(ServerProcessor::new(transactions.clone(), channels.clone()))
                as BoxedServerProcessor
        };
        let mut service = Server::new(processor);

        service.serve_tcp(listener).await
    });
    (jh, addr)
}

fn do_something(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime created");
    let (jh, addr) = rt.block_on(async { spawn_server().await });

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut group = c.benchmark_group("do_something");
    group.bench_with_input(BenchmarkId::new("connection", 5_000), &addr, |b, addr| {
        let my_counter = counter.clone();
        b.to_async(Runtime::new().expect("new runtime"))
            .iter(|| async {
                let connect_frame = match addr.to_string().split_once(':') {
                    Some((addr, port)) => {
                        let port_header = Header::new("port", Some(port));
                        Frame::stomp(addr, vec![port_header])
                    }
                    None => panic!("Couldn't parse the server address"),
                };

                let operations = vec![
                    connect_frame,
                    Frame::send("dest", vec![], b"some"),
                    Frame::disconnect::<&str>(Some("shutdown")),
                ];

                let connector = |addr: String| async { TcpStream::connect(addr).await };
                let processor = || Box::new(ClientProcessor::new()) as BoxedClientProcessor;
                let mut client = Client::new(connector, processor);
                client.send(futures::stream::iter(operations)).await?;
                Ok::<(), StompError>(())
            });
    });
    group.finish();
    jh.abort();
    println!(
        "counter: {}",
        counter.load(std::sync::atomic::Ordering::Relaxed)
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .nresamples(1001)
        .warm_up_time(std::time::Duration::from_millis(10));
    targets = do_something
);

criterion_main!(benches);

#[derive(Debug)]
struct ClientProcessor;

impl ClientProcessor {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl ClientCommandProcessor for ClientProcessor {
    async fn process_server_frame(
        &mut self,
        state: &StompState,
        cmd: &ServerCommand,
        frame: Frame,
    ) -> Result<Vec<Frame>, StompError> {
        match state {
            StompState::Connected => match cmd {
                ServerCommand::Connected => {
                    println!("connected: \n{frame}\nstomp> ");
                }
                ServerCommand::Message => {
                    println!("message: \n{frame}\nstomp> ");
                }
                ServerCommand::Error => {
                    println!("error: \n{frame}\nstomp> ");
                }
                ServerCommand::Receipt => {
                    println!("receipt: \n{frame}\nstomp> ");
                }
            },
            StompState::Disconnected => match cmd {
                ServerCommand::Receipt => {
                    println!("receipt: \n{frame}\nstomp> ");
                    if let Some(receipt) = frame.get_header("receipt-id") {
                        if receipt == "shutdown" {
                            return Err(StompError::Disconnected(vec![frame]));
                        }
                    }
                }
                ServerCommand::Message => {
                    println!("message: \n{frame}\nstomp> ");
                }
                ServerCommand::Error => {
                    println!("error: \n{frame}\nstomp> ");
                }
                _ => {
                    println!("Unrecognised ServerCommand: \n{frame}\nstomp> ");
                }
            },
            StompState::Transaction => match cmd {
                ServerCommand::Message => {
                    println!("message: \n{frame}\nstomp> ");
                }
                ServerCommand::Error => {
                    println!("error: \n{frame}\nstomp> ");
                }
                ServerCommand::Receipt => {
                    println!("receipt: \n{frame}\nstomp> ");
                }
                _ => {
                    println!("Unrecognised ServerCommand in transaction: \n{frame}\nstomp> ");
                }
            },
        }
        Ok(vec![])
    }
}
