use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use balter::prelude::*;
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
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

async fn spawn_server() -> JoinHandle<Result<(), std::io::Error>> {
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
    ADDR.get_or_init(|| addr);
    jh
}

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

static ADDR: OnceLock<SocketAddr> = OnceLock::new();

#[tokio::main]
async fn main() {
    let jh = spawn_server().await;
    /*
    connect_and_disconnect()
        .tps(50)
        .duration(Duration::from_secs(30))
        .await;

    connect_and_disconnect()
        .saturate()
        .duration(Duration::from_secs(30))
        .await;
    */
    connect_subscribe_send_and_disconnect()
        .tps(50)
        .duration(Duration::from_secs(30))
        .await;

    jh.abort();
    println!("terminating...");
}

#[scenario]
async fn connect_and_disconnect() {
    // ADDR in global until balter improves
    let client = connect_client(ADDR.get().unwrap())
        .await
        .expect("it has to work for now");
    disconnect_client(client)
        .await
        .expect("it has to work for now");
}

#[scenario]
async fn connect_subscribe_send_and_disconnect() {
    // ADDR in global until balter improves
    let mut client = connect_client(ADDR.get().unwrap())
        .await
        .expect("it has to work for now");
    subscribe_channel(&mut client)
        .await
        .expect("it has to work for now");
    send_channel(&mut client)
        .await
        .expect("it has to work for now");
    disconnect_client(client)
        .await
        .expect("it has to work for now");
}

#[transaction]
async fn connect_client(addr: &SocketAddr) -> Result<Client<TcpStream>, StompError> {
    let connect_frame = match addr.to_string().split_once(':') {
        Some((addr, port)) => {
            let port_header = Header::new("port", Some(port));
            Frame::stomp(addr, vec![port_header])
        }
        None => panic!("Couldn't parse the server address"),
    };
    let operations = vec![connect_frame];

    let connector = |addr: String| Box::pin(async { TcpStream::connect(addr).await });
    let processor = || Box::new(ClientProcessor::new()) as BoxedClientProcessor;
    let mut client = Client::new(Box::new(connector), processor);
    client.send(futures::stream::iter(operations)).await?;
    client.recv(1).await?;
    Ok(client)
}

#[transaction]
async fn disconnect_client(mut client: Client<TcpStream>) -> Result<(), StompError> {
    let operations = vec![Frame::disconnect::<&str>(Some("shutdown"))];

    client.send(futures::stream::iter(operations)).await?;
    client.recv(1).await?;
    Ok(())
}

#[transaction]
async fn subscribe_channel(client: &mut Client<TcpStream>) -> Result<(), StompError> {
    let operations = vec![Frame::subscribe("id-1", "/channel/1", None)];

    client.send(futures::stream::iter(operations)).await?;
    Ok(())
}

#[transaction]
async fn send_channel(client: &mut Client<TcpStream>) -> Result<(), StompError> {
    let operations = vec![Frame::send("/channel/1", vec![], &[])];

    client.send(futures::stream::iter(operations)).await?;
    println!("received: {:?}", client.recv(1).await?);
    Ok(())
}
