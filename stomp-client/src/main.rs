use std::env;
use std::fs::metadata;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::DefaultEditor;
use rustyline::Editor;
use stomp::BoxedClientProcessor;
use stomp::Client;
use stomp::ClientCommand;
use stomp::ClientCommandProcessor;
use stomp::Frame;
use stomp::Header;
use stomp::ServerCommand;
use stomp::State as StompState;
use stomp::StompError;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel as MpscChannel;
use tokio::sync::Mutex;

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

fn get_history_file() -> Option<PathBuf> {
    dirs::preference_dir()
        .and_then(|mut base| {
            base.push("stomp");
            // Note: Not create_dir_all(), because we don't want to create preference
            // dirs if they don't exist.
            if metadata(base.clone()).ok().is_none() {
                std::fs::create_dir(base.clone()).ok()?
            }
            Some(base)
        })
        .map(|mut base| {
            base.push("history.txt");
            base
        })
}

fn parse_headers(input: String) -> Result<Vec<Header>> {
    Ok(input
        .split(',')
        .filter_map(|pairs| {
            pairs.split_once(':').map(|(k, v)| {
                let value = if v.is_empty() { None } else { Some(v) };
                Header::new(k, value)
            })
        })
        .collect::<Vec<Header>>())
}

async fn process_line(line: String, my_txn_id: &Option<String>) -> Result<Frame> {
    if line.is_empty() {
        return Err(anyhow::anyhow!("empty line"));
    }
    let mut words = line
        .split_whitespace()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let cmd = words.remove(0);
    let frame_result = match ClientCommand::from_str(&cmd) {
        Ok(command) => match command {
            ClientCommand::Connect | ClientCommand::Stomp => {
                if !words.is_empty() {
                    match words[0].split_once(':') {
                        Some((addr, port)) => {
                            let port_header = Header::new("port", Some(port));
                            Frame::stomp(addr, vec![port_header])
                        }
                        None => return Err(anyhow::anyhow!("Couldn't parse the server address")),
                    }
                } else {
                    return Err(anyhow::anyhow!("stomp <address>"));
                }
            }
            ClientCommand::Disconnect => {
                if words.is_empty() {
                    Frame::disconnect::<&str>(None)
                } else {
                    Frame::disconnect(Some(&words[0]))
                }
            }
            ClientCommand::Subscribe => {
                if words.len() < 2 {
                    return Err(anyhow::anyhow!("subscribe <id> <destination> [ack mode]"));
                }
                if words.len() >= 3 {
                    Frame::subscribe(&words[0], &words[1], Some(&words[2]))
                } else {
                    Frame::subscribe(&words[0], &words[1], None)
                }
            }
            ClientCommand::Unsubscribe => {
                if words.is_empty() {
                    return Err(anyhow::anyhow!("unsubscribe <id>"));
                }
                Frame::unsubscribe(&words[0])
            }
            ClientCommand::Send => {
                if words.len() == 1 {
                    Frame::send(&words[0], vec![], &[])
                } else if words.len() == 2 {
                    // Assume the headers argument is ,: separated. e.g.
                    let hdrs = parse_headers(words[1].clone())?;
                    Frame::send(&words[0], hdrs, &[])
                } else if words.len() == 3 {
                    let hdrs = parse_headers(words[1].clone())?;
                    Frame::send(&words[0], hdrs, words[2].as_bytes())
                } else {
                    return Err(anyhow::anyhow!(
                        "send <destination> [additional headers] [body]"
                    ));
                }
            }
            ClientCommand::Begin => {
                let id = if words.is_empty() {
                    uuid::Uuid::new_v4()
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer())
                        .to_string()
                } else {
                    words[0].to_string()
                };
                Frame::begin(id)
            }
            ClientCommand::Commit => match my_txn_id {
                Some(txn_id) => Frame::commit(txn_id),
                None => return Err(anyhow::anyhow!("commit <id>")),
            },
            ClientCommand::Abort => match my_txn_id {
                Some(txn_id) => Frame::abort(txn_id),
                None => return Err(anyhow::anyhow!("abort <id>")),
            },
            ClientCommand::Ack => {
                if words.is_empty() {
                    return Err(anyhow::anyhow!("ack <id>"));
                }
                if words.len() == 1 {
                    Frame::ack(&words[0], None)
                } else if words.len() == 2 {
                    Frame::ack(&words[0], Some(&words[1]))
                } else {
                    return Err(anyhow::anyhow!("ack <id> [txn id]"));
                }
            }
            ClientCommand::Nack => {
                if words.is_empty() {
                    return Err(anyhow::anyhow!("nack <id>"));
                }
                if words.len() == 1 {
                    Frame::nack(&words[0], None)
                } else if words.len() == 2 {
                    Frame::nack(&words[0], Some(&words[1]))
                } else {
                    return Err(anyhow::anyhow!("nack <id> [txn id]"));
                }
            }
        },
        Err(e) => {
            return Err(anyhow::anyhow!("error {e}"));
        }
    };
    Ok(frame_result)
}

fn print_help() {
    println!("TODO: implement help");
}

#[tokio::main]
async fn main() -> Result<()> {
    let (line_tx, line_rx) = MpscChannel(1);

    let log_dir = match env::var("TMPDIR") {
        Ok(d) => d,
        Err(_e) => ".".to_string(),
    };

    let file_appender = tracing_appender::rolling::daily(log_dir, "stomp.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_writer(non_blocking)
        .init();

    let line_monitor = tokio::task::spawn_blocking(move || -> Result<()> {
        // `()` can be used when no completer is required
        let mut rl = DefaultEditor::new()?;
        if let Some(file_location) = get_history_file() {
            if let Err(e) = rl.load_history(&file_location) {
                println!("error loading history: {e}");
            }
        }
        println!("terminate with ctrl-c or ctrl-d");
        loop {
            tracing::debug!("about to readline");
            match rl.readline("stomp> ") {
                Ok(line) => {
                    // TODO Implement "pseudo" commands here. Intercept line processing for commands
                    // like "help", "exit"...
                    if line == "exit" {
                        return Err(anyhow::anyhow!("terminating"));
                    } else if line == "help" {
                        print_help();
                        continue;
                    }
                    rl.add_history_entry(line.as_str())?;
                    tracing::debug!("about to blocking send, capacity: {}", line_tx.capacity());
                    if line_tx.capacity() == 0 {
                        return Err(anyhow::anyhow!("other end terminated"));
                    }
                    line_tx.blocking_send(line)?;
                }
                Err(ReadlineError::Interrupted) => {
                    close_history(rl);
                    return Err(anyhow::anyhow!("interrupted"));
                }
                Err(ReadlineError::Eof) => {
                    close_history(rl);
                    return Err(anyhow::anyhow!("terminating"));
                }
                Err(err) => {
                    close_history(rl);
                    return Err(anyhow::anyhow!("error {err}"));
                }
            }
        }

        fn close_history(mut rl: Editor<(), FileHistory>) {
            if let Some(file_location) = get_history_file() {
                if let Err(e) = rl.save_history(&file_location) {
                    println!("error saving history: {e}");
                }
            }
        }
    });

    let shared_rx = Arc::new(Mutex::new(line_rx));
    let my_rx = shared_rx.clone();
    let shared_txn_id = Arc::new(Mutex::new(None));
    let my_txn_id = shared_txn_id.clone();

    let svc_func = |output_opt: Option<String>| async {
        if let Some(output) = output_opt {
            println!("{}", output);
        }
        let mut guard = my_rx.lock().await;
        match guard.recv().await {
            Some(line) => {
                if line.is_empty() {
                    return Ok(None);
                }
                let mut txn_guard = my_txn_id.lock().await;
                let mut frame = process_line(line, &txn_guard).await.map_err(|e| {
                    StompError::Parse(e.to_string().as_bytes().to_vec(), stomp::ErrorKind::Fail)
                })?;
                // Post process our frame to preserve transaction details
                if frame.is_client_begin() {
                    *txn_guard = frame.get_header("transaction");
                } else if frame.is_client_commit() || frame.is_client_abort() {
                    *txn_guard = None;
                } else if frame.is_client_send() && txn_guard.is_some() {
                    frame.upsert_header("transaction", txn_guard.clone().as_deref());
                }
                tracing::debug!("received a frame: {frame:?}");
                Ok(Some(frame))
            }
            None => Err(StompError::Disconnected(vec![])),
        }
    };

    let connector = |addr: String| async { TcpStream::connect(addr).await };
    let processor = || Box::new(ClientProcessor::new()) as BoxedClientProcessor;
    let mut client = Client::new(Box::new(connector), processor);
    client.serve(svc_func).await?;

    println!("Finished serving...");
    line_monitor.await?
}
