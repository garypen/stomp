//! Provides a STOMP 1.2 Protocol - <https://stomp.github.io/stomp-specification-1.2.html> implementation.
//!
//! The library provides functionality to simplify implementing STOMP 1.2 compliant servers and
//! clients.
use std::fmt;

use indexmap::IndexMap;
use strum::Display;
use strum::EnumString;
use tokio::sync::broadcast::Receiver;

mod error;
mod net;
mod parser;
mod sm;

#[cfg(test)]
mod test;

pub use error::*;
pub use net::BoxedClientProcessor;
pub use net::BoxedServerProcessor;
pub use net::Client;
pub use net::ClientCommandProcessor;
pub use net::Server;
pub use net::ServerCommandProcessor;
pub use net::SubscriptionMap;
pub(crate) use parser::parse_frame;
pub use sm::StompOutput;
pub use sm::StompState;

/// ClientCommands
///
/// Commands that may be sent to a server by a client.
#[derive(Clone, Debug, Display, EnumString, PartialEq)]
#[strum(ascii_case_insensitive, serialize_all = "UPPERCASE")]
pub enum ClientCommand {
    Abort,
    Ack,
    Begin,
    Commit,
    Connect,
    Disconnect,
    Nack,
    Send,
    Stomp,
    Subscribe,
    Unsubscribe,
}

impl From<ClientCommand> for sm::StompInput {
    fn from(value: ClientCommand) -> Self {
        match &value {
            ClientCommand::Connect | ClientCommand::Stomp => sm::StompInput::Connect,
            ClientCommand::Disconnect => sm::StompInput::Disconnect,
            ClientCommand::Send => sm::StompInput::Send,
            ClientCommand::Subscribe => sm::StompInput::Subscribe,
            ClientCommand::Unsubscribe => sm::StompInput::Unsubscribe,
            ClientCommand::Ack => sm::StompInput::Ack,
            ClientCommand::Nack => sm::StompInput::Nack,
            ClientCommand::Begin => sm::StompInput::Begin,
            ClientCommand::Commit => sm::StompInput::Commit,
            ClientCommand::Abort => sm::StompInput::Abort,
        }
    }
}

/// ServerCommand
///
/// Commands that may be sent to a client by a server.
#[derive(Clone, Debug, Display, EnumString, PartialEq)]
#[strum(ascii_case_insensitive, serialize_all = "UPPERCASE")]
pub enum ServerCommand {
    Connected,
    Error,
    Message,
    Receipt,
}

/// Command
///
/// Union of all types of client and server commands.
#[derive(Clone, Debug, Display, PartialEq)]
pub enum Command {
    ClientCommand(ClientCommand),
    ServerCommand(ServerCommand),
}

/// Type of [`Subscription`] (as per the spec): auto, client, client-individual
///
/// See the specification for descriptions of how the various modes affect behaviour.
#[derive(Clone, Debug, Display, EnumString, PartialEq)]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum SubscriptionMode {
    Auto,
    Client,
    #[strum(serialize = "client-individual")]
    Individual,
}

/// A Subscription to a topic of interest
#[derive(Debug)]
pub struct Subscription {
    /// The id of the Subscription.
    pub id: String,
    pub mode: SubscriptionMode,
    registered: IndexMap<String, Frame>,
    rx: Receiver<Frame>,
}

/// A STOMP header.
///
/// Holds the Key and Value of a STOMP header.
///
/// * `key` - name of the header
/// * `value` - optional value of the header
#[derive(Clone, Debug, Default)]
pub struct Header {
    key: String,
    value: Option<String>,
}

/// A STOMP frame.
///
/// Holds the various details about a STOMP frame.
///
#[derive(Clone, Debug)]
pub struct Frame {
    command: Command,
    headers: Vec<Header>,
    /// The body of this frame.
    pub body: Vec<u8>,
}

impl Header {
    /// Create a STOMP header.
    ///
    /// Create a STOMP header for use in a [`Frame`].
    ///
    /// * `key` - name of the header
    /// * `value` - optional value of the header
    pub fn new<T: Into<String>>(key: T, value: Option<T>) -> Self {
        Self {
            key: key.into(),
            value: value.map(Into::into),
        }
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.key)?;
        match &self.value {
            Some(value) => writeln!(f, "{}", value)?,
            None => writeln!(f)?,
        };
        Ok(())
    }
}

impl Frame {
    fn new(command: Command, headers: Vec<Header>, body: Vec<u8>) -> Self {
        Frame {
            command,
            headers,
            body,
        }
    }

    fn without_body(command: Command, headers: Vec<Header>) -> Self {
        Frame {
            command,
            headers,
            body: vec![],
        }
    }

    fn valid(&self) -> bool {
        // Enforce header rules
        let header_valid = match &self.command {
            Command::ClientCommand(cc) => match cc {
                ClientCommand::Connect | ClientCommand::Stomp => {
                    let mut has_accept = false;
                    let mut has_host = false;
                    for header in &self.headers {
                        if header.key == "accept-version" {
                            has_accept = true;
                        }
                        if header.key == "host" {
                            has_host = true;
                        }
                    }
                    has_accept && has_host
                }
                ClientCommand::Send => {
                    let mut has_destination = false;
                    for header in &self.headers {
                        if header.key == "destination" {
                            has_destination = true;
                            break;
                        }
                    }
                    has_destination
                }
                ClientCommand::Subscribe => {
                    let mut has_destination = false;
                    let mut has_id = false;
                    for header in &self.headers {
                        if header.key == "destination" {
                            has_destination = true;
                        }
                        if header.key == "id" {
                            has_id = true;
                        }
                    }
                    has_destination && has_id
                }
                ClientCommand::Unsubscribe | ClientCommand::Ack | ClientCommand::Nack => {
                    let mut has_id = false;
                    for header in &self.headers {
                        if header.key == "id" {
                            has_id = true;
                            break;
                        }
                    }
                    has_id
                }
                ClientCommand::Begin | ClientCommand::Commit | ClientCommand::Abort => {
                    let mut has_transaction = false;
                    for header in &self.headers {
                        if header.key == "transaction" {
                            has_transaction = true;
                            break;
                        }
                    }
                    has_transaction
                }
                _ => true,
            },
            Command::ServerCommand(sc) => match sc {
                ServerCommand::Connected => {
                    let mut has_version = false;
                    for header in &self.headers {
                        if header.key == "version" {
                            has_version = true;
                            break;
                        }
                    }
                    has_version
                }
                ServerCommand::Message => {
                    let mut has_destination = false;
                    let mut has_message_id = false;
                    let mut has_subscription = false;
                    for header in &self.headers {
                        if header.key == "destination" {
                            has_destination = true;
                        }
                        if header.key == "message-id" {
                            has_message_id = true;
                        }
                        if header.key == "subscription" {
                            has_subscription = true;
                        }
                    }
                    has_destination && has_message_id && has_subscription
                }
                ServerCommand::Receipt => {
                    let mut has_receipt_id = false;
                    for header in &self.headers {
                        if header.key == "receipt-id" {
                            has_receipt_id = true;
                            break;
                        }
                    }
                    has_receipt_id
                }
                _ => true,
            },
        };
        if !header_valid {
            return false;
        }
        // Enforce body rules
        match &self.command {
            Command::ClientCommand(cc) => match cc {
                ClientCommand::Send => {}
                _ => {
                    if !self.body.is_empty() {
                        return false;
                    }
                }
            },
            Command::ServerCommand(sc) => match sc {
                ServerCommand::Message | ServerCommand::Error => {}
                _ => {
                    if !self.body.is_empty() {
                        return false;
                    }
                }
            },
        }
        true
    }

    // Construct various message frames
    // Client Frames

    /// Create an ABORT frame.
    ///
    /// Creates a STOMP ABORT frame to abort a transaction.
    ///
    /// * `transaction` - the REQUIRED transaction ID header.
    pub fn abort<T: Into<String>>(transaction: T) -> Self {
        let command = Command::ClientCommand(ClientCommand::Abort);
        let transaction = Header::new("transaction", Some(&transaction.into()));
        let headers = vec![transaction];
        Frame::without_body(command, headers)
    }

    /// Create an ACK frame.
    ///
    /// Creates an ACK frame to acknowledge receipt of a message.
    ///
    /// * `id` - the REQUIRED acknowledgement ID header.
    /// * `transaction_opt` - the OPTIONAL transaction ID header.
    pub fn ack<T: Into<String>>(id: T, transaction_opt: Option<T>) -> Self {
        let command = Command::ClientCommand(ClientCommand::Ack);
        let id = Header::new("id", Some(&id.into()));
        let mut headers = vec![id];
        if let Some(transaction) = transaction_opt {
            headers.push(Header::new("transaction", Some(&transaction.into())));
        }
        Frame::without_body(command, headers)
    }

    /// Create a BEGIN frame.
    ///
    /// Creates a BEGIN frame to begin a transaction.
    ///
    /// * `transaction` - the REQUIRED transaction ID header.
    pub fn begin<T: Into<String>>(transaction: T) -> Self {
        let command = Command::ClientCommand(ClientCommand::Begin);
        let transaction = Header::new("transaction", Some(&transaction.into()));
        let headers = vec![transaction];
        Frame::without_body(command, headers)
    }

    /// Create a COMMIT frame.
    ///
    /// Creates a COMMIT frame to commit a transaction.
    ///
    /// * `transaction` - the REQUIRED transaction ID header.
    pub fn commit<T: Into<String>>(transaction: T) -> Self {
        let command = Command::ClientCommand(ClientCommand::Commit);
        let transaction = Header::new("transaction", Some(&transaction.into()));
        let headers = vec![transaction];
        Frame::without_body(command, headers)
    }

    /// Create a STOMP frame.
    ///
    /// Creates a STOMP frame to connect to a server.
    ///
    /// * `host` - the REQUIRED host header.
    /// * `additional` - the OPTIONAL additional headers.
    pub fn stomp<T: Into<String>>(host: T, additional: Vec<Header>) -> Self {
        let command = Command::ClientCommand(ClientCommand::Stomp);
        let accept = Header::new("accept-version", Some("1.2"));
        let host = Header::new("host", Some(&host.into()));
        let mut headers = vec![host, accept];
        headers.extend(additional);
        headers.retain(|hdr| {
            [
                "accept-version",
                "host",
                "login",
                "passcode",
                "heart-beat",
                "port",
            ]
            .contains(&hdr.key.as_str())
        });
        Frame::without_body(command, headers)
    }

    /// Create a DISCONNECT frame.
    ///
    /// Create a DISCONNECT frame to disconnect from a server.
    ///
    /// * `receipt_opt` - the OPTIONAL receipt header.
    pub fn disconnect<T: Into<String>>(receipt_opt: Option<T>) -> Self {
        let command = Command::ClientCommand(ClientCommand::Disconnect);
        let mut headers = vec![];
        if let Some(receipt) = receipt_opt {
            headers.push(Header::new("receipt", Some(&receipt.into())));
        }
        Frame::without_body(command, headers)
    }

    /// Create a NACK frame
    ///
    /// Creates a NACK frame to NOT acknowledge receipt of a message.
    ///
    /// * `id` - the REQUIRED acknowledgement ID header.
    /// * `transaction_opt` - the OPTIONAL transaction ID header.
    pub fn nack<T: Into<String>>(id: T, transaction_opt: Option<T>) -> Self {
        let command = Command::ClientCommand(ClientCommand::Nack);
        let id = Header::new("id", Some(&id.into()));
        let mut headers = vec![id];
        if let Some(transaction) = transaction_opt {
            headers.push(Header::new("transaction", Some(&transaction.into())));
        }
        Frame::without_body(command, headers)
    }

    /// Create a SEND frame.
    ///
    /// Creates a SEND frame to send a message to a destination.
    ///
    /// * `destination` - the REQUIRED destination header.
    /// * `additional` - the OPTIONAL additional headers.
    /// * `body` - the REQUIRED body.
    pub fn send<T: Into<String>>(destination: T, additional: Vec<Header>, body: &[u8]) -> Self {
        let command = Command::ClientCommand(ClientCommand::Send);
        let destination = Header::new("destination", Some(&destination.into()));
        let mut headers = vec![destination];
        headers.extend(additional);
        Frame::new(command, headers, body.to_vec())
    }

    /// Create a SUBSCRIBE frame
    ///
    /// Creates a SUBSCRIBE frame to subscribe to a destination.
    ///
    /// * `id` - the REQUIRED ID header.
    /// * `destination` - the REQUIRED destination header.
    /// * `ack_opt` - the OPTIONAL ack header.
    pub fn subscribe<T: Into<String>>(id: T, destination: T, ack_opt: Option<T>) -> Self {
        let command = Command::ClientCommand(ClientCommand::Subscribe);
        let id = Header::new("id", Some(&id.into()));
        let destination = Header::new("destination", Some(&destination.into()));
        let mut headers = vec![id, destination];
        if let Some(ack) = ack_opt {
            headers.push(Header::new("ack", Some(&ack.into())));
        }
        Frame::without_body(command, headers)
    }

    /// Create an UNSUBSCRIBE frame
    ///
    /// Creates an UNSUBSCRIBE frame to unsubscribe from a destination.
    ///
    /// * `id` - the REQUIRED ID header.
    pub fn unsubscribe<T: Into<String>>(id: T) -> Self {
        let command = Command::ClientCommand(ClientCommand::Unsubscribe);
        let id = Header::new("id", Some(&id.into()));
        let headers = vec![id];
        Frame::without_body(command, headers)
    }

    // Server Frames

    /// Create a CONNECTED frame
    ///
    /// Creates a CONNECTED frame to notify a [`Client`] that a previous CONNECT attempt succeeded.
    ///
    /// * `heart_beat_opt` - the OPTIONAL heart-beat header.
    pub fn connected<T: Into<String>>(heart_beat_opt: Option<T>) -> Self {
        let command = Command::ServerCommand(ServerCommand::Connected);
        let version = Header::new("version", Some("1.2"));
        let session = Header::new(
            "session",
            Some(
                uuid::Uuid::new_v4()
                    .hyphenated()
                    .encode_lower(&mut uuid::Uuid::encode_buffer()),
            ),
        );
        let server_str = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        let server = Header::new("server", Some(&server_str));
        let mut headers = vec![version, session, server];
        if let Some(heart_beat) = heart_beat_opt {
            headers.push(Header::new("heart-beat", Some(&heart_beat.into())));
        }
        Frame::without_body(command, headers)
    }

    /// Create an ERROR frame.
    ///
    /// Creates an Error frame to let a [`Client`] know that message processing failed. It is the
    /// responsibility of the client to decipher exactly which preceding message the error refers
    /// to.
    ///
    /// * `message` - The REQUIRED message header.
    /// * `additional` - the OPTIONAL additional headers.
    /// * `body` - the REQUIRED body.
    pub fn error<T: Into<String>>(message: T, additional: Vec<Header>, body: &[u8]) -> Self {
        let command = Command::ServerCommand(ServerCommand::Error);
        let message = Header::new("message", Some(&message.into()));
        let mut headers = vec![message];
        headers.extend(additional);
        Frame::new(command, headers, body.to_vec())
    }

    /// Create a MESSAGE frame.
    ///
    /// Creates a MESSAGE frame to convey a message to a [`Client`].
    ///
    /// * `destination` - the REQUIRED destination header.
    /// * `subscription_opt` - the OPTIONAL subscription header.
    /// * `additional` - the OPTIONAL additional headers.
    /// * `body` - the REQUIRED body.
    pub fn message<T: Into<String>>(
        destination: T,
        subscription_opt: Option<T>,
        additional: Vec<Header>,
        body: &[u8],
    ) -> Self {
        let command = Command::ServerCommand(ServerCommand::Message);
        let destination_hdr = Header::new("destination", Some(&destination.into()));
        let msg_uuid = uuid::Uuid::new_v4()
            .hyphenated()
            .encode_lower(&mut uuid::Uuid::encode_buffer())
            .to_string();
        let message_id = Header::new("message-id", Some(&msg_uuid));
        let mut headers = vec![destination_hdr, message_id];
        if let Some(subscription) = subscription_opt {
            headers.push(Header::new("subscription", Some(&subscription.into())));
        }
        headers.extend(additional);
        Frame::new(command, headers, body.to_vec())
    }

    /// Create a RECEIPT frame.
    ///
    /// Creates a RECEIPT frame to notify a [`Client`] that a previous message was received and
    /// processed.
    ///
    /// * `receipt_id` - the REQUIRED receipt-id.
    pub fn receipt<T: Into<String>>(receipt_id: T) -> Self {
        let command = Command::ServerCommand(ServerCommand::Receipt);
        let receipt_id = Header::new("receipt-id", Some(&receipt_id.into()));
        let headers = vec![receipt_id];
        Frame::without_body(command, headers)
    }

    /// Creates a MESSAGE frame from a SEND frame.
    ///
    /// Creates a MESSAGE frame to convey a message to a [`Client`].
    ///
    /// * `send` - the source END frame.
    pub fn message_from_send(send: Frame) -> Result<Frame, StompError> {
        if send.is_client_send() {
            let destination = send
                .get_header("destination")
                .ok_or(StompError::FrameProcessing(vec![send.clone()]))?;
            let subscription_opt = send.get_header("subscription");

            // if let Some(subscription) = subscription_opt {
            Ok(Self::message(
                destination,
                subscription_opt,
                send.headers,
                &send.body,
            ))
        } else {
            Err(StompError::FrameProcessing(vec![send]))
        }
    }

    /// Is the frame a [`ClientCommand::Begin`] command?
    pub fn is_client_begin(&self) -> bool {
        matches!(&self.command, Command::ClientCommand(cmd) if matches!(cmd, ClientCommand::Begin))
    }

    /// Is the frame a [`ClientCommand::Commit`] command?
    pub fn is_client_commit(&self) -> bool {
        matches!(&self.command, Command::ClientCommand(cmd) if matches!(cmd, ClientCommand::Commit))
    }

    /// Is the frame a [`ClientCommand::Stomp`] command?
    pub fn is_client_stomp(&self) -> bool {
        matches!(&self.command, Command::ClientCommand(cmd) if matches!(cmd, ClientCommand::Stomp) || matches!(cmd, ClientCommand::Connect) )
    }

    /// Is the frame a [`ClientCommand::Abort`] command?
    pub fn is_client_abort(&self) -> bool {
        matches!(&self.command, Command::ClientCommand(cmd) if matches!(cmd, ClientCommand::Abort))
    }

    /// Is the frame a [`ClientCommand::Send`] command?
    pub fn is_client_send(&self) -> bool {
        matches!(&self.command, Command::ClientCommand(cmd) if matches!(cmd, ClientCommand::Send))
    }

    /// Is the frame a [`ServerCommand::Message`] command?
    pub fn is_server_message(&self) -> bool {
        matches!(&self.command, Command::ServerCommand(cmd) if matches!(cmd, ServerCommand::Message))
    }

    /// Does the frame contain all the specified header keys?
    ///
    /// Search the header keys for the supplied keys. If all of them are present, return True, else
    /// False.
    ///
    /// * `keys` - keys to search for.
    pub fn has_keys(&self, keys: &[&str]) -> bool {
        keys.iter().all(|key| {
            self.headers
                .iter()
                .map(|header| &header.key)
                .any(|k| k == key)
        })
    }

    /// Get the value of the [`Header`] with a matching key.
    ///
    /// Returns a clone of the header value or None if the key isn't found.
    ///
    /// * `key` - the key to search for.
    pub fn get_header(&self, key: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|header| header.key == key)
            .map(|header| header.value.clone())?
    }

    /// Append the supplied [`Header`] to the frame.
    ///
    /// Note: this doesn't check if the header is already present. It is legitimate within the
    /// STOMP protocol to have multiple headers with the same key.
    ///
    /// * `header` - the header to append.
    pub fn append_header(&mut self, header: Header) {
        self.headers.push(header);
    }

    /// Update any headers which match the supplied key to the supplied value.
    ///
    /// Since there may be multiple headers with the same key, this will update all of the matching
    /// headers with the supplied value. Returns true if any headers are modified, false if not.
    ///
    /// * `key` - the key to search for.
    /// * `value` - the new header value.
    pub fn update_header<T: Into<String> + Clone>(&mut self, key: T, value: Option<T>) -> bool {
        let mut modified = false;
        let search_key = key.into();
        self.headers.iter_mut().for_each(|header| {
            if header.key == search_key {
                modified = true;
                header.value = value.clone().map(|v| v.into())
            }
        });
        modified
    }

    /// Updates any headers which match the supplied key to the supplied value. If no headers are
    /// found, then a new header is inserted.
    ///
    /// * `key` - the key to search for.
    /// * `value` - the new header value.
    pub fn upsert_header<T: Into<String> + Clone>(&mut self, key: T, value: Option<T>) {
        if !self.update_header(key.clone(), value.clone()) {
            let hdr = Header::new(key, value);
            self.append_header(hdr);
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.command {
            Command::ClientCommand(cmd) => writeln!(f, "{}", cmd)?,
            Command::ServerCommand(cmd) => writeln!(f, "{}", cmd)?,
        };
        for header in &self.headers {
            write!(f, "{}", header)?;
        }
        writeln!(f)?;
        match std::str::from_utf8(&self.body) {
            Ok(s) => write!(f, "{}", s)?,
            Err(err) => panic!("body contains non-utf8 data: {err}"),
        }
        writeln!(f, "\u{00}")
    }
}

impl Subscription {
    /// Create a new Subscription.
    ///
    /// Use the provided ID, [mode](`SubscriptionMode`) and [`Receiver`] to create a new subscription for a client.
    ///
    /// * `id` - The subscription ID as required by the STOMP specification.
    /// * `mode` - The STOMP specification subscription mode.
    /// * `rx` - A broadcast receiver on which subscription updates will be transmitted.
    pub fn new(id: String, mode: SubscriptionMode, rx: Receiver<Frame>) -> Self {
        Self {
            id,
            mode,
            registered: IndexMap::new(),
            rx,
        }
    }

    /// Register a Message frame for acknowledgement processing.
    ///
    /// All Message frames are processed to determine if they need to be registered for
    /// acknowlegements/nacknowledgements. Frames are only registered If the [mode](`SubscriptionMode) of the
    /// Subscription is not [`SubscriptionMode::Auto`].
    ///
    /// * `frame` - The frame we are to register.
    pub fn register(&mut self, mut frame: Frame) -> Result<Vec<Frame>, StompError> {
        if !frame.is_server_message() {
            return Err(StompError::FrameProcessing(vec![frame]));
        }
        // Behaviour is controlled by the type of subscription
        match self.mode {
            SubscriptionMode::Auto => (),
            SubscriptionMode::Client | SubscriptionMode::Individual => {
                // Create an ACK header which can be easily broken down to find during ACK/NACK
                // processing
                let ack_hdr = format!(
                    "{}|{}",
                    self.id,
                    frame
                        .get_header("message-id")
                        .expect("Message frame must have a message-id")
                );
                frame.upsert_header("ack", Some(&ack_hdr));
                if self.registered.len() == 1024 {
                    self.registered.shift_remove_index(0);
                }
                self.registered.insert(ack_hdr, frame.clone());
            }
        }
        Ok(vec![frame])
    }

    /// ACKnowledge that an id has been processed.
    ///
    /// Update the registration.
    ///
    /// * `id` - ACK id.
    pub fn acknowledge(&mut self, id: &str) -> Result<(), StompError> {
        match self.mode {
            SubscriptionMode::Auto => Ok(()),
            SubscriptionMode::Client => {
                let index_opt = self.registered.get_index_of(id);
                match index_opt {
                    Some(index) => {
                        self.registered = self.registered.split_off(index + 1);
                        Ok(())
                    }
                    None => Err(StompError::FrameProcessing(vec![])),
                }
            }
            SubscriptionMode::Individual => match self.registered.shift_remove(id) {
                Some(_value) => Ok(()),
                None => Err(StompError::FrameProcessing(vec![])),
            },
        }
    }

    /// NACKnowledge that an id has been processed.
    ///
    /// Update the registration.
    ///
    /// * `id` - NACK id.
    pub fn nacknowledge(&mut self, id: &str) -> Result<Vec<Frame>, StompError> {
        match self.mode {
            SubscriptionMode::Auto => Ok(vec![]),
            SubscriptionMode::Client => {
                let index_opt = self.registered.get_index_of(id);
                match index_opt {
                    Some(index) => {
                        let frames = self
                            .registered
                            .get_range(0..=index)
                            .map_or(vec![], |slice| {
                                slice.values().cloned().collect::<Vec<Frame>>()
                            });
                        Ok(frames)
                    }
                    None => Err(StompError::FrameProcessing(vec![])),
                }
            }
            SubscriptionMode::Individual => {
                let frame_opt = self.registered.get(id);
                match frame_opt {
                    Some(frame) => Ok(vec![frame.clone()]),
                    None => Err(StompError::FrameProcessing(vec![])),
                }
            }
        }
    }
}
