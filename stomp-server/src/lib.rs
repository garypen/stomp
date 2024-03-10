use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use stomp::Frame;
use stomp::ServerCommand;
use stomp::ServerCommandProcessor;
use stomp::StompError;
use stomp::StompOutput;
use stomp::StompState;
use stomp::Subscription;
use stomp::SubscriptionMap;
use stomp::SubscriptionMode;
use tokio::sync::broadcast::channel;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;

pub type Channels = Arc<RwLock<HashMap<String, (Sender<Frame>, Receiver<Frame>)>>>;
pub type Transactions = Arc<RwLock<HashMap<String, Vec<Frame>>>>;

#[derive(Debug, Default)]
pub struct ServerProcessor {
    transactions: Transactions,
    channels: Channels,
}

impl ServerProcessor {
    pub fn new(transactions: Transactions, channels: Channels) -> Self {
        Self {
            transactions,
            channels,
        }
    }

    async fn process_destination(
        &mut self,
        subscriptions: &mut HashMap<String, Subscription>,
        mut frame: Frame,
    ) -> Result<Vec<Frame>, StompError> {
        if let Some(key) = frame.get_header("destination") {
            if let Some(subscription) = subscriptions.get_mut(&key) {
                frame.upsert_header("subscription", Some(subscription.id.as_ref()));
                if let Some(transaction) = frame.get_header("transaction") {
                    let mut guard = self.transactions.write().await;
                    match guard.get_mut(&transaction) {
                        Some(pending) => {
                            pending.push(frame);
                            Ok(vec![])
                        }
                        None => {
                            let frames = subscription.register(frame)?;
                            Ok(frames)
                        }
                    }
                } else {
                    // It's possible to receive a message from a different client whilst we are in a
                    // transaction. If we do, it won't have a transaction header, so just
                    // process it normally.
                    let frames = subscription.register(frame)?;
                    Ok(frames)
                }
            } else {
                Ok(vec![])
            }
        } else {
            let frames = vec![Frame::error("Destination header is missing", vec![], &[])];
            Err(StompError::FrameProcessing(frames))
        }
    }
}

#[async_trait::async_trait]
impl ServerCommandProcessor for ServerProcessor {
    async fn process_client_frame(
        &mut self,
        output: StompOutput,
        subscriptions: &mut SubscriptionMap,
        mut frame: Frame,
    ) -> Result<Vec<Frame>, StompError> {
        match output {
            StompOutput::SendConnected => {
                //TODO Implement heart-beat at
                //some point?
                Ok(vec![Frame::connected::<String>(None)])
            }
            StompOutput::CloseConnection => {
                let mut frames = vec![];
                if let Some(receipt_id) = frame.get_header("receipt") {
                    frames.push(Frame::receipt(receipt_id));
                }
                Err(StompError::Disconnected(frames))
            }
            StompOutput::AddSubscriber => {
                let key = frame
                    .get_header("destination")
                    .expect("must have destination");
                let id = frame.get_header("id").expect("must have id");
                let mode_opt = match frame.get_header("ack") {
                    Some(mode_s) => SubscriptionMode::from_str(&mode_s).ok(),
                    None => Some(SubscriptionMode::Auto),
                };
                if let Some(mode) = mode_opt {
                    let mut guard = self.channels.write().await;
                    let (tx, _rx) = guard.entry(key.clone()).or_insert(channel(2));
                    let subscription = Subscription::new(id, mode, tx.subscribe());
                    subscriptions.insert(key, subscription);
                    let mut frames = vec![];
                    if let Some(receipt_id) = frame.get_header("receipt") {
                        frames.push(Frame::receipt(receipt_id));
                    }
                    Ok(frames)
                } else {
                    let frames = vec![Frame::error("ACK mode not recognised", vec![], &[])];
                    Err(StompError::FrameProcessing(frames))
                }
            }
            StompOutput::RemoveSubscriber => {
                // Unsubscribing is slow. We key subscriptions by dest, because that's fast for
                // message distribution. However, unsubscribe is by ID, so we need to find an ID
                // and then remove. I note that other clients also provide a destination and it
                // makes me wonder if we could use that as an optimisation, but not doing that for
                // now.
                tracing::debug!("unsubscribing: {frame:?}");

                let id = frame.get_header("id").expect("must have id");
                let key_opt = subscriptions
                    .iter()
                    .find(|(_key, value)| value.id == id)
                    .map(|(key, _value)| key.clone());
                let mut frames = vec![];
                match key_opt {
                    Some(key) => {
                        let mut guard = self.channels.write().await;
                        let count_opt = guard.get(&key).map(|channel| channel.0.receiver_count());
                        if let Some(count) = count_opt {
                            // 2 is the right amount. It represents the original rx
                            // plus this one about to unsubscribe. <= is defensive...
                            if count <= 2 {
                                guard.remove(&key);
                            }
                        }
                        subscriptions.remove(&key);
                        if let Some(receipt_id) = frame.get_header("receipt") {
                            frames.push(Frame::receipt(receipt_id));
                        }
                    }
                    None => {
                        frames.push(Frame::error(
                            "not subscribed",
                            vec![],
                            frame.to_string().as_bytes(),
                        ));
                    }
                }
                Ok(frames)
            }
            StompOutput::StoreMessage => {
                tracing::debug!("storing frame: {frame:?}");
                // TODO: Think about if we need to manipulate headers here
                let key = frame
                    .get_header("destination")
                    .expect("must have destination");
                {
                    let subscription_opt = subscriptions.get(&key);
                    frame.upsert_header("id", subscription_opt.map(|x| x.id.as_str()));
                }
                let receipt_opt = frame.get_header("receipt");
                let msg = Frame::message_from_send(frame)?;
                if let Some(pair) = self.channels.read().await.get(&key) {
                    pair.0.send(msg).map_err(StompError::Send)?;
                }
                let mut frames = vec![];
                if let Some(receipt_id) = receipt_opt {
                    frames.push(Frame::receipt(receipt_id));
                }
                Ok(frames)
            }
            StompOutput::AckMessage => {
                // Acknowledging is slow. We key subscriptions by dest, because that's fast for
                // message distribution. However, ACK is by ID, so we need to find an ID
                // and then process.
                tracing::debug!("acknowledging: {frame:?}");
                match frame.get_header("id") {
                    Some(id_hdr) => {
                        let id = match id_hdr.split_once('|') {
                            Some((id, _)) => id,
                            None => {
                                let frames =
                                    vec![Frame::error("ACK frame invalid id", vec![], &[])];
                                return Err(StompError::FrameProcessing(frames));
                            }
                        };
                        let subscription_opt =
                            subscriptions.values_mut().find(|value| value.id == id);
                        if let Some(subscription) = subscription_opt {
                            subscription.acknowledge(&id_hdr)?;
                            Ok(vec![])
                        } else {
                            let frames =
                                vec![Frame::error("Unexpected ACK frame received", vec![], &[])];
                            Err(StompError::FrameProcessing(frames))
                        }
                    }
                    None => {
                        let frames = vec![Frame::error("ACK frame missing id", vec![], &[])];
                        Err(StompError::FrameProcessing(frames))
                    }
                }
            }
            StompOutput::NackMessage => {
                // Nacknowledging is slow. We key subscriptions by dest, because that's fast for
                // message distribution. However, NACK is by ID, so we need to find an ID
                // and then process.
                tracing::debug!("nacknowledging: {frame:?}");
                match frame.get_header("id") {
                    Some(id_hdr) => {
                        let id = match id_hdr.split_once('|') {
                            Some((id, _)) => id,
                            None => {
                                let frames =
                                    vec![Frame::error("NACK frame invalid id", vec![], &[])];
                                return Err(StompError::FrameProcessing(frames));
                            }
                        };
                        let subscription_opt =
                            subscriptions.values_mut().find(|value| value.id == id);
                        if let Some(subscription) = subscription_opt {
                            subscription.nacknowledge(&id_hdr)
                        } else {
                            let frames =
                                vec![Frame::error("Unexpected NACK frame received", vec![], &[])];
                            Err(StompError::FrameProcessing(frames))
                        }
                    }
                    None => {
                        let frames = vec![Frame::error("NACK frame missing id", vec![], &[])];
                        Err(StompError::FrameProcessing(frames))
                    }
                }
            }
            StompOutput::AbortTransaction => {
                if let Some(transaction) = frame.get_header("transaction") {
                    let mut guard = self.transactions.write().await;
                    guard.remove(&transaction);
                    Ok(vec![])
                } else {
                    let frames = vec![Frame::error("ABORT frame missing transaction", vec![], &[])];
                    Err(StompError::FrameProcessing(frames))
                }
            }
            StompOutput::BeginTransaction => {
                if let Some(transaction) = frame.get_header("transaction") {
                    let mut guard = self.transactions.write().await;
                    guard.insert(transaction, vec![]);
                    Ok(vec![])
                } else {
                    let frames = vec![Frame::error("BEGIN frame missing transaction", vec![], &[])];
                    Err(StompError::FrameProcessing(frames))
                }
            }
            StompOutput::CommitTransaction => {
                if let Some(transaction) = frame.get_header("transaction") {
                    let mut guard = self.transactions.write().await;
                    if let Some(frames) = guard.remove(&transaction) {
                        for frame in frames {
                            if let Some(dest_name) = frame.get_header("destination") {
                                if let Some(destination) =
                                    self.channels.read().await.get(&dest_name)
                                {
                                    destination.0.send(frame).map_err(StompError::Send)?;
                                }
                            }
                        }
                        Ok(vec![])
                    } else {
                        Ok(vec![])
                    }
                } else {
                    let frames = vec![Frame::error(
                        "COMMIT frame missing transaction",
                        vec![],
                        &[],
                    )];
                    Err(StompError::FrameProcessing(frames))
                }
            }
        }
    }

    async fn process_server_frame(
        &mut self,
        state: &StompState,
        cmd: &ServerCommand,
        subscriptions: &mut SubscriptionMap,
        frame: Frame,
    ) -> Result<Vec<Frame>, StompError> {
        match state {
            StompState::Connected => match cmd {
                ServerCommand::Connected => Ok(vec![frame]),
                ServerCommand::Message => {
                    if let Some(transaction) = frame.get_header("transaction") {
                        let guard = self.transactions.read().await;
                        if guard.get(&transaction).is_some() {
                            return Ok(vec![]);
                        }
                    }
                    self.process_destination(subscriptions, frame).await
                }
                ServerCommand::Error => Err(StompError::FrameProcessing(vec![frame])),
                _ => {
                    let frames = vec![Frame::error("Unrecognised ServerCommand", vec![], &[])];
                    Err(StompError::FrameProcessing(frames))
                }
            },
            StompState::Disconnected => {
                let frames = vec![Frame::error("Unrecognised ServerCommand", vec![], &[])];
                Err(StompError::FrameProcessing(frames))
            }
            StompState::Transaction => match cmd {
                ServerCommand::Message => self.process_destination(subscriptions, frame).await,
                _ => {
                    let frames = vec![Frame::error(
                        "Unrecognised  ServerCommand in transaction",
                        vec![],
                        &[],
                    )];
                    Err(StompError::FrameProcessing(frames))
                }
            },
        }
    }
}
