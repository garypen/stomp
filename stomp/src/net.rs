use std::collections::HashMap;
use std::pin::Pin;

use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use rust_fsm::StateMachine;
use std::future::Future;
use std::marker::PhantomData;
use tokio::io;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::task::JoinSet;

use crate::error::StompError;
use crate::parse_frame;
use crate::sm::Stomp;
use crate::sm::StompInput;
use crate::sm::StompOutput;
use crate::ClientCommand;
use crate::Command;
use crate::Frame;
use crate::ServerCommand;
use crate::StompState;
use crate::Subscription;

#[cfg(test)]
mod test;

/// A map of subscription names to Subscriptions
pub type SubscriptionMap = HashMap<String, Subscription>;

/// A processor object to be used by a [`Client`]
pub type BoxedClientProcessor = Box<dyn ClientCommandProcessor>;

/// A processor object to be used by a [`Server`]
pub type BoxedServerProcessor = Box<dyn ServerCommandProcessor>;

type ReadFrame = Result<Option<Frame>, StompError>;

type ActiveChannels<'a> = FuturesUnordered<Pin<Box<dyn Future<Output = ReadFrame> + Send + 'a>>>;

/// Implement for a Server to receive frame notifications
#[async_trait::async_trait]
pub trait ServerCommandProcessor: std::fmt::Debug + Send {
    async fn process_client_frame(
        &mut self,
        output: StompOutput,
        subscriptions: &mut SubscriptionMap,
        mut frame: Frame,
    ) -> Result<Vec<Frame>, StompError>;

    async fn process_server_frame(
        &mut self,
        state: &StompState,
        cmd: &ServerCommand,
        subscriptions: &mut SubscriptionMap,
        frame: Frame,
    ) -> Result<Vec<Frame>, StompError>;
}

/// Implement for a Client to receive frame notifications
#[async_trait::async_trait]
pub trait ClientCommandProcessor: std::fmt::Debug + Send {
    async fn process_server_frame(
        &mut self,
        state: &StompState,
        cmd: &ServerCommand,
        frame: Frame,
    ) -> Result<Vec<Frame>, StompError>;
}

/// Identify an async source for a Client
pub trait AsyncSource<Stream> {
    fn call(&self, args: String) -> BoxFuture<'static, io::Result<Stream>>;
}

impl<C, CFut, Stream> AsyncSource<Stream> for C
where
    C: Fn(String) -> CFut,
    CFut: Future<Output = io::Result<Stream>> + Send + 'static,
{
    fn call(&self, arg: String) -> BoxFuture<'static, io::Result<Stream>> {
        Box::pin(self(arg))
    }
}

/// Serves [`Client`] connections
pub struct Server<Stream> {
    set: JoinSet<Result<(), StompError>>,
    processor: Box<dyn Fn() -> BoxedServerProcessor + Send>,
    _phantom: PhantomData<Stream>,
}

struct Connection<Stream> {
    stream: BufWriter<Stream>,
    buffer: BytesMut,
    machine: StateMachine<Stomp>,
}

// TODO: Make it possible to hold multiple connections at a time
/// Provides a client connection to a STOMP [`Server`]
pub struct Client<Stream> {
    connector: Box<dyn AsyncSource<Stream> + Send>,
    connection: Option<Connection<Stream>>,
    processor: Box<dyn Fn() -> BoxedClientProcessor + Send>,
}

impl<Stream> Client<Stream>
where
    Stream: AsyncWrite + AsyncRead + Unpin + Send,
{
    /// Create a new Client.
    ///
    /// The client should provide both a connector and a processor. The connector is a way to
    /// provide a connection mechanism to a Client and the processor interacts with frames of
    /// data which may originate from a connected source or any connected servers.
    ///
    /// * `connector` - Provides a connection mechanism which can process an input address string.
    /// * `processor` - Implements [`ClientCommandProcessor`] trait.
    pub fn new<P>(connector: Box<dyn AsyncSource<Stream> + Send>, processor: P) -> Self
    where
        P: Fn() -> BoxedClientProcessor + Send + 'static,
    {
        Self {
            connector,
            connection: None,
            processor: Box::new(processor),
        }
    }

    /// Serve a connection to a [`Server`].
    ///
    /// The client will loop and act as a STOMP protocol client processor. Connecting to a server
    /// is required for other commands to be meaningful. It's the responsibility of the Client to
    /// make sure that meaningful frames are provided for processing and transmission to a
    /// connected [`Server`].
    ///
    /// * `source` - Input frames to be processed by the client when Serving connections.
    pub async fn serve<F, Fut>(&mut self, source: F) -> Result<(), StompError>
    where
        F: Fn(Option<String>) -> Fut,
        Fut: Future<Output = ReadFrame> + Send,
    {
        let mut my_processor = (self.processor)();

        // A DISCONNECT may request a receipt. On processing a disconnect, we force one more
        // loop to try and get a frame before we ditch the connection.
        let mut loop_for_receipt = true;

        // If we want to send some data back to our client (via the source), then we should update
        // this variable with the String we are transferring.
        let mut provide_to_client = None;

        // Note: Unlike the server loop, we generally want to keep this loop running. Especially
        // when not connected.  Most error conditions will result in continue, apart from
        // when the client disconnects.
        loop {
            let mut finished = false;
            // Do some special processing here based on whether or not we are connected to a server. If
            // we aren't connected, just process frames from the source provided by the client. If we
            // are connected, select across both the source and any frames we might read from our
            // connection.
            let frames = match &mut self.connection {
                Some(ref mut connection) => {
                    let frame_result = select! {
                        frame_result = (source)(provide_to_client.clone()) => {
                            tracing::trace!("got a frame from source: {frame_result:?}");
                            provide_to_client = None;
                            frame_result
                        },
                        frame_result = connection.read_frame() => {
                            tracing::trace!("got a frame from connection: {frame_result:?}");
                            frame_result
                        }
                    };
                    tracing::debug!("frame_result: {frame_result:?}");
                    // Many messages will result in sending frames back to the client. we
                    // process our frames here and we may end up with result frames to
                    // write back to the client. Other outcomes usually involve
                    // short-cutting this process by breaking or continuing our message
                    // processing loop. Most times there is only a single frame.
                    match frame_result {
                        Ok(fo) => match fo {
                            Some(frame) => {
                                let (frames, fin) =
                                    Self::process_frame(&mut my_processor, connection, frame).await;
                                finished = fin;
                                frames
                            }
                            None => {
                                continue;
                                /*
                                let output_opt = connection
                                    .machine
                                    .consume(&StompInput::Disconnect)
                                    .map_err(StompError::StateTransition)?;
                                if let Some(StompOutput::CloseConnection) = output_opt {
                                    finished = true;
                                    vec![]
                                } else {
                                    panic!("State Machine implementation error");
                                }
                                */
                            }
                        },
                        Err(e) => {
                            tracing::error!("connected error");
                            tracing::error!("error during frame reading: {e}");
                            match e {
                                StompError::Parse(bytes, _) => {
                                    // vec![Frame::error("Error reading client frame", vec![], &bytes)]
                                    provide_to_client = Some(format!(
                                        "client parsing error: {}",
                                        String::from_utf8_lossy(&bytes)
                                    ));
                                    vec![]
                                }
                                StompError::Disconnected(frames) => {
                                    let output_opt = connection
                                        .machine
                                        .consume(&StompInput::Disconnect)
                                        .map_err(StompError::StateTransition)?;
                                    if let Some(StompOutput::CloseConnection) = output_opt {
                                        frames
                                    } else {
                                        panic!("State Machine implementation error");
                                    }
                                }
                                _ => {
                                    tracing::debug!("FINISHED PROCESSING ERR FRAME: {e}");
                                    continue;
                                    /*
                                    // Transition State Machine to Unsuccessful
                                    let output_opt = connection
                                        .machine
                                        .consume(&StompInput::Disconnect)
                                        .map_err(StompError::StateTransition)?;
                                    if let Some(StompOutput::CloseConnection) = output_opt {
                                        finished = true;
                                        vec![Frame::error(
                                            "Error reading client frame",
                                            vec![],
                                            &[],
                                        )]
                                    } else {
                                        panic!("State Machine implementation error");
                                    }
                                    */
                                }
                            }
                        }
                    }
                }
                None => {
                    let frame_result = (source)(provide_to_client.clone()).await;
                    provide_to_client = None;
                    tracing::debug!("got a frame result: {frame_result:?}");
                    match frame_result {
                        Ok(fo) => match fo {
                            Some(frame) => {
                                // Only one type of frame is valid here, STOMP, so we need
                                // to figure out how to process that without a connection
                                match &frame.command {
                                    Command::ClientCommand(cc) => match cc {
                                        ClientCommand::Connect | ClientCommand::Stomp => {
                                            let server_addr = format!(
                                                "{}:{}",
                                                frame.get_header("host").expect("has a host"),
                                                frame.get_header("port").expect("has a port")
                                            );
                                            let stream = self
                                                .connector
                                                .call(server_addr)
                                                .await
                                                .map_err(StompError::IO)?;

                                            self.connection = Some(Connection::new(stream));
                                            let (frames, fin) = Self::process_frame(
                                                &mut my_processor,
                                                self.connection
                                                    .as_mut()
                                                    .expect("connection just created"),
                                                frame,
                                            )
                                            .await;
                                            finished = fin;
                                            frames
                                        }
                                        _ => {
                                            // Ignore other frames and continue
                                            // TODO: Maybe should have an error if being strict?
                                            provide_to_client = Some(format!(
                                                "unexpected frame: {:?}",
                                                frame.command
                                            ));
                                            continue;
                                        }
                                    },
                                    _ => {
                                        // Ignore other frames and continue
                                        // TODO: Maybe should have an error if being strict?
                                        continue;
                                    }
                                }
                            }
                            None => {
                                continue;
                            }
                        },
                        Err(e) => {
                            tracing::error!("source error");
                            tracing::error!("error during frame reading: {e}");
                            match e {
                                StompError::Parse(bytes, _) => {
                                    provide_to_client = Some(format!(
                                        "client parsing error: {}",
                                        String::from_utf8_lossy(&bytes)
                                    ));
                                    continue;
                                }
                                StompError::Disconnected(frames) => {
                                    finished = true;
                                    frames
                                }
                                _ => {
                                    // TODO: Maybe should have an error if being strict?
                                    vec![]
                                }
                            }
                        }
                    }
                }
            };
            // We have frames to write to our server
            if let Some(connection) = &mut self.connection {
                tracing::debug!(
                    "connection machine state before writing: {:?}",
                    connection.machine.state()
                );
                for frame in frames {
                    tracing::trace!("writing a frame: {frame:?}");
                    connection.write_frame(frame).await?;
                }
            }
            if self.connection.is_some()
                && matches!(
                    self.connection
                        .as_ref()
                        .expect("connection is some")
                        .machine
                        .state(),
                    &StompState::Disconnected
                )
            {
                if loop_for_receipt {
                    loop_for_receipt = false;
                } else {
                    loop_for_receipt = true;
                    self.connection = None;
                }
            }
            tracing::debug!(
                "at loop end and finished: {finished}, has connection: {}",
                self.connection.is_some()
            );
            if finished {
                break;
            }
        }
        tracing::debug!("client loop terminated normally");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connection.is_some()
            && matches!(
                self.connection
                    .as_ref()
                    .expect("connection is some")
                    .machine
                    .state(),
                &StompState::Connected
            )
    }

    async fn connect(
        &mut self,
        my_processor: &mut BoxedClientProcessor,
        frame: Frame,
    ) -> Result<Frame, StompError> {
        match &frame.command {
            Command::ClientCommand(cc) => match &cc {
                ClientCommand::Connect | ClientCommand::Stomp => {
                    let server_addr = format!(
                        "{}:{}",
                        frame.get_header("host").expect("has a host"),
                        frame.get_header("port").expect("has a port")
                    );
                    let stream = self
                        .connector
                        .call(server_addr)
                        .await
                        .map_err(StompError::IO)?;

                    self.connection = Some(Connection::new(stream));
                    let (mut frames, _fin) = Self::process_frame(
                        my_processor,
                        self.connection.as_mut().expect("connection just created"),
                        frame,
                    )
                    .await;
                    Ok(frames.pop().unwrap())
                }
                _ => Err(StompError::FrameProcessing(vec![frame])),
            },
            Command::ServerCommand(_) => Err(StompError::FrameProcessing(vec![frame])),
        }
    }

    pub async fn send<S>(&mut self, mut source: S) -> Result<(), StompError>
    where
        S: futures::Stream<Item = Frame> + Unpin,
    {
        let mut my_processor = (self.processor)();
        let mut tried_to_connect = false;

        while let Some(mut frame) = source.next().await {
            if !self.is_connected() {
                if !tried_to_connect {
                    tried_to_connect = true;
                    frame = self.connect(&mut my_processor, frame).await?;
                } else {
                    return Err(StompError::FrameProcessing(vec![frame]));
                }
            }
            tracing::trace!("writing a frame: {frame:?}");
            match &mut self.connection {
                Some(connection) => connection.write_frame(frame).await?,
                None => return Err(StompError::Disconnected(vec![frame])),
            }
        }
        Ok(())
    }

    /// Receive `limit` frames.
    ///
    /// If the [`Client`] is not connected, this function will immediately fail.
    ///
    /// Otherwise, it will try to read frames from the connection until either:
    ///  - the limit is reached
    ///  - the [`Client`] is disconnected
    ///
    /// * `limit` - The number of frames to be processed by the client before returning.
    pub async fn recv(&mut self, mut limit: usize) -> Result<Vec<Frame>, StompError> {
        match &mut self.connection {
            Some(connection) => {
                let mut result = Vec::with_capacity(limit);
                while let Some(frame) = connection.read_frame().await? {
                    result.push(frame);
                    limit -= 1;
                    if limit == 0 {
                        return Ok(result);
                    }
                }
                Ok(result)
            }
            None => Err(StompError::Disconnected(vec![])),
        }
    }

    async fn process_frame(
        processor: &mut BoxedClientProcessor,
        connection: &mut Connection<Stream>,
        frame: Frame,
    ) -> (Vec<Frame>, bool) {
        let mut finished = false;
        let cmd = frame.command.clone();
        let frames = match cmd {
            Command::ClientCommand(cc) => {
                let input: StompInput = cc.into();
                let output_opt = match connection.machine.consume(&input) {
                    Ok(output) => output,
                    Err(err) => {
                        tracing::error!("Could not process frame in state machine: {frame}, {err}");
                        return (vec![], false);
                    }
                };
                match output_opt {
                    Some(_output) => {
                        vec![frame]
                    }
                    None => {
                        vec![Frame::error("Unexpected frame received", vec![], &[])]
                    }
                }
            }
            Command::ServerCommand(sc) => {
                let state = connection.machine.state();
                match processor.process_server_frame(state, &sc, frame).await {
                    Ok(frames) => frames,
                    Err(err) => match err {
                        StompError::Disconnected(frames) => {
                            finished = true;
                            frames
                        }
                        StompError::FrameProcessing(frames) => frames,
                        _ => {
                            vec![]
                        }
                    },
                }
            }
        };
        tracing::debug!("processed a client frame: {frames:?}, {finished}");
        (frames, finished)
    }
}

impl<Stream: AsyncWrite + AsyncRead + Unpin + Send + 'static> Server<Stream> {
    pub fn new<F>(processor: F) -> Self
    where
        F: Fn() -> BoxedServerProcessor + Send + 'static,
    {
        Self {
            set: JoinSet::new(),
            processor: Box::new(processor),
            _phantom: PhantomData,
        }
    }

    async fn spawn(&mut self, stream: Stream) {
        let mut connection = Connection::new(stream);

        let mut subscriptions: SubscriptionMap = HashMap::new();
        let mut my_processor = (self.processor)();

        self.set.spawn(async move {
            loop {
                let finished;
                // We can either get frames as a:
                //  - subscription MESSAGE
                //  - new frame from a client
                let frame_result = {
                    let mut active_channels: ActiveChannels = FuturesUnordered::new();

                    active_channels.push(Box::pin(connection.read_frame()));
                    subscriptions.values_mut().for_each(|sub| {
                        let fut = Box::pin(async {
                            let frame = sub.rx.recv().await.map_err(StompError::Recv)?;
                            Ok(Some(frame))
                        });
                        active_channels.push(fut)
                    });
                    tracing::trace!("active_channels: {}", active_channels.len());
                    active_channels
                        .next()
                        .await
                        .ok_or(StompError::FrameProcessing(vec![]))?
                };
                // Many messages will result in sending frames back to the client. we
                // process our frames here and we may end up with result frames to
                // write back to the client. Other outcomes usually involve
                // short-cutting this process by breaking or continuing our message
                // processing loop. Most times there is only a single frame.
                let frames = match frame_result {
                    Ok(fo) => match fo {
                        Some(frame) => {
                            let (frames, fin) = Self::process_frame(
                                &mut my_processor,
                                &mut connection,
                                &mut subscriptions,
                                frame,
                            )
                            .await;
                            finished = fin;
                            frames
                        }
                        None => {
                            let output_opt = connection
                                .machine
                                .consume(&StompInput::Disconnect)
                                .map_err(StompError::StateTransition)?;
                            if let Some(StompOutput::CloseConnection) = output_opt {
                                finished = true;
                                vec![]
                            } else {
                                panic!("State Machine implementation error");
                            }
                        }
                    },
                    Err(e) => {
                        tracing::error!("error during frame reading: {e}");
                        match e {
                            StompError::Disconnected(frames) => {
                                // Transition State Machine to Unsuccessful
                                let output_opt = connection
                                    .machine
                                    .consume(&StompInput::Disconnect)
                                    .map_err(StompError::StateTransition)?;
                                if let Some(StompOutput::CloseConnection) = output_opt {
                                    finished = true;
                                    frames
                                } else {
                                    panic!("State Machine implementation error");
                                }
                            }
                            _ => {
                                // Ignore other errors, eventually we will disconnect
                                tracing::warn!("Ignoring an error: {e}");
                                continue;
                            }
                        }
                    }
                };

                tracing::trace!("writing frames to client: {frames:?}");
                // We have frames to write back to our client
                for frame in frames {
                    connection.write_frame(frame).await?;
                }
                if finished {
                    break;
                }
            }
            Ok(())
        });
    }

    async fn process_frame(
        processor: &mut Box<dyn ServerCommandProcessor>,
        connection: &mut Connection<Stream>,
        subscriptions: &mut SubscriptionMap,
        frame: Frame,
    ) -> (Vec<Frame>, bool) {
        let mut finished = false;
        let cmd = frame.command.clone();
        let frames = match cmd {
            Command::ClientCommand(cc) => {
                let input: StompInput = cc.into();
                let output_opt = match connection.machine.consume(&input) {
                    Ok(output) => output,
                    Err(err) => {
                        tracing::error!("Could not process frame in state machine: {frame}, {err}");
                        return (vec![], true);
                    }
                };
                match output_opt {
                    Some(output) => {
                        match processor
                            .process_client_frame(output, subscriptions, frame)
                            .await
                        {
                            Ok(frames) => frames,
                            Err(err) => match err {
                                StompError::Disconnected(frames) => {
                                    finished = true;
                                    frames
                                }
                                StompError::FrameProcessing(frames) => {
                                    finished = true;
                                    frames
                                }
                                _ => {
                                    finished = true;
                                    vec![]
                                }
                            },
                        }
                    }
                    None => {
                        finished = true;
                        vec![Frame::error("Unexpected frame received", vec![], &[])]
                    }
                }
            }
            Command::ServerCommand(sc) => {
                let state = connection.machine.state();
                match processor
                    .process_server_frame(state, &sc, subscriptions, frame)
                    .await
                {
                    Ok(frames) => frames,
                    Err(err) => match err {
                        StompError::Disconnected(frames) => {
                            finished = true;
                            frames
                        }
                        StompError::FrameProcessing(frames) => {
                            finished = true;
                            frames
                        }
                        _ => {
                            finished = true;
                            vec![]
                        }
                    },
                }
            }
        };
        (frames, finished)
    }
}

impl Server<TcpStream> {
    /// Serve connections to a provided  [`TcpListener`].
    ///
    /// The [`Server`] will continue to listen for connections using the provided listener and will
    /// spawn a new [`tokio::task::spawn`] for each new connection.
    ///
    /// * `listener` - listen to this address for new connections
    #[tracing::instrument(skip(self))]
    pub async fn serve_tcp(&mut self, listener: TcpListener) -> io::Result<()> {
        loop {
            tracing::trace!("connected: {}", self.set.len());

            select! {
                connect_pair_maybe = listener.accept() => {
                    tracing::trace!("accepted: {:?}", connect_pair_maybe);
                    let (stream, _) = connect_pair_maybe?;
                    self.spawn(stream).await;
                },
                Some(res) = self.set.join_next() => {
                    tracing::trace!("res done: {res:?}");
                }
            }
        }
    }

    /// Serve a single connection to the provided [`TcpListener`].
    ///
    /// The [`Server`] will listen for a single connection using the provided listener and will
    /// spawn a new [`tokio::task::spawn`] for that connection. It will wait for the spawned task to
    /// complete before returning.
    ///
    /// This is designed to simplify some testing scenarios where it's useful to only listen for a
    /// single connection and then terminate, rather than processing in a never-ending loop.
    ///
    /// * `listener` - listen to this address for new connections
    pub async fn serve_one_tcp(&mut self, listener: TcpListener) -> io::Result<()> {
        let (stream, _addr) = listener.accept().await?;
        self.spawn(stream).await;

        self.set
            .join_next()
            .await
            .ok_or(io::Error::new(io::ErrorKind::Other, "nothing found"))??
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl<Stream: AsyncWrite + AsyncRead + Unpin> Connection<Stream> {
    fn new(stream: Stream) -> Self {
        Self {
            buffer: BytesMut::with_capacity(1024),
            stream: BufWriter::new(stream),
            machine: StateMachine::new(),
        }
    }

    /// Shutdown the writer
    #[allow(unused)]
    async fn shutdown(self) -> io::Result<()> {
        self.stream.into_inner().shutdown().await
    }

    async fn read_frame(&mut self) -> ReadFrame {
        tracing::trace!("reading a frame");
        let mut retry = false;
        loop {
            if (self.buffer.is_empty() || retry)
                && self
                    .stream
                    .read_buf(&mut self.buffer)
                    .await
                    .map_err(StompError::IO)?
                    == 0
            {
                tracing::trace!("No data, return disconnected");
                return Err(StompError::Disconnected(vec![]));
            }
            tracing::debug!("about to parse: {:?}", self.buffer);
            match parse_frame(&self.buffer) {
                Ok((remaining, frame)) => {
                    tracing::debug!("new remaining: {:?}", remaining);
                    self.buffer = remaining.into();
                    return Ok(Some(frame));
                }
                Err(e) => match e {
                    nom::Err::Incomplete(needed) => {
                        tracing::debug!("incomplete frame, needs: {needed:?}");
                        retry = true;
                        continue;
                    }
                    nom::Err::Error(e) | nom::Err::Failure(e) => {
                        let pos = e.input.to_vec();
                        let code = e.code;
                        tracing::debug!("pos: {pos:?}, code: {code:?}");
                        return Err(StompError::Parse(pos, code));
                    }
                },
            }
        }
    }

    async fn write_frame(&mut self, frame: Frame) -> Result<(), StompError> {
        let payload = frame.to_string();
        tracing::trace!("writing frame payload: {payload}");
        self.stream
            .write_all(payload.as_bytes())
            .await
            .map_err(StompError::IO)?;
        self.stream.flush().await.map_err(StompError::IO)
    }
}
