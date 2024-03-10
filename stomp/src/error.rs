use crate::Frame;
pub use nom::error::ErrorKind;
use rust_fsm::TransitionImpossibleError;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::SendError;

/// Errors that may arise during processing
#[derive(Error, Debug)]
pub enum StompError {
    /// IO error
    #[error("io error")]
    IO(std::io::Error),

    /// Parsing error
    #[error("parsing error")]
    Parse(Vec<u8>, ErrorKind),

    /// STOMP protocol state transition error
    #[error("state transition error")]
    StateTransition(TransitionImpossibleError),

    /// Channel receive error
    #[error("channel receive error")]
    Recv(RecvError),

    /// Channel send error
    #[error("channel send error")]
    Send(SendError<Frame>),

    /// Frame processing error
    #[error("frame processing error")]
    FrameProcessing(Vec<Frame>),

    /// Client is disconnected error
    #[error("client disconnected")]
    Disconnected(Vec<Frame>),
}
