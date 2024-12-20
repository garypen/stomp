use rust_fsm::*;

state_machine! {
    #[derive(Debug)]
    pub stomp(Disconnected)

    Disconnected => {
        Connect => Connected [SendConnected],
        Disconnect => Disconnected [CloseConnection]
    },

    Connected => {
        Send => Connected [StoreMessage],
        Subscribe => Connected [AddSubscriber],
        Unsubscribe => Connected [RemoveSubscriber],
        Disconnect => Disconnected [CloseConnection],
        Ack => Connected [AckMessage],
        Nack => Connected [NackMessage],
        Begin => Transaction [BeginTransaction]
    },

    Transaction => {
        Send => Transaction [StoreMessage],
        Subscribe => Transaction [AddSubscriber],
        Unsubscribe => Transaction [RemoveSubscriber],
        Disconnect => Disconnected [CloseConnection],
        Ack => Transaction [AckMessage],
        Nack => Transaction [NackMessage],
        Commit => Connected [CommitTransaction],
        Abort => Connected [AbortTransaction]
    }
}
