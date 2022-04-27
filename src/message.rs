//! Used for communicating between parser and processor.

/// [Internally-tagged enums] [can't] be deserialized by csv crate, which is why records are
/// read as structs, followed by conversion into valid enum. Message encapsulates message
/// validation logic.
///
/// [Internally-tagged enums]: https://serde.rs/enum-representations.html#internally-tagged
/// [can't]: https://github.com/BurntSushi/rust-csv/issues/211
#[derive(Debug)]
pub enum Message {
    Deposit { client: u16, tx: u32, amount: f32 },
    Withdraw { client: u16, tx: u32, amount: f32 },
    Dispute { client: u16, tx: u32 },
    Resolve { client: u16, tx: u32 },
    Chargeback { client: u16, tx: u32 },
}

impl Message {
    pub fn client_id(&self) -> u16 {
        match self {
            Message::Deposit { client, .. } => *client,
            Message::Withdraw { client, .. } => *client,
            Message::Dispute { client, .. } => *client,
            Message::Resolve { client, .. } => *client,
            Message::Chargeback { client, .. } => *client,
        }
    }

    #[allow(dead_code)]
    pub fn transaction_id(&self) -> u32 {
        match self {
            Message::Deposit { tx, .. } => *tx,
            Message::Withdraw { tx, .. } => *tx,
            Message::Dispute { tx, .. } => *tx,
            Message::Resolve { tx, .. } => *tx,
            Message::Chargeback { tx, .. } => *tx,
        }
    }

    /// Returns `true` if the message is [`Deposit`].
    ///
    /// [`Deposit`]: Message::Deposit
    #[must_use]
    pub fn is_deposit(&self) -> bool {
        matches!(self, Self::Deposit { .. })
    }
}
