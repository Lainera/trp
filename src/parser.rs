//! Parses input from csv in a separate thread via [`parser::start`](start).

const PARSER_CHAN_SIZE: usize = 100;

use serde::Deserialize;
use std::path::Path;
use tokio::sync::mpsc::Receiver;

use crate::Message;

impl TryFrom<&Record> for Message {
    type Error = anyhow::Error;

    fn try_from(record: &Record) -> Result<Self, Self::Error> {
        let Record {
            kind,
            client,
            tx,
            amount,
        } = record;
        let client = *client;
        let tx = *tx;
        let amount = *amount;

        match (kind.as_str(), amount) {
            ("deposit", Some(amount)) => Ok(Message::Deposit { client, tx, amount }),
            ("withdrawal", Some(amount)) => Ok(Message::Withdraw { client, tx, amount }),
            ("dispute", None) => Ok(Message::Dispute { client, tx }),
            ("resolve", None) => Ok(Message::Resolve { client, tx }),
            ("chargeback", None) => Ok(Message::Chargeback { client, tx }),
            _ => Err(anyhow::anyhow!("Invalid record")),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type")]
    kind: String,
    client: u16,
    tx: u32,
    amount: Option<f32>,
}

/// Spawns separate thread for reading csv.
/// Simpler design would be to `read -> parse -> handle transaction` in a single loop,
/// chosen approach scales better for concurrent handling of parsed transactions, as well as
/// larger data sets (i.e. transaction history does not have to be stored in one place).
pub fn start<P>(input: P) -> Result<Receiver<Message>, anyhow::Error>
where
    P: AsRef<Path>,
{
    let mut rdr = csv::ReaderBuilder::new().from_path(input)?;

    let (tx, rx) = tokio::sync::mpsc::channel(PARSER_CHAN_SIZE);

    std::thread::spawn(move || {
        for result in rdr.deserialize() {
            let record: Record = if let Err(err) = result {
                eprintln!("Failed to parse record: {err}");
                continue;
            } else {
                result.unwrap()
            };

            if let Ok(message) = Message::try_from(&record) {
                tx.blocking_send(message)
                    .unwrap_or_else(|err| eprintln!("Failed to send from csv: {err}"));
            } else {
                eprintln!("Parsed record, but it is invalid: {record:?}");
            }
        }
    });

    Ok(rx)
}
