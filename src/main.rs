use std::thread;
use crate::message::Message;

const RESULT_CHAN_SIZE: usize = 100;

/// Parses input from csv in a separate thread via [`parser::start`].
mod parser {
    const PARSER_CHAN_SIZE: usize = 100;

    use std::path::Path;
    use serde::Deserialize;
    use tokio::sync::mpsc::Receiver;

    use crate::Message;


    impl TryFrom<&Record> for Message {
        type Error = anyhow::Error;

        fn try_from(record: &Record) -> Result<Self, Self::Error> {
            let Record { kind, client, tx, amount } = record;
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
    /// bigger data sets (i.e. transaction history does not have to be stored in one place).
    pub fn start<P>(input: P) -> Result<Receiver<Message>, anyhow::Error> 
        where
            P: AsRef<Path>,
    {

        let mut rdr = csv::ReaderBuilder::new()
            .from_path(input)?;

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
}

mod message {
    #[derive(Debug)]
    /// [Internally-tagged enums] [can't] be deserialized by csv crate, which is why records are
    /// read as structs, followed by conversion into valid enum. Message encapsulates message
    /// validation logic. 
    /// [internally-tagged enums]: https://serde.rs/enum-representations.html#internally-tagged
    /// [can't]: https://github.com/BurntSushi/rust-csv/issues/211
    pub enum Message {
        Deposit { client: u16, tx: u32, amount: f32 },
        Withdraw { client: u16, tx: u32, amount: f32 },
        Dispute {client: u16, tx: u32 },
        Resolve {client: u16, tx: u32 },
        Chargeback {client: u16, tx: u32 },
    }

    impl Message {
        pub fn client_id(&self) -> u16 {
            match self {
                Message::Deposit { client, ..} => *client,
                Message::Withdraw { client, ..} => *client,
                Message::Dispute { client, ..} => *client,
                Message::Resolve { client, ..} => *client,
                Message::Chargeback { client, ..} => *client,
            }
        }

        #[allow(dead_code)]
        pub fn transaction_id(&self) -> u32 {
            match self {
                Message::Deposit { tx, ..} => *tx,
                Message::Withdraw { tx, ..} => *tx,
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
}

/// Deals with everything related to management of client transactions.
mod processor {
    const ACCOUNT_CHAN_SIZE: usize = 100;

    use std::{collections::HashMap, fmt::Display};
    use serde::Serialize;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use crate::Message;

    /// Given message is for client who does not have an account yet,
    /// When message is Withdraw - then op would fail, since account balance is 0
    /// When message is Dispute/Resolve/Chargeback - then op would fail since there is
    /// no previous deposit to dispute/resolve/chargeback.
    /// When message is Deposit - then op would succeed. 
    pub fn should_create_account(msg: &Message) -> bool {
        msg.is_deposit()
    }

    pub async fn start(mut rx: Receiver<Message>, done_tx: Sender<Account<Running>>) {
        let mut clients = HashMap::new();

        while let Some(msg) = rx.recv().await {
            let client_id = msg.client_id();
            if clients.get(&client_id).is_none() {
                if !should_create_account(&msg) {
                    eprintln!("Got out of order message: {msg:?}, ignoring");
                    continue
                }

                let account = Account::new(client_id);
                match account.start(done_tx.clone()) {
                    Ok(client_tx) => {
                        clients.insert(client_id, client_tx);
                    },
                    Err(err) => {
                        eprintln!("Failed to spawn task for account({client_id}) : {err}");
                        continue;
                    },
                };
            }

            let tx = clients.get(&client_id).unwrap();
            if let Err(msg) = tx.send(msg).await {
                eprintln!("Failed to send {msg} to task for account({client_id})");
            }
        }
    }

    #[derive(Debug, Serialize)]
    pub struct Account<T> {
        client: u16,
        available: f32,
        held: f32,
        total: f32,
        locked: bool,
        #[serde(skip)]
        _state: T,
    }

    #[derive(Debug)]
    pub struct Running;

    #[derive(Default, Debug)]
    pub struct Ready;

    impl Account<Ready> {
        pub fn new(client: u16) -> Self {
            Account { 
                client, 
                available: 0.0, 
                held: 0.0, 
                total: 0.0, 
                locked: false,
                _state: Ready,
            }
        }

        fn start(self, done: mpsc::Sender<Account<Running>>) -> Result<mpsc::Sender<Message>, anyhow::Error> {
            let (tx, mut rx) = mpsc::channel(ACCOUNT_CHAN_SIZE);
            let mut history: TXHistory = HashMap::new();
            let Self { client, available, held, total, locked, _state } = self;
            let mut account = Account {
                client,
                available,
                held,
                total,
                locked,
                _state: Running,
            };

            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    account.apply(&msg, &mut history)
                        .unwrap_or_else(|err| eprintln!("Failed to apply message {msg:?}: {err}"));
                }

                done.send(account).await
                    .unwrap_or_else(|err| eprintln!("Failed to send results: {err}"));
            });

            Ok(tx)
        }
    }
    
    enum Transaction<T = f32> {
        Deposited(T),
        Disputed(T),
        Reversed(T),
    }

    impl<T> Transaction<T> {
        /// Returns `true` if the transaction is [`Deposited`].
        ///
        /// [`Deposited`]: Transaction::Deposited
        #[must_use]
        fn is_deposited(&self) -> bool {
            matches!(self, Self::Deposited(..))
        }
    
        /// Returns `true` if the transaction is [`Disputed`].
        ///
        /// [`Disputed`]: Transaction::Disputed
        #[must_use]
        fn is_disputed(&self) -> bool {
            matches!(self, Self::Disputed(..))
        }
    }

    impl<T: Copy> Transaction<T> {
        fn amount(&self) -> T {
            match self {
                Transaction::Deposited(x) => *x,
                Transaction::Disputed(x) => *x,
                Transaction::Reversed(x) => *x,
            }
        }
    }

    type TXHistory = HashMap<u32, Transaction>;

    #[derive(Debug)]
    enum ProcessingError {
        InsufficientFunds,
        AccountLocked,
    }

    impl Display for ProcessingError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ProcessingError::InsufficientFunds => f.write_str("PE_INSF"),
                ProcessingError::AccountLocked => f.write_str("PE_ACCLCK"),
            }
        }
    }

    impl std::error::Error for ProcessingError {}

    impl Account<Running> {
        fn apply(&mut self, message: &Message, tx_history: &mut TXHistory) -> Result<(), ProcessingError> {
            if self.locked {
                return Err(ProcessingError::AccountLocked)
            }
            match message {
                Message::Deposit { tx, amount, .. } => {
                    self.available += amount;
                    self.total += amount;
                    tx_history.insert(*tx, Transaction::Deposited(*amount));
                },
                Message::Withdraw {amount, .. } => {
                    if self.available < *amount {
                        return Err(ProcessingError::InsufficientFunds);
                    }
                    self.available -= amount;
                    self.total -= amount;
                },
                Message::Dispute { tx, .. } => {
                    if let Some(existing) = tx_history.get_mut(tx)
                        .filter(|existing| existing.is_deposited())
                        .filter(|existing| self.available >= existing.amount()) {
                            let amount = existing.amount();
                            self.available -= amount;
                            self.held += amount;
                            *existing = Transaction::Disputed(amount);
                        }
                },
                Message::Resolve { tx, .. } => {
                    if let Some(existing) = tx_history.get_mut(tx)
                        .filter(|existing| existing.is_disputed()) {
                            let amount = existing.amount();
                            self.available += amount;
                            self.held -= amount;
                            *existing = Transaction::Deposited(amount);
                        }
                },
                Message::Chargeback { tx, .. } => {
                    if let Some(existing) = tx_history.get_mut(tx)
                        .filter(|existing| existing.is_disputed()) {
                            let amount = existing.amount();
                            self.held -= amount;
                            self.total -= amount;
                            self.locked = true;
                            *existing = Transaction::Reversed(amount);
                        }
                },
            }

            Ok(())
        } 
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use crate::{message::Message, processor::ProcessingError};
        use super::{Account, Running, Transaction};

        fn running(id: u16) -> Account<Running> {
            Account {
                client: id,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
                _state: Running,
            }
        }
        
        #[test]
        fn valid_deposit_is_handled() {
            let mut account = running(42);
            let mut history = HashMap::new();
            let msg = Message::Deposit {
                client: 42,
                amount: 1.1,
                tx: 123,
            };

            let outcome = account.apply(&msg, &mut history);
            assert!(outcome.is_ok());
            assert_eq!(account.total, 1.1);
            assert_eq!(account.available, 1.1);
            assert_eq!(account.held, 0.0);
            assert!(!account.locked);

            let saved = history.get(&msg.transaction_id());
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(saved.is_deposited());
        }

        #[test]
        fn valid_withdrawal_is_handled() {
            let client = 42;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 10.0,
                tx: 123,
                client,
            };

            let withdrawal = Message::Withdraw { client, tx: 144, amount: 3.0 };

            assert!(account.apply(&deposit, &mut history).is_ok());
            assert!(account.apply(&withdrawal, &mut history).is_ok());

            assert_eq!(account.available, 7.0);
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 7.0);
            assert!(!account.locked);
        } 
        
        #[test]
        fn invalid_withdrawal_is_handled() {
            let client = 42;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx: 123,
                client,
            };

            let withdrawal = Message::Withdraw { client, tx: 144, amount: 3.0 };

            assert!(account.apply(&deposit, &mut history).is_ok());

            let outcome = account.apply(&withdrawal, &mut history);
            assert!(outcome.is_err());
            let outcome = outcome.unwrap_err();
            assert!(matches!(outcome, ProcessingError::InsufficientFunds));
            assert_eq!(account.available, 1.0);
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 1.0);
            assert!(!account.locked);
        } 

        #[test]
        fn valid_dispute_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            let dispute = Message::Dispute { client, tx };

            assert!(account.apply(&deposit, &mut history).is_ok());
            assert!(account.apply(&dispute, &mut history).is_ok());
            assert_eq!(account.held, 1.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 0.0);
            assert!(!account.locked);
            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Disputed(_)));
        } 
        
        #[test]
        fn invalid_dispute_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            let dispute = Message::Dispute { client, tx: 124 };

            assert!(account.apply(&deposit, &mut history).is_ok());
            assert!(account.apply(&dispute, &mut history).is_ok());
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 1.0);
            assert!(!account.locked);
        } 

        #[test]
        fn valid_resolve_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            let dispute = Message::Dispute { client, tx };

            assert!(account.apply(&deposit, &mut history).is_ok());
            assert!(account.apply(&dispute, &mut history).is_ok());
            assert_eq!(account.held, 1.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 0.0);
            assert!(!account.locked);

            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Disputed(_)));

            let resolve = Message::Resolve { client, tx };
            assert!(account.apply(&resolve, &mut  history).is_ok());
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 1.0);
            assert!(!account.locked);
            
            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Deposited(_)));
        } 
        

        #[test]
        fn invalid_resolve_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            assert!(account.apply(&deposit, &mut history).is_ok());

            let resolve = Message::Resolve { client, tx };
            assert!(account.apply(&resolve, &mut  history).is_ok());
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 1.0);
            assert!(!account.locked);
            
            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Deposited(_)));
        }
        
        #[test]
        fn valid_chargeback_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            let dispute = Message::Dispute { client, tx };

            assert!(account.apply(&deposit, &mut history).is_ok());
            assert!(account.apply(&dispute, &mut history).is_ok());
            assert_eq!(account.held, 1.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 0.0);
            assert!(!account.locked);

            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Disputed(_)));

            let chargeback = Message::Chargeback { client, tx };
            assert!(account.apply(&chargeback, &mut  history).is_ok());
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 0.0);
            assert_eq!(account.available, 0.0);
            assert!(account.locked);
            
            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Reversed(_)));
        } 
        

        #[test]
        fn invalid_chargeback_is_handled() {
            let client = 42;
            let tx = 123;
            let mut account = running(client);
            let mut history = HashMap::new();
            let deposit = Message::Deposit {
                amount: 1.0,
                tx,
                client,
            };

            assert!(account.apply(&deposit, &mut history).is_ok());

            let resolve = Message::Chargeback { client, tx };
            assert!(account.apply(&resolve, &mut  history).is_ok());
            assert_eq!(account.held, 0.0);
            assert_eq!(account.total, 1.0);
            assert_eq!(account.available, 1.0);
            assert!(!account.locked);
            
            let saved = history.get(&tx);
            assert!(saved.is_some());
            let saved = saved.unwrap();
            assert!(matches!(saved, Transaction::Deposited(_)));
        }


    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fname = std::env::args()
        .nth(1)
        .expect("Must provide input file to read");

    let rx = parser::start(fname)?;
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(RESULT_CHAN_SIZE);


    let writer_handle = thread::spawn(move || {
        let mut out = csv::Writer::from_writer(std::io::stdout());
        
        while let Some(account) = done_rx.blocking_recv() {
            out.serialize(account)?;
        }

        Ok::<(), csv::Error>(())
    });

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        processor::start(rx, done_tx).await;
    });

    writer_handle.join()
        .map_err(|err| anyhow::anyhow!("Writer panic: {err:?}"))??;

    Ok(())
}

