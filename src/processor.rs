//! Deals with everything related to management of client transactions.

const ACCOUNT_CHAN_SIZE: usize = 100;

use crate::Message;
use serde::Serialize;
use std::{collections::HashMap, fmt::Display};
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Given message is for client who does not have an account yet:
/// - When message is [`Message::Withdraw`] - then op would fail, since starting account balance is 0.
/// - When message is [`Message::Dispute`] | [`Message::Resolve`] | [`Message::Chargeback`] - then op would fail since there is
/// no previous deposit to dispute/resolve/chargeback.
/// - When message is [`Message::Deposit`] - then op would succeed.
pub fn should_create_account(msg: &Message) -> bool {
    msg.is_deposit()
}

/// Functions as a router for the [`Account`] tasks. Spawns task if there is no task for
/// client, then forwards message to appropriate task.
/// When there is no more input from [`parser::start`](crate::parser::start), exits, causing `clients` to be dropped.
/// This in return causes all tasks to stop listening for messages and report their stats to
/// writer thread.
pub async fn start(mut rx: Receiver<Message>, done_tx: Sender<Account<Running>>) {
    let mut clients = HashMap::new();

    while let Some(msg) = rx.recv().await {
        let client_id = msg.client_id();
        if clients.get(&client_id).is_none() {
            if !should_create_account(&msg) {
                eprintln!("Got out of order message: {msg:?}, ignoring");
                continue;
            }

            let account = Account::new(client_id);
            match account.start(done_tx.clone()) {
                Ok(client_tx) => {
                    clients.insert(client_id, client_tx);
                }
                Err(err) => {
                    eprintln!("Failed to spawn task for account({client_id}) : {err}");
                    continue;
                }
            };
        }

        let tx = clients.get(&client_id).unwrap();
        if let Err(msg) = tx.send(msg).await {
            eprintln!("Failed to send {msg} to task for account({client_id})");
        }
    }
}

/// Represents state of the clients account. Generic attribute is used for typestate checks,
/// to ensure task for account is started only once.
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

/// Typestate ZST
#[derive(Debug)]
pub struct Running;

/// Typestate ZST
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

    /// Starts the task for the account.
    ///
    /// # Panics
    ///
    /// Since function spawns a task, it would panic when called outside of
    /// runtime context.
    fn start(
        self,
        done: mpsc::Sender<Account<Running>>,
    ) -> Result<mpsc::Sender<Message>, anyhow::Error> {
        let (tx, mut rx) = mpsc::channel(ACCOUNT_CHAN_SIZE);
        let mut history: TXHistory = HashMap::new();
        let Self {
            client,
            available,
            held,
            total,
            locked,
            _state,
        } = self;
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
                account
                    .apply(&msg, &mut history)
                    .unwrap_or_else(|err| eprintln!("Failed to apply message {msg:?}: {err}"));
            }

            done.send(account)
                .await
                .unwrap_or_else(|err| eprintln!("Failed to send results: {err}"));
        });

        Ok(tx)
    }
}

/// State of transaction in transaction history.
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

/// Simple in-memory storage for transaction history.
/// Used by account task to lookup [`Message::Deposit`] amounts.
/// In a real world situation this could also be a remote store.
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
    fn apply(
        &mut self,
        message: &Message,
        tx_history: &mut TXHistory,
    ) -> Result<(), ProcessingError> {
        if self.locked {
            return Err(ProcessingError::AccountLocked);
        }
        match message {
            Message::Deposit { tx, amount, .. } => {
                self.available += amount;
                self.total += amount;
                tx_history.insert(*tx, Transaction::Deposited(*amount));
            }
            Message::Withdraw { amount, .. } => {
                if self.available < *amount {
                    return Err(ProcessingError::InsufficientFunds);
                }
                self.available -= amount;
                self.total -= amount;
            }
            Message::Dispute { tx, .. } => {
                if let Some(existing) = tx_history
                    .get_mut(tx)
                    .filter(|existing| existing.is_deposited())
                    .filter(|existing| self.available >= existing.amount())
                {
                    let amount = existing.amount();
                    self.available -= amount;
                    self.held += amount;
                    *existing = Transaction::Disputed(amount);
                }
            }
            Message::Resolve { tx, .. } => {
                if let Some(existing) = tx_history
                    .get_mut(tx)
                    .filter(|existing| existing.is_disputed())
                {
                    let amount = existing.amount();
                    self.available += amount;
                    self.held -= amount;
                    *existing = Transaction::Deposited(amount);
                }
            }
            Message::Chargeback { tx, .. } => {
                if let Some(existing) = tx_history
                    .get_mut(tx)
                    .filter(|existing| existing.is_disputed())
                {
                    let amount = existing.amount();
                    self.held -= amount;
                    self.total -= amount;
                    self.locked = true;
                    *existing = Transaction::Reversed(amount);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Account, Running, Transaction};
    use crate::{message::Message, processor::ProcessingError};
    use std::collections::HashMap;

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

        let withdrawal = Message::Withdraw {
            client,
            tx: 144,
            amount: 3.0,
        };

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

        let withdrawal = Message::Withdraw {
            client,
            tx: 144,
            amount: 3.0,
        };

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
        assert!(account.apply(&resolve, &mut history).is_ok());
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
        assert!(account.apply(&resolve, &mut history).is_ok());
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
        assert!(account.apply(&chargeback, &mut history).is_ok());
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
        assert!(account.apply(&resolve, &mut history).is_ok());
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
