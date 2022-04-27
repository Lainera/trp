use crate::message::Message;
use std::thread;

const RESULT_CHAN_SIZE: usize = 100;

mod message;
mod parser;
mod processor;

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

    writer_handle
        .join()
        .map_err(|err| anyhow::anyhow!("Writer panic: {err:?}"))??;

    Ok(())
}
