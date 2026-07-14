use crate::histogram::LatencyHistogram;
use anyhow::{Context, Result};
use slatedb::Db;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct DurabilitySender {
    tx: mpsc::UnboundedSender<AcceptedWrite>,
}

#[derive(Debug)]
struct AcceptedWrite {
    sequence: u64,
    returned_at: Instant,
}

#[derive(Debug)]
pub struct DurabilityResult {
    pub lag: LatencyHistogram,
    pub final_durable_sequence: u64,
    pub covered_at: Instant,
}

pub struct DurabilityTracker {
    sender: Option<DurabilitySender>,
    task: tokio::task::JoinHandle<Result<DurabilityResult>>,
}

impl DurabilityTracker {
    pub fn start(db: Arc<Db>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = DurabilitySender { tx };
        let task = tokio::spawn(track(db, rx));
        Self {
            sender: Some(sender),
            task,
        }
    }

    pub fn sender(&self) -> DurabilitySender {
        self.sender
            .as_ref()
            .expect("durability sender is present")
            .clone()
    }

    pub async fn finish(mut self) -> Result<DurabilityResult> {
        self.sender.take();
        self.task.await.context("joining durability tracker")?
    }
}

impl DurabilitySender {
    pub fn accepted(&self, sequence: u64, returned_at: Instant) {
        let _ = self.tx.send(AcceptedWrite {
            sequence,
            returned_at,
        });
    }
}

async fn track(
    db: Arc<Db>,
    mut rx: mpsc::UnboundedReceiver<AcceptedWrite>,
) -> Result<DurabilityResult> {
    let mut status = db.subscribe();
    let mut pending = BTreeMap::<u64, Instant>::new();
    let mut lag = LatencyHistogram::new();
    let mut input_closed = false;
    loop {
        let durable = status.borrow().durable_seq;
        let covered = pending
            .range(..=durable)
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>();
        let covered_at = Instant::now();
        for sequence in covered {
            if let Some(returned_at) = pending.remove(&sequence) {
                lag.record(covered_at.saturating_duration_since(returned_at));
            }
        }
        if input_closed && pending.is_empty() {
            return Ok(DurabilityResult {
                lag,
                final_durable_sequence: durable,
                covered_at,
            });
        }

        tokio::select! {
            value = rx.recv(), if !input_closed => {
                match value {
                    Some(write) => { pending.insert(write.sequence, write.returned_at); }
                    None => { input_closed = true; }
                }
            }
            changed = status.changed() => {
                changed.context("database closed before measured writes became durable")?;
            }
        }
    }
}
