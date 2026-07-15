use crate::histogram::LatencyHistogram;
use crate::model::DurabilityWindow;
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
    pub windows: Vec<DurabilityWindow>,
    pub final_durable_sequence: u64,
    pub covered_at: Instant,
}

pub struct DurabilityTracker {
    sender: Option<DurabilitySender>,
    task: tokio::task::JoinHandle<Result<DurabilityResult>>,
}

impl DurabilityTracker {
    pub fn start(db: Arc<Db>, measured_started: Instant) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = DurabilitySender { tx };
        let task = tokio::spawn(track(db, rx, measured_started));
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
    measured_started: Instant,
) -> Result<DurabilityResult> {
    let mut status = db.subscribe();
    let mut pending = BTreeMap::<u64, Instant>::new();
    let mut lag = WindowedDurability::new(measured_started);
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
                lag.record(
                    covered_at,
                    covered_at.saturating_duration_since(returned_at),
                )?;
            }
        }
        if input_closed && pending.is_empty() {
            let (lag, windows) = lag.finish(covered_at)?;
            return Ok(DurabilityResult {
                lag,
                windows,
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

const WINDOW_NS: u64 = 1_000_000_000;

struct WindowedDurability {
    started: Instant,
    current_index: u64,
    current: LatencyHistogram,
    overall: LatencyHistogram,
    windows: Vec<DurabilityWindow>,
}

impl WindowedDurability {
    fn new(started: Instant) -> Self {
        Self {
            started,
            current_index: 0,
            current: LatencyHistogram::new(),
            overall: LatencyHistogram::new(),
            windows: Vec::new(),
        }
    }

    fn record(&mut self, covered_at: Instant, duration: std::time::Duration) -> Result<()> {
        let offset_ns = covered_at
            .saturating_duration_since(self.started)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.advance_to(offset_ns)?;
        self.current.record(duration);
        Ok(())
    }

    fn advance_to(&mut self, offset_ns: u64) -> Result<()> {
        let target_index = offset_ns / WINDOW_NS;
        while self.current_index < target_index {
            self.finish_current(WINDOW_NS)?;
            self.current_index += 1;
        }
        Ok(())
    }

    fn finish_current(&mut self, duration_ns: u64) -> Result<()> {
        let current = std::mem::take(&mut self.current);
        let summary = (!current.is_empty()).then(|| current.summary());
        self.overall.add(&current)?;
        self.windows.push(DurabilityWindow {
            start_offset_ns: self.current_index.saturating_mul(WINDOW_NS),
            duration_ns,
            writes_made_durable: current.len(),
            durability_lag: summary,
        });
        Ok(())
    }

    fn finish(mut self, covered_at: Instant) -> Result<(LatencyHistogram, Vec<DurabilityWindow>)> {
        let elapsed_ns = covered_at
            .saturating_duration_since(self.started)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.advance_to(elapsed_ns)?;
        let partial_ns = elapsed_ns.saturating_sub(self.current_index.saturating_mul(WINDOW_NS));
        if partial_ns > 0 || !self.current.is_empty() {
            self.finish_current(partial_ns.max(1))?;
        }
        Ok((self.overall, self.windows))
    }
}

#[cfg(test)]
mod tests {
    use super::WindowedDurability;
    use std::time::{Duration, Instant};

    #[test]
    fn durability_windows_follow_coverage_time() {
        let started = Instant::now();
        let mut collector = WindowedDurability::new(started);
        collector
            .record(
                started + Duration::from_millis(200),
                Duration::from_millis(80),
            )
            .expect("first lag");
        collector
            .record(
                started + Duration::from_millis(1_200),
                Duration::from_millis(120),
            )
            .expect("second lag");

        let (overall, windows) = collector
            .finish(started + Duration::from_millis(1_500))
            .expect("finish windows");

        assert_eq!(overall.len(), 2);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].writes_made_durable, 1);
        assert_eq!(windows[1].writes_made_durable, 1);
        assert_eq!(windows[1].duration_ns, 500_000_000);
    }
}
