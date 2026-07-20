use crate::system::{ApplicationRecorder, ApplicationRegistry};
use anyhow::{Context, Result};
use slatedb::Db;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Debug)]
pub struct DurabilitySender {
    tx: mpsc::UnboundedSender<AcceptedWrite>,
}

#[derive(Debug)]
struct AcceptedWrite {
    sequence: u64,
    returned_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct FrontierObservation {
    sequence: u64,
    observed_at: Instant,
}

type FrontierHistory = Arc<RwLock<Vec<FrontierObservation>>>;

pub struct DurabilityResult {
    pub count: u64,
    pub final_durable_sequence: u64,
}

struct DurabilityShardResult {
    count: u64,
    #[cfg(test)]
    latencies: Vec<Duration>,
}

pub struct DurabilityTracker {
    history: FrontierHistory,
    frontier: watch::Receiver<u64>,
    registry: Arc<ApplicationRegistry>,
    shards: JoinSet<Result<DurabilityShardResult>>,
    observer_stop: Option<oneshot::Sender<()>>,
    observer: JoinHandle<Result<u64>>,
}

impl DurabilityTracker {
    pub fn start(db: Arc<Db>, registry: Arc<ApplicationRegistry>) -> Self {
        let status = db.subscribe();
        let initial = FrontierObservation {
            sequence: status.borrow().durable_seq,
            observed_at: Instant::now(),
        };
        let history = Arc::new(RwLock::new(vec![initial]));
        let (frontier_tx, frontier) = watch::channel(0_u64);
        let (observer_stop, stop_rx) = oneshot::channel();
        let observer = tokio::spawn(observe_frontier(
            status,
            Arc::clone(&history),
            frontier_tx,
            stop_rx,
        ));
        Self {
            history,
            frontier,
            registry,
            shards: JoinSet::new(),
            observer_stop: Some(observer_stop),
            observer,
        }
    }

    pub fn sender(&mut self) -> DurabilitySender {
        let (tx, rx) = mpsc::unbounded_channel();
        self.shards.spawn(track_shard(
            self.registry.recorder(),
            Arc::clone(&self.history),
            self.frontier.clone(),
            rx,
        ));
        DurabilitySender { tx }
    }

    pub async fn finish(mut self) -> Result<DurabilityResult> {
        let mut count = 0_u64;
        while let Some(result) = self.shards.join_next().await {
            let shard = result.context("joining durability tracker shard")??;
            count = count.saturating_add(shard.count);
        }
        if let Some(stop) = self.observer_stop.take() {
            let _ = stop.send(());
        }
        let final_durable_sequence = self
            .observer
            .await
            .context("joining durable frontier observer")??;
        Ok(DurabilityResult {
            count,
            final_durable_sequence,
        })
    }

    pub fn abort(mut self) {
        self.shards.abort_all();
        if let Some(stop) = self.observer_stop.take() {
            let _ = stop.send(());
        }
        self.observer.abort();
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

async fn observe_frontier(
    mut status: watch::Receiver<slatedb::DbStatus>,
    history: FrontierHistory,
    frontier: watch::Sender<u64>,
    mut stop: oneshot::Receiver<()>,
) -> Result<u64> {
    let mut durable = history
        .read()
        .expect("durable frontier history lock")
        .last()
        .expect("durable frontier history has an initial observation")
        .sequence;
    let observed_at = Instant::now();
    let current = status.borrow_and_update().durable_seq;
    if current > durable {
        durable = current;
        history
            .write()
            .expect("durable frontier history lock")
            .push(FrontierObservation {
                sequence: durable,
                observed_at,
            });
        frontier.send_modify(|version| {
            *version = version.saturating_add(1);
        });
    }
    loop {
        tokio::select! {
            _ = &mut stop => return Ok(durable),
            changed = status.changed() => {
                changed.context("database closed before measured writes became durable")?;
                let observed_at = Instant::now();
                let next = status.borrow_and_update().durable_seq;
                if next > durable {
                    durable = next;
                    history
                        .write()
                        .expect("durable frontier history lock")
                        .push(FrontierObservation {
                            sequence: durable,
                            observed_at,
                        });
                    frontier.send_modify(|version| {
                        *version = version.saturating_add(1);
                    });
                }
            }
        }
    }
}

async fn track_shard(
    recorder: ApplicationRecorder,
    history: FrontierHistory,
    mut frontier: watch::Receiver<u64>,
    mut rx: mpsc::UnboundedReceiver<AcceptedWrite>,
) -> Result<DurabilityShardResult> {
    let mut pending = VecDeque::<AcceptedWrite>::new();
    let mut history_index = 0_usize;
    let mut latest_sequence = history
        .read()
        .expect("durable frontier history lock")
        .last()
        .expect("durable frontier history has an initial observation")
        .sequence;
    let mut input_closed = false;
    let mut result = DurabilityShardResult {
        count: 0,
        #[cfg(test)]
        latencies: Vec::new(),
    };

    loop {
        if input_closed && pending.is_empty() {
            return Ok(result);
        }
        tokio::select! {
            value = rx.recv(), if !input_closed => {
                match value {
                    Some(write) => {
                        let covered = write.sequence <= latest_sequence;
                        pending.push_back(write);
                        if covered {
                            drain_covered(
                                &recorder,
                                &history,
                                &mut history_index,
                                &mut pending,
                                &mut result,
                            );
                        }
                    }
                    None => input_closed = true,
                }
            }
            changed = frontier.changed() => {
                changed.context("durable frontier observer stopped")?;
                latest_sequence = history
                    .read()
                    .expect("durable frontier history lock")
                    .last()
                    .expect("durable frontier history has an observation")
                    .sequence;
                drain_covered(
                    &recorder,
                    &history,
                    &mut history_index,
                    &mut pending,
                    &mut result,
                );
            }
        }
    }
}

fn drain_covered(
    recorder: &ApplicationRecorder,
    history: &FrontierHistory,
    history_index: &mut usize,
    pending: &mut VecDeque<AcceptedWrite>,
    result: &mut DurabilityShardResult,
) {
    let observations = history.read().expect("durable frontier history lock");
    while let Some(write) = pending.front() {
        while observations
            .get(*history_index)
            .is_some_and(|observation| observation.sequence < write.sequence)
        {
            *history_index += 1;
        }
        let Some(observation) = observations.get(*history_index) else {
            break;
        };
        let write = pending.pop_front().expect("pending write exists");
        let elapsed = observation
            .observed_at
            .saturating_duration_since(write.returned_at);
        recorder.record_latency("durable", elapsed);
        result.count = result.count.saturating_add(1);
        #[cfg(test)]
        result.latencies.push(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe(
        history: &FrontierHistory,
        frontier: &watch::Sender<u64>,
        sequence: u64,
        observed_at: Instant,
    ) {
        history
            .write()
            .expect("frontier history lock")
            .push(FrontierObservation {
                sequence,
                observed_at,
            });
        frontier.send_modify(|version| *version += 1);
    }

    #[tokio::test]
    async fn uses_first_covering_frontier_observation_for_delayed_writes() {
        let base = Instant::now();
        let history = Arc::new(RwLock::new(vec![FrontierObservation {
            sequence: 0,
            observed_at: base,
        }]));
        let (frontier_tx, frontier_rx) = watch::channel(0_u64);
        observe(
            &history,
            &frontier_tx,
            10,
            base + Duration::from_millis(100),
        );
        observe(
            &history,
            &frontier_tx,
            20,
            base + Duration::from_millis(200),
        );
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(AcceptedWrite {
            sequence: 5,
            returned_at: base + Duration::from_millis(25),
        })
        .expect("send first accepted write");
        tx.send(AcceptedWrite {
            sequence: 15,
            returned_at: base + Duration::from_millis(50),
        })
        .expect("send second accepted write");
        drop(tx);

        let registry = ApplicationRegistry::default();
        let result = track_shard(registry.recorder(), history, frontier_rx, rx)
            .await
            .expect("track durability shard");

        assert_eq!(result.count, 2);
        assert_eq!(
            result.latencies,
            vec![Duration::from_millis(75), Duration::from_millis(150)]
        );
    }

    #[tokio::test]
    async fn drains_pending_writes_when_frontier_advances() {
        let base = Instant::now();
        let history = Arc::new(RwLock::new(vec![FrontierObservation {
            sequence: 0,
            observed_at: base,
        }]));
        let (frontier_tx, frontier_rx) = watch::channel(0_u64);
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(AcceptedWrite {
            sequence: 10,
            returned_at: base,
        })
        .expect("send accepted write");

        let registry = ApplicationRegistry::default();
        let shard = tokio::spawn(track_shard(
            registry.recorder(),
            Arc::clone(&history),
            frontier_rx,
            rx,
        ));
        tokio::task::yield_now().await;
        observe(
            &history,
            &frontier_tx,
            10,
            base + Duration::from_millis(125),
        );
        drop(tx);

        let result = shard
            .await
            .expect("join durability shard")
            .expect("track durability shard");
        assert_eq!(result.count, 1);
        assert_eq!(result.latencies, vec![Duration::from_millis(125)]);
    }
}
