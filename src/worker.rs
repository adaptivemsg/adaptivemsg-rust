use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::time::interval;

use crate::wire::Priority;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub resident_workers: usize,
    pub qsize_per_core: usize,
    pub q_weight: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            resident_workers: 1,
            qsize_per_core: 128,
            q_weight: 8,
        }
    }
}

pub struct WorkerPool<T: Send + 'static> {
    ingress_high: mpsc::Sender<T>,
    ingress_normal: mpsc::Sender<T>,
    ingress_low: mpsc::Sender<T>,
    queue_len: Arc<AtomicUsize>,
    workers: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
}

impl<T: Send + 'static> WorkerPool<T> {
    pub fn new(
        cfg: WorkerConfig,
        worker: impl Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        let total_qsize = cfg.qsize_per_core * num_cpus::get().max(1);
        let (egress_tx, egress_rx) = mpsc::channel::<T>(total_qsize);
        let (ingress_normal, mut ingress_normal_rx) = mpsc::channel::<T>(total_qsize);
        let (ingress_high, mut ingress_high_rx) = mpsc::channel::<T>(total_qsize);
        let (ingress_low, mut ingress_low_rx) = mpsc::channel::<T>(total_qsize);

        let queue_len = Arc::new(AtomicUsize::new(0));
        let qlen_reorder = queue_len.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = ingress_high_rx.recv() => {
                        if let Some(msg) = msg {
                            let _ = egress_tx.send(msg).await;
                        } else {
                            break;
                        }
                    }
                    msg = ingress_normal_rx.recv() => {
                        if let Some(msg) = msg {
                            let _ = egress_tx.send(msg).await;
                        } else {
                            break;
                        }
                    }
                    msg = ingress_low_rx.recv() => {
                        if let Some(msg) = msg {
                            let _ = egress_tx.send(msg).await;
                        } else {
                            break;
                        }
                    }
                }
            }
            qlen_reorder.store(0, Ordering::Relaxed);
        });

        let worker: Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync> = Arc::new(worker);
        let workers = Arc::new(Mutex::new(Vec::new()));

        let shared_rx = Arc::new(AsyncMutex::new(egress_rx));
        for _ in 0..cfg.resident_workers {
            spawn_worker(worker.clone(), queue_len.clone(), shared_rx.clone(), workers.clone());
        }

        if cfg.q_weight > 0 {
            let workers = workers.clone();
            let worker = worker.clone();
            let queue_len = queue_len.clone();
            let shared_rx = shared_rx.clone();
            let mut tick = interval(Duration::from_millis(500));
            tokio::spawn(async move {
                loop {
                    tick.tick().await;
                    let backlog = queue_len.load(Ordering::Relaxed);
                    let should = backlog / cfg.q_weight + cfg.resident_workers;
                    let mut guard = workers.lock().unwrap();
                    let now = guard.len();
                    if should > now {
                        for _ in 0..(should - now) {
                            spawn_worker(worker.clone(), queue_len.clone(), shared_rx.clone(), workers.clone());
                        }
                    } else if should < now {
                        for _ in 0..(now - should) {
                            if let Some(stop) = guard.pop() {
                                let _ = stop.send(());
                            }
                        }
                    }
                }
            });
        }

        Self {
            ingress_high,
            ingress_normal,
            ingress_low,
            queue_len,
            workers,
        }
    }

    pub async fn enqueue(&self, msg: T, prio: Priority) {
        self.queue_len.fetch_add(1, Ordering::Relaxed);
        let send_res = match prio {
            Priority::High => self.ingress_high.send(msg).await,
            Priority::Normal => self.ingress_normal.send(msg).await,
            Priority::Low => self.ingress_low.send(msg).await,
        };
        if send_res.is_err() {
            self.queue_len.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn spawn_worker<T: Send + 'static>(
    worker: Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
    queue_len: Arc<AtomicUsize>,
    rx: Arc<AsyncMutex<mpsc::Receiver<T>>>,
    workers: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
) {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    workers.lock().unwrap().push(stop_tx);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                msg = async {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                } => {
                    if let Some(msg) = msg {
                        queue_len.fetch_sub(1, Ordering::Relaxed);
                        (worker)(msg).await;
                    } else {
                        break;
                    }
                }
            }
        }
    });
}
