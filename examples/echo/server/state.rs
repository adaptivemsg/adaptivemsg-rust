use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

pub struct StatMgr {
    clients: Mutex<HashSet<String>>,
    subscribers: broadcast::Sender<String>,
    counter: AtomicU64,
}

impl StatMgr {
    pub fn new() -> Self {
        let (subscribers, _) = broadcast::channel(1024);
        Self {
            clients: Mutex::new(HashSet::new()),
            subscribers,
            counter: AtomicU64::new(0),
        }
    }

    pub fn inc_counter(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn counter(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    pub fn on_connect(&self, addr: &str) {
        let addr = addr.to_string();
        self.clients.lock().unwrap().insert(addr.clone());
        let _ = self.subscribers.send(addr);
    }

    pub fn on_disconnect(&self, addr: &str) {
        self.clients.lock().unwrap().remove(addr);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.subscribers.subscribe()
    }

    pub fn list_clients(&self) -> String {
        let mut out = String::new();
        for c in self.clients.lock().unwrap().iter() {
            out.push(' ');
            out.push_str(c);
        }
        out.trim_start().to_string()
    }
}
