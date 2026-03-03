use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

pub struct StatMgr {
    clients: Mutex<HashSet<String>>,
    subscribers: Mutex<HashMap<u64, mpsc::Sender<String>>>,
    session_num: AtomicU64,
    counter: AtomicU64,
    sub_seq: AtomicU64,
}

pub struct StreamContext {
    mgr: Arc<StatMgr>,
    subscriber: Mutex<Option<u64>>,
}

impl StreamContext {
    pub fn new(mgr: Arc<StatMgr>) -> Self {
        Self {
            mgr,
            subscriber: Mutex::new(None),
        }
    }

    pub fn mgr(&self) -> Arc<StatMgr> {
        Arc::clone(&self.mgr)
    }

    pub fn set_subscriber(&self, id: u64) {
        *self.subscriber.lock().unwrap() = Some(id);
    }

    pub fn take_subscriber(&self) -> Option<u64> {
        self.subscriber.lock().unwrap().take()
    }
}

impl StatMgr {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashSet::new()),
            subscribers: Mutex::new(HashMap::new()),
            session_num: AtomicU64::new(0),
            counter: AtomicU64::new(0),
            sub_seq: AtomicU64::new(0),
        }
    }

    pub fn next_session(&self) -> u64 {
        self.session_num.fetch_add(1, Ordering::Relaxed) + 1
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
        for tx in self.subscribers.lock().unwrap().values() {
            let _ = tx.try_send(addr.clone());
        }
    }

    pub fn on_disconnect(&self, addr: &str) {
        self.clients.lock().unwrap().remove(addr);
    }

    pub fn add_subscriber(&self, tx: mpsc::Sender<String>) -> u64 {
        let id = self.sub_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.subscribers.lock().unwrap().insert(id, tx);
        id
    }

    pub fn remove_subscriber(&self, id: u64) {
        self.subscribers.lock().unwrap().remove(&id);
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
