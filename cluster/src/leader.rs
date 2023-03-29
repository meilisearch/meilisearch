use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{atomic, Arc, Mutex, RwLock};
use std::time::Duration;

use bus::{Bus, BusReader};
use crossbeam::channel::{unbounded, Receiver, Sender};
use ductile::{ChannelReceiver, ChannelSender, ChannelServer};
use log::{info, warn};
use meilisearch_types::keys::Key;
use meilisearch_types::tasks::Task;
use synchronoise::SignalEvent;
use uuid::Uuid;

use crate::batch::Batch;
use crate::{ApiKeyOperation, Consistency, FollowerMsg, LeaderMsg};

#[derive(Clone)]
pub struct Leader {
    task_ready_to_commit: Receiver<u32>,
    broadcast_to_follower: Sender<LeaderMsg>,
    needs_key_sender: Sender<Sender<Vec<Key>>>,
    needs_key_receiver: Receiver<Sender<Vec<Key>>>,

    pub wake_up: Arc<SignalEvent>,

    new_followers: Arc<AtomicUsize>,
    active_followers: Arc<AtomicUsize>,

    batch_id: Arc<RwLock<u32>>,
}

impl Leader {
    pub fn new(
        listen_on: impl ToSocketAddrs + Send + 'static,
        master_key: Option<String>,
    ) -> Leader {
        let new_followers = Arc::new(AtomicUsize::new(0));
        let active_followers = Arc::new(AtomicUsize::new(1));
        let wake_up = Arc::new(SignalEvent::auto(true));
        let (broadcast_to_follower, process_batch_receiver) = unbounded();
        let (task_finished_sender, task_finished_receiver) = unbounded();
        let (needs_key_sender, needs_key_receiver) = unbounded();

        let nf = new_followers.clone();
        let af = active_followers.clone();
        let wu = wake_up.clone();
        std::thread::spawn(move || {
            Self::listener(
                listen_on,
                master_key,
                nf,
                af,
                wu,
                process_batch_receiver,
                task_finished_sender,
            )
        });

        Leader {
            task_ready_to_commit: task_finished_receiver,
            broadcast_to_follower,
            needs_key_sender,
            needs_key_receiver,

            wake_up,

            new_followers,
            active_followers,
            batch_id: Arc::default(),
        }
    }

    pub fn has_new_followers(&self) -> bool {
        self.new_followers.load(Ordering::Relaxed) != 0
    }

    /// Takes all the necessary channels to chat with the scheduler and give them
    /// to each new followers
    fn listener(
        listen_on: impl ToSocketAddrs,
        master_key: Option<String>,
        new_followers: Arc<AtomicUsize>,
        active_followers: Arc<AtomicUsize>,
        wake_up: Arc<SignalEvent>,
        broadcast_to_follower: Receiver<LeaderMsg>,
        task_finished: Sender<u32>,
    ) {
        let listener: ChannelServer<LeaderMsg, FollowerMsg> = if let Some(ref master_key) =
            master_key
        {
            let mut enc = [0; 32];
            let master_key = master_key.as_bytes();
            if master_key.len() < 32 {
                warn!("Master key is not secure, use a longer master key (at least 32 bytes long)");
            }
            enc.iter_mut().zip(master_key).for_each(|(enc, mk)| *enc = *mk);
            info!("Listening with encryption enabled");
            ChannelServer::bind_with_enc(listen_on, enc).unwrap()
        } else {
            ChannelServer::bind(listen_on).unwrap()
        };

        info!("Ready to the receive connections");

        // We're going to broadcast all the batches to all our follower
        let bus: Bus<LeaderMsg> = Bus::new(10);
        let bus = Arc::new(Mutex::new(bus));
        let b = bus.clone();

        std::thread::spawn(move || loop {
            let msg = broadcast_to_follower.recv().expect("Main thread is dead");
            b.lock().unwrap().broadcast(msg);
        });

        for (sender, receiver, _addr) in listener {
            let task_finished = task_finished.clone();
            let nf = new_followers.clone();
            let af = active_followers.clone();
            let wu = wake_up.clone();

            let process_batch = bus.lock().unwrap().add_rx();

            std::thread::spawn(move || {
                Self::follower(sender, receiver, nf, af, wu, process_batch, task_finished)
            });
        }
    }

    /// Allow a follower to chat with the scheduler
    fn follower(
        sender: ChannelSender<LeaderMsg>,
        receiver: ChannelReceiver<FollowerMsg>,
        new_followers: Arc<AtomicUsize>,
        active_followers: Arc<AtomicUsize>,
        wake_up: Arc<SignalEvent>,
        mut broadcast_to_follower: BusReader<LeaderMsg>,
        task_finished: Sender<u32>,
    ) {
        let size = new_followers.fetch_add(1, Ordering::Relaxed) + 1;
        wake_up.signal();
        info!("A new follower joined the cluster. {} members.", size);

        loop {
            if let msg @ LeaderMsg::JoinFromDump(_) =
                broadcast_to_follower.recv().expect("Main thread died")
            {
                // we exit the new_follower state and become an active follower even though
                // the dump will takes some time to index
                new_followers.fetch_sub(1, Ordering::Relaxed);
                let size = active_followers.fetch_add(1, Ordering::Relaxed) + 1;
                info!("A new follower became active. {} active members.", size);

                sender.send(msg).unwrap();
                break;
            }
        }

        // send messages to the follower
        std::thread::spawn(move || loop {
            let msg = broadcast_to_follower.recv().expect("Main thread died");
            match msg {
                LeaderMsg::JoinFromDump(_) => (),
                msg => {
                    if sender.send(msg).is_err() {
                        // the follower died, the logging and cluster size update should be done
                        // in the other thread
                        break;
                    }
                }
            }
        });

        // receive messages from the follower
        loop {
            match receiver.recv() {
                Err(_) => break,
                Ok(msg) => match msg {
                    FollowerMsg::ReadyToCommit(id) => {
                        task_finished.send(id).expect("Can't reach the main thread")
                    }
                    FollowerMsg::RegisterNewTask(_) => todo!(),
                },
            }
        }

        // if we exited from the previous loop it means the follower is down and should
        // be removed from the cluster
        let size = active_followers.fetch_sub(1, atomic::Ordering::Relaxed) - 1;
        info!("A follower left the cluster. {} members.", size);
    }

    // ============= Everything related to the setup of the cluster
    pub fn join_me(&self, dump: Vec<u8>) {
        self.broadcast_to_follower
            .send(LeaderMsg::JoinFromDump(dump))
            .expect("Lost the link with the followers");
    }

    // ============= Everything related to the scheduler

    pub fn starts_batch(&self, batch: Batch) {
        let mut batch_id = self.batch_id.write().unwrap();

        info!("Send the batch to process to the followers");
        *batch_id += 1;

        self.broadcast_to_follower
            .send(LeaderMsg::StartBatch { id: *batch_id, batch })
            .expect("Can't reach the cluster");
    }

    pub fn commit(&self, consistency_level: Consistency) {
        info!("Wait until enough followers are ready to commit a batch");

        let batch_id = self.batch_id.write().unwrap();

        let mut nodes_ready_to_commit = 1;

        loop {
            let size = self.active_followers.load(atomic::Ordering::Relaxed);

            info!("{nodes_ready_to_commit} nodes are ready to commit for a cluster size of {size}");
            let all = nodes_ready_to_commit == size;

            match consistency_level {
                Consistency::One if nodes_ready_to_commit >= 1 || all => break,
                Consistency::Two if nodes_ready_to_commit >= 2 || all => break,
                Consistency::Quorum if nodes_ready_to_commit >= (size / 2) || all => break,
                Consistency::All if all => break,
                _ => (),
            }

            // we can't wait forever here because if a node dies the cluster size might get updated while we're stuck
            match self.task_ready_to_commit.recv_timeout(Duration::new(1, 0)) {
                Ok(id) if id == *batch_id => nodes_ready_to_commit += 1,
                _ => continue,
            };
        }

        info!("Tells all the follower to commit");

        self.broadcast_to_follower.send(LeaderMsg::Commit(*batch_id)).unwrap();
    }

    pub fn register_new_task(&self, task: Task, update_file: Option<Vec<u8>>) {
        info!("Tells all the follower to register a new task");
        self.broadcast_to_follower
            .send(LeaderMsg::RegisterNewTask { task, update_file })
            .expect("Main thread is dead");
    }

    // ============= Everything related to the api-keys

    pub fn insert_key(&self, key: Key) {
        self.broadcast_to_follower
            .send(LeaderMsg::ApiKeyOperation(ApiKeyOperation::Insert(key)))
            .unwrap()
    }

    pub fn delete_key(&self, uuid: Uuid) {
        self.broadcast_to_follower
            .send(LeaderMsg::ApiKeyOperation(ApiKeyOperation::Delete(uuid)))
            .unwrap()
    }

    pub fn needs_keys(&self) -> Sender<Vec<Key>> {
        self.needs_key_receiver.recv().expect("The cluster is dead")
    }

    pub fn get_keys(&self) -> Vec<Key> {
        let (send, rcv) = crossbeam::channel::bounded(1);
        self.needs_key_sender.send(send).expect("The cluster is dead");
        rcv.recv().expect("The auth controller is dead")
    }
}
