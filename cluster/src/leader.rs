use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{atomic, Arc, Mutex, RwLock};

use bus::{Bus, BusReader};
use crossbeam::channel::{unbounded, Receiver, Sender};
use ductile::{ChannelReceiver, ChannelSender, ChannelServer};
use log::info;
use meilisearch_types::tasks::Task;
use synchronoise::SignalEvent;

use crate::batch::Batch;
use crate::{Consistency, FollowerMsg, LeaderMsg};

#[derive(Clone)]
pub struct Leader {
    task_ready_to_commit: Receiver<u32>,
    broadcast_to_follower: Sender<LeaderMsg>,

    pub wake_up: Arc<SignalEvent>,

    new_followers: Arc<AtomicUsize>,
    active_followers: Arc<AtomicUsize>,

    batch_id: Arc<RwLock<u32>>,
}

impl Leader {
    pub fn new(listen_on: impl ToSocketAddrs + Send + 'static) -> Leader {
        let new_followers = Arc::new(AtomicUsize::new(0));
        let active_followers = Arc::new(AtomicUsize::new(1));
        let wake_up = Arc::new(SignalEvent::auto(true));
        let (broadcast_to_follower, process_batch_receiver) = unbounded();
        let (task_finished_sender, task_finished_receiver) = unbounded();

        let nf = new_followers.clone();
        let af = active_followers.clone();
        let wu = wake_up.clone();
        std::thread::spawn(move || {
            Self::listener(listen_on, nf, af, wu, process_batch_receiver, task_finished_sender)
        });

        Leader {
            task_ready_to_commit: task_finished_receiver,
            broadcast_to_follower,

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
        new_followers: Arc<AtomicUsize>,
        active_followers: Arc<AtomicUsize>,
        wake_up: Arc<SignalEvent>,
        broadcast_to_follower: Receiver<LeaderMsg>,
        task_finished: Sender<u32>,
    ) {
        let listener: ChannelServer<LeaderMsg, FollowerMsg> =
            ChannelServer::bind(listen_on).unwrap();

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

    pub fn wake_up(&self) {
        todo!()
    }

    pub fn join_me(&self, dump: Vec<u8>) {
        self.broadcast_to_follower
            .send(LeaderMsg::JoinFromDump(dump))
            .expect("Lost the link with the followers");
    }

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

        // if zero nodes needs to be sync we can commit right away and early exit
        if consistency_level != Consistency::One {
            // else, we wait till enough nodes are ready to commit
            for ready_to_commit in self
                .task_ready_to_commit
                .iter()
                // we need to filter out the messages from the old batches
                .filter(|id| *id == *batch_id)
                .enumerate()
                // we do a +2 because enumerate starts at 1 and we must includes ourselves in the count
                .map(|(id, _)| id + 2)
            {
                // TODO: if the last node dies we're stuck on the iterator

                // we need to reload the cluster size everytime in case a node dies
                let size = self.active_followers.load(atomic::Ordering::Relaxed);

                info!("{ready_to_commit} nodes are ready to commit for a cluster size of {size}");
                match consistency_level {
                    Consistency::Two if ready_to_commit >= 1 => break,
                    Consistency::Quorum if ready_to_commit >= (size / 2) => break,
                    Consistency::All if ready_to_commit == size => break,
                    _ => (),
                }
            }
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
}
