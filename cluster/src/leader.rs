use std::net::ToSocketAddrs;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc, Mutex};

use bus::{Bus, BusReader};
use crossbeam::channel::{unbounded, Receiver, Sender};
use ductile::{ChannelReceiver, ChannelSender, ChannelServer};
use log::info;

use crate::batch::Batch;
use crate::{Consistency, FollowerMsg, LeaderMsg};

pub struct Leader {
    task_ready_to_commit: Receiver<u32>,
    broadcast_to_follower: Sender<LeaderMsg>,

    cluster_size: Arc<AtomicUsize>,

    batch_id: u32,
}

impl Leader {
    pub fn new(listen_on: impl ToSocketAddrs + Send + 'static) -> Leader {
        let cluster_size = Arc::new(AtomicUsize::new(1));
        let (process_batch_sender, process_batch_receiver) = unbounded();
        let (task_finished_sender, task_finished_receiver) = unbounded();

        let cs = cluster_size.clone();
        std::thread::spawn(move || {
            Self::listener(listen_on, cs, process_batch_receiver, task_finished_sender)
        });

        Leader {
            task_ready_to_commit: task_finished_receiver,
            broadcast_to_follower: process_batch_sender,
            cluster_size,
            batch_id: 0,
        }
    }

    /// Takes all the necessary channels to chat with the scheduler and give them
    /// to each new followers
    fn listener(
        listen_on: impl ToSocketAddrs,
        cluster_size: Arc<AtomicUsize>,
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
            let cs = cluster_size.clone();

            let process_batch = bus.lock().unwrap().add_rx();

            std::thread::spawn(move || {
                Self::follower(sender, receiver, cs, process_batch, task_finished)
            });
        }
    }

    /// Allow a follower to chat with the scheduler
    fn follower(
        sender: ChannelSender<LeaderMsg>,
        receiver: ChannelReceiver<FollowerMsg>,
        cluster_size: Arc<AtomicUsize>,
        mut broadcast_to_follower: BusReader<LeaderMsg>,
        task_finished: Sender<u32>,
    ) {
        let size = cluster_size.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        info!("A new follower joined the cluster. {} members.", size);

        // send messages to the follower
        std::thread::spawn(move || loop {
            let msg = broadcast_to_follower.recv().expect("Main thread died");
            if sender.send(msg).is_err() {
                // the follower died, the logging and cluster size update should be done
                // in the other thread
                break;
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
        let size = cluster_size.fetch_sub(1, atomic::Ordering::Relaxed) - 1;
        info!("A follower left the cluster. {} members.", size);
    }

    pub fn starts_batch(&mut self, batch: Batch) {
        assert!(
            self.batch_id % 2 == 0,
            "Tried to start processing a batch before commiting the previous one"
        );
        self.batch_id += 1;

        self.broadcast_to_follower
            .send(LeaderMsg::StartBatch { id: self.batch_id, batch })
            .expect("Can't reach the cluster");
    }

    pub fn commit(&mut self, consistency_level: Consistency) {
        // if zero nodes needs to be sync we can commit right away and early exit
        if consistency_level != Consistency::Zero {
            // else, we wait till enough nodes are ready to commit
            for (ready_to_commit, _id) in self
                .task_ready_to_commit
                .iter()
                // we need to filter out the messages from the old batches
                .filter(|id| *id == self.batch_id)
                .enumerate()
            {
                let cluster_size = self.cluster_size.load(atomic::Ordering::Relaxed);

                match consistency_level {
                    Consistency::One if ready_to_commit >= 1 => break,
                    Consistency::Two if ready_to_commit >= 2 => break,
                    Consistency::Quorum if ready_to_commit >= (cluster_size / 2) => break,
                    _ => (),
                }
            }
        }

        self.broadcast_to_follower.send(LeaderMsg::Commit(self.batch_id)).unwrap();

        self.batch_id += 1;
    }
}
