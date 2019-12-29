use std::collections::HashMap;

use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;
use env_logger;

use meilisearch_raft::IndexServer as Raft;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long, default_value = "0")]
    id: u64,
    #[structopt(long, default_value = "127.0.0.1")]
    host: String,
    #[structopt(long, default_value = "7800")]
    port: u16,
    #[structopt(long)]
    peer: Option<String>
}

fn main() {
    env_logger::Builder::from_default_env().init();
    let opt = Opt::from_args();
    let mut rl = Editor::<()>::new();

    let mut map = HashMap::new();
    opt.peer.map(|peer| {
        let mut parts = peer.split("=");
        map.insert(parts.next().unwrap().parse::<u64>().unwrap(), parts.next().unwrap().to_string());
    });

    let raft = Raft::start_server(opt.id, &opt.host, opt.port, map);
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if &line == "probe" {
                    println!("{}", raft.clerk().probe());
                } else if &line == "peers" {
                    println!("{}", raft.clerk().peers());
                } else {
                    raft.clerk().put("1", &line);
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    ()
}
