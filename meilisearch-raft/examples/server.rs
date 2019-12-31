use std::collections::HashMap;

use env_logger;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

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
    peer: Option<String>,
}

fn main() {
    env_logger::Builder::from_default_env().init();
    let opt = Opt::from_args();
    let mut rl = Editor::<()>::new();

    let mut map = HashMap::new();
    opt.peer.map(|peer| {
        let mut parts = peer.split("=");
        map.insert(
            parts.next().unwrap().parse::<u64>().unwrap(),
            parts.next().unwrap().to_string(),
        );
    });

    let mut raft = Raft::start_server(opt.id, &opt.host, opt.port, map);
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if &line == "peers" {
                    println!("{}", raft.get_peers());
                } else {
                    raft.put_data("1", &line);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    ()
}
