use std::error::Error;

use structopt::StructOpt;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use meilisearch_raft::AppRaft;

#[derive(Debug, StructOpt)]
struct Opt {
    addr: String,
    master_addr: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let _opt = Opt::from_args();

    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                println!("Line: {}", line);
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

    Ok(())
}
