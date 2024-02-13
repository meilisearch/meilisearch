use std::collections::vec_deque::Drain;
use std::collections::VecDeque;
use std::io::{self, BufReader, BufWriter, Stdout, Write};
use std::mem;

use anyhow::Context;
use byte_unit::Byte;
use clap::Parser;
use tracing_trace::entry::{Entry, NewSpan};

/// A program that filters trace logs to only keeps
/// the logs related to memory usage above the given threshold.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The threshold that a log must have to be returned by this program.
    #[arg(short, long)]
    memory_threshold: Byte,

    /// Number of context lines to keep around high memory log lines.
    #[arg(long, default_value_t = 10)]
    context: usize,
}

fn main() -> anyhow::Result<()> {
    let Args { memory_threshold, context } = Args::parse();

    let mut context = EntryContext::new(context);
    let mut currently_in_threshold = false;

    let input = BufReader::new(io::stdin());
    let mut output = io::BufWriter::new(io::stdout());
    for result in tracing_trace::TraceReader::new(input) {
        let entry = result?;

        match entry {
            Entry::NewCallsite(_) | Entry::NewThread(_) => {
                write_to_output(&mut output, &entry)?;
            }
            Entry::NewSpan(NewSpan { id, call_id, parent_id, thread_id }) => todo!(),
            Entry::SpanEnter(_) => todo!(),
            Entry::SpanExit(_) => todo!(),
            Entry::SpanClose(_) => todo!(),
            Entry::Event(_) => todo!(),
        }

        // if matches!(entry, Entry::NewCallsite(_) | Entry::NewThread(_)) {
        //     write_to_output(&mut output, &entry)?;
        // } else if entry.memory().map_or(true, |m| m.resident < memory_threshold.as_u64()) {
        //     if mem::replace(&mut currently_in_threshold, false) {
        //         for entry in context.drain() {
        //             write_to_output(&mut output, &entry)?;
        //         }
        //     }
        //     context.push(entry);
        // } else {
        //     currently_in_threshold = true;
        //     for entry in context.drain() {
        //         write_to_output(&mut output, &entry)?;
        //     }
        //     write_to_output(&mut output, &entry)?;
        // }
    }

    for entry in context.drain() {
        write_to_output(&mut output, &entry)?;
    }

    output.flush().context("flushing stdout")?;
    Ok(())
}

fn write_to_output(writer: &mut BufWriter<Stdout>, entry: &Entry) -> anyhow::Result<()> {
    serde_json::to_writer(writer, &entry).context("while serializing and writing to stdout")
}

/// Keeps only the last `size` element in memory.
/// It's basically a sliding window.
pub struct EntryContext {
    size: usize,
    queue: VecDeque<Entry>,
}

impl EntryContext {
    pub fn new(size: usize) -> EntryContext {
        EntryContext { size, queue: VecDeque::with_capacity(size) }
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.queue.len()
    }

    pub fn push(&mut self, entry: Entry) {
        if self.queue.len() == self.size {
            self.queue.pop_front();
        }
        self.queue.push_back(entry);
    }

    pub fn drain(&mut self) -> Drain<Entry> {
        self.queue.drain(..)
    }
}
