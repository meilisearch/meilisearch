use std::io::{self, BufReader, BufRead};
use std::collections::HashSet;
use std::path::Path;
use std::fs::File;

pub type CommonWords = HashSet<String>;

pub fn from_file<P>(path: P) -> io::Result<CommonWords>
where P: AsRef<Path>,
{
    let file = File::open(path)?;
    let file = BufReader::new(file);
    let mut set = HashSet::new();
    for line in file.lines().filter_map(|l| l.ok()) {
        for word in line.split_whitespace() {
            set.insert(word.to_owned());
        }
    }
    Ok(set)
}
