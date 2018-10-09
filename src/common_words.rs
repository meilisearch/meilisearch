use std::io::{self, BufReader, BufRead};
use std::collections::HashSet;
use std::path::Path;
use std::fs::File;

#[derive(Debug)]
pub struct CommonWords(HashSet<String>);

impl CommonWords {
    pub fn from_file<P>(path: P) -> io::Result<Self>
    where P: AsRef<Path>
    {
        let file = File::open(path)?;
        let file = BufReader::new(file);
        let mut set = HashSet::new();
        for line in file.lines().filter_map(|l| l.ok()) {
            let word = line.trim().to_owned();
            set.insert(word);
        }
        Ok(CommonWords(set))
    }

    pub fn contains(&self, word: &str) -> bool {
        self.0.contains(word)
    }
}
