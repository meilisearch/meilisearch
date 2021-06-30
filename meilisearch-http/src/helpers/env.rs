use walkdir::WalkDir;

pub trait EnvSizer {
    fn size(&self) -> u64;
}

impl EnvSizer for heed::Env {
    fn size(&self) -> u64 {
        WalkDir::new(self.path())
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len())
    }
}
