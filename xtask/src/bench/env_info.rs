use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub repo_url: Option<String>,
    pub branch_or_tag: String,
    pub commit_id: String,
    pub commit_msg: String,
    pub author_name: String,
    pub author_email: String,
    pub committer_name: String,
    pub committer_email: String,
}

impl Source {
    pub fn from_repo(
        path: impl AsRef<std::path::Path>,
    ) -> Result<(Self, OffsetDateTime), git2::Error> {
        use git2::Repository;

        let repo = Repository::open(path)?;
        let remote = repo.remotes()?;
        let remote = remote.get(0).expect("No remote associated to the repo");
        let remote = repo.find_remote(remote)?;

        let head = repo.head()?;

        let commit = head.peel_to_commit()?;

        let time = OffsetDateTime::from_unix_timestamp(commit.time().seconds()).unwrap();

        let author = commit.author();
        let committer = commit.committer();

        Ok((
            Self {
                repo_url: remote.url().map(|s| s.to_string()),
                branch_or_tag: head.name().unwrap().to_string(),
                commit_id: commit.id().to_string(),
                commit_msg: String::from_utf8_lossy(commit.message_bytes())
                    .to_string()
                    .lines()
                    .next()
                    .map_or(String::new(), |s| s.to_string()),
                author_name: author.name().unwrap().to_string(),
                author_email: author.email().unwrap().to_string(),
                committer_name: committer.name().unwrap().to_string(),
                committer_email: committer.email().unwrap().to_string(),
            },
            time,
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Environment {
    pub hostname: Option<String>,
    pub cpu: String,

    /// Advertised or nominal clock speed in Hertz.
    pub clock_speed: u64,

    /// Total number of bytes of memory provided by the system. */
    pub memory: u64,
    pub os_type: String,
    pub software: Vec<VersionInfo>,

    pub user_name: String,

    /// Is set true when the data was gathered by a manual run,
    /// possibly on a developer machine, instead of the usual benchmark server.
    pub manual_run: bool,
}

impl Environment {
    pub fn generate_from_current_config() -> Self {
        use sysinfo::System;

        let unknown_string = String::from("Unknown");
        let mut system = System::new();
        system.refresh_cpu();
        system.refresh_cpu_frequency();
        system.refresh_memory();

        let (cpu, frequency) = match system.cpus().first() {
            Some(cpu) => (
                format!("{} @ {:.2}GHz", cpu.brand(), cpu.frequency() as f64 / 1000.0),
                cpu.frequency() * 1_000_000,
            ),
            None => (unknown_string.clone(), 0),
        };

        let mut software = Vec::new();
        if let Some(distribution) = System::name() {
            software
                .push(VersionInfo { name: distribution, version: String::from("distribution") });
        }
        if let Some(kernel) = System::kernel_version() {
            software.push(VersionInfo { name: kernel, version: String::from("kernel") });
        }
        if let Some(os) = System::os_version() {
            software.push(VersionInfo { name: os, version: String::from("kernel-release") });
        }
        if let Some(arch) = System::cpu_arch() {
            software.push(VersionInfo { name: arch, version: String::from("arch") });
        }

        Self {
            hostname: System::host_name(),
            cpu,
            clock_speed: frequency,
            memory: system.total_memory(),
            os_type: System::long_os_version().unwrap_or(unknown_string.clone()),
            user_name: System::name().unwrap_or(unknown_string.clone()),
            manual_run: false,
            software,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionInfo {
    pub name: String,
    pub version: String,
}
