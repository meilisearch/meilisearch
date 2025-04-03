use serde::{Deserialize, Serialize};

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
        system.refresh_cpu_all();
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
        software.push(VersionInfo { name: System::cpu_arch(), version: String::from("arch") });

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
