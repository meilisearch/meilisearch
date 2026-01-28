pub mod reqwest;
pub mod ureq;

pub mod policy;

#[cfg(test)]
mod tests {
    use crate::policy::IpPolicy;

    #[test]
    fn ureq_direct_ip() {
        let config = crate::ureq::config::Config::builder()
            .prepare(|builder| builder.timeout_global(Some(std::time::Duration::from_millis(100))))
            .build();
        let agent = crate::ureq::Agent::new_with_config(config, IpPolicy::deny_all_local_ips());
        assert!(matches!(
            agent.post("http://10.0.0.2:7700").send("HELO"),
            Err(::ureq::Error::BadUri(_))
        ));
    }

    #[test]
    fn ureq_redirect() {}

    #[test]
    fn ureq_hostname() {
        let config = crate::ureq::config::Config::builder()
            .prepare(|builder| builder.timeout_global(Some(std::time::Duration::from_millis(100))))
            .build();
        let agent = crate::ureq::Agent::new_with_config(config, IpPolicy::deny_all_local_ips());
        assert!(matches!(agent.get("https://localhost").call(), Err(::ureq::Error::BadUri(_))));
    }

    #[test]
    fn reqwest_direct_ip() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let client = crate::reqwest::ClientBuilder::new()
                .prepare(|builder| builder.timeout(std::time::Duration::from_millis(100)))
                .build_with_policies(IpPolicy::deny_all_local_ips(), Default::default())
                .unwrap();
            let err = client.get("http://10.0.0.1:7700").send().await.unwrap_err();
            let mut origin: &dyn std::error::Error = &err;
            while let Some(source) = origin.source() {
                origin = source;
            }

            assert_eq!(format!("{origin:?}"), "Policy(DeniedLocalIp)");
        })
    }

    #[test]
    fn reqwest_redirect() {}

    #[test]
    fn reqwest_hostname() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let client = crate::reqwest::ClientBuilder::new()
                .build_with_policies(IpPolicy::deny_all_local_ips(), Default::default())
                .unwrap();
            let err = client.get("http://localhost:7700").send().await.unwrap_err();
            let crate::reqwest::Error::Reqwest(err) = err else {
                panic!("Errors during DNS resolution are reqwest errors")
            };
            let mut origin: &dyn std::error::Error = &err;
            while let Some(source) = origin.source() {
                origin = source;
            }

            assert_eq!(format!("{origin:?}"), "DeniedLocalIp");
        })
    }
}
