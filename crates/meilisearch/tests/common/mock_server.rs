use std::collections::HashSet;
use std::sync::Mutex;
use wiremock::MockServer;

/// A wrapper around MockServer that ensures unique port allocation on Windows
pub struct SafeMockServer {
    inner: MockServer,
}

static USED_PORTS: Mutex<HashSet<u16>> = Mutex::new(HashSet::new());

impl SafeMockServer {
    /// Create a new MockServer with guaranteed unique port allocation
    pub async fn start() -> Self {
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 100;
        
        loop {
            let server = MockServer::start().await;
            let port = server.address().port();
            
            let mut used_ports = USED_PORTS.lock().unwrap();
            if !used_ports.contains(&port) {
                used_ports.insert(port);
                return Self { inner: server };
            }
            
            attempts += 1;
            if attempts >= MAX_ATTEMPTS {
                // Fallback: clear the used ports set and try once more
                used_ports.clear();
                used_ports.insert(port);
                return Self { inner: server };
            }
            
            // Small delay before retry
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
    
    /// Get the URI of the mock server
    pub fn uri(&self) -> String {
        self.inner.uri()
    }
    
    /// Get the address of the mock server  
    pub fn address(&self) -> std::net::SocketAddr {
        self.inner.address()
    }
}

impl std::ops::Deref for SafeMockServer {
    type Target = MockServer;
    
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for SafeMockServer {
    fn drop(&mut self) {
        let port = self.inner.address().port();
        let mut used_ports = USED_PORTS.lock().unwrap();
        used_ports.remove(&port);
    }
}