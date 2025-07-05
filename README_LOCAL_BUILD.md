# Local Build and Run Instructions for Meilisearch

This comprehensive guide details the process for building and running Meilisearch from source. It includes step-by-step instructions, troubleshooting tips, and a video demonstration of the local web interface.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Repository Setup](#1-repository-setup)
3. [Documentation Review](#2-documentation-review)
4. [Development Environment](#3-development-environment)
5. [Build Process](#4-build-process)
6. [Server Configuration](#5-server-configuration)
7. [Verification Steps](#6-verification-steps)
8. [Web Interface](#7-web-interface)
9. [Video Demonstration](#8-video-demonstration)
10. [Troubleshooting](#troubleshooting)
11. [References](#references)

## Prerequisites

Before starting, ensure you have:
- Git installed
- A Unix-like environment (macOS, Linux, or WSL for Windows)
- At least 2GB of available RAM
- 1GB of free disk space
- Internet connection for downloading dependencies

---

## 1. Repository Setup

Clone the Meilisearch repository and navigate to the project directory:
```sh
git clone https://github.com/stix26/meilisearch.git
cd meilisearch
```

Verify you're in the correct directory:
```sh
pwd  # Should show your meilisearch directory path
ls   # Should show project files including Cargo.toml
```

---

## 2. Documentation Review

Essential documentation to review before building:
- `README.md` - Project overview and features
- `CONTRIBUTING.md` - Development guidelines
- `Cargo.toml` - Project dependencies and configuration

Key points to understand:
- Meilisearch is built in Rust
- Uses Cargo as the build system
- Requires specific Rust features and dependencies

---

## 3. Development Environment

### Install Rust Toolchain
First, verify if Rust is installed:
```sh
rustup --version
cargo --version
```

If not installed, use rustup:
```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env  # Load the new PATH settings
```

### Verify Rust Installation
```sh
rustc --version    # Should show Rust version
cargo --version    # Should show Cargo version
rustup show       # Should show stable toolchain
```

---

## 4. Build Process

### Development Build
For development and testing:
```sh
cargo build
```

### Production Build
For optimal performance:
```sh
cargo build --release
```

Build artifacts location:
- Debug build: `./target/debug/meilisearch`
- Release build: `./target/release/meilisearch`

Expected build time: 5-15 minutes depending on your system.

---

## 5. Server Configuration

### Basic Start
```sh
./target/release/meilisearch
```

### Recommended Configuration
```sh
./target/release/meilisearch \
  --no-analytics \          # Disable telemetry
  --log-level INFO \       # Set logging verbosity
  --db-path ./meili_data \ # Specify data directory
  --env development        # Set environment
```

### Environment Variables
Optional configuration through environment variables:
```sh
export MEILI_MASTER_KEY="your-secret-key"    # Set master key
export MEILI_NO_ANALYTICS=true               # Disable analytics
export MEILI_LOG_LEVEL=INFO                  # Set log level
```

---

## 6. Verification Steps

### Check Server Status
Verify the server is running:
```sh
lsof -i :7700 | grep LISTEN
```

### Check Logs
Monitor server logs:
```sh
tail -f meili_data/meilisearch.log
```

### Test API Availability
```sh
curl http://localhost:7700/health
# Should return {"status": "available"}
```

---

## 7. Web Interface

Access the web interface at:
```
http://localhost:7700
```

Default credentials:
- No authentication required in development mode
- Use master key if configured

---

## 8. Video Demonstration

A comprehensive video demonstration of the local web interface is available here:
[Video Demonstration](https://drive.google.com/file/d/1omLlU_LtPbnPsdc7_lDhuShi0ydLFYQs/view?usp=sharing)

This video covers:
- Initial setup verification
- Interface navigation
- Feature demonstration
- Comparison with cloud version

---

## Troubleshooting

### Common Issues and Solutions

1. **Cargo.toml Not Found**
   ```sh
   error: could not find `Cargo.toml`
   ```
   Solution:
   - Verify current directory: `pwd`
   - Navigate to project root: `cd path/to/meilisearch`
   - Confirm file exists: `ls Cargo.toml`

2. **Build Failures**
   ```sh
   error: failed to compile
   ```
   Solutions:
   - Update Rust: `rustup update stable`
   - Clean build: `cargo clean && cargo build`
   - Check dependencies: `cargo check`

3. **Server Won't Start**
   ```sh
   Error: Address already in use
   ```
   Solutions:
   - Check running processes: `lsof -i :7700`
   - Kill existing process: `kill $(lsof -t -i :7700)`
   - Try different port: `./meilisearch --http-addr 'localhost:7701'`

4. **Permission Denied**
   ```sh
   Permission denied (os error 13)
   ```
   Solution:
   ```sh
   chmod +x ./target/release/meilisearch
   ```

### UI Differences
- Local interface may differ from cloud version
- Refer to video demonstration for expected appearance
- Core functionality remains consistent

---

## References

### Official Resources
- [Meilisearch GitHub Repository](https://github.com/stix26/meilisearch.git)
- [Meilisearch Documentation](https://www.meilisearch.com/docs/)
- [API Reference](https://docs.meilisearch.com/reference/api/)

### Additional Resources
- [Video Demonstration](https://drive.google.com/file/d/1omLlU_LtPbnPsdc7_lDhuShi0ydLFYQs/view?usp=sharing)
- [Rust Installation Guide](https://www.rust-lang.org/tools/install)
- [Cargo Documentation](https://doc.rust-lang.org/cargo/) 