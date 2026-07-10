# iedb-agent

IotEdgeDB agent — a Rust-based edge service for collecting, buffering, and forwarding IoT sensor data.

## Prerequisites

- Rust toolchain (stable)
- For ARM32 cross-compilation: `gcc-arm-linux-gnueabihf` (see below)

## Build (native)

```bash
cargo build --release
```

## Configuration

Copy and edit the example config:

```bash
cp iedb-agent.toml.example iedb-agent.toml
```

## ARM32 Cross-Compilation (armv7-unknown-linux-gnueabihf)

### 1. Install the ARM32 target

```bash
rustup target add armv7-unknown-linux-gnueabihf
```

### 2. Install the cross-compiler

**Ubuntu/Debian:**
```bash
apt install gcc-arm-linux-gnueabihf
```

**macOS (Homebrew):**
```bash
brew install arm-linux-gnueabihf-binutils
```

### 3. Build

```bash
cargo build --target armv7-unknown-linux-gnueabihf --release
```

The linker configuration is in `cross/armv7-unknown-linux-gnueabihf.toml`.
