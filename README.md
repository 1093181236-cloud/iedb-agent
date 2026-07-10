# iedb-agent

IotEdgeDB agent — a Rust-based edge service for collecting, buffering, and forwarding IoT sensor data.

## Prerequisites

- Rust toolchain (stable)
- For ARM32 cross-compilation: `cargo-zigbuild` (recommended) or ARM GCC cross-compiler

## Build (native)

```bash
cargo build --release
```

## Configuration

Copy and edit the example config:

```bash
cp iedb-agent.toml.example iedb-agent.toml
```

## ARM32 Cross-Compilation

Target device: ARMv7 Linux (e.g., Raspberry Pi, embedded boards).

### Method 1: cargo-zigbuild (recommended)

Zig bundles cross-compilation sysroots for many targets including ARM32 musl.
Produces a **statically linked** binary with no GLIBC dependency.

```bash
# Install
cargo install cargo-zigbuild

# Build (musl, static, stripped)
cargo zigbuild --target armv7-unknown-linux-musleabihf --release
```

Binary: `target/armv7-unknown-linux-musleabihf/release/iedb-agent`
(ELF 32-bit ARM, statically linked, ~5MB)

### Method 2: GNU cross-compiler (macOS Homebrew)

Produces a dynamically linked binary that requires GLIBC >= 2.28 on the target.

```bash
# Install Rust target
rustup target add armv7-unknown-linux-gnueabihf

# Install cross-compiler (macOS)
brew install messense/macos-cross-toolchains/armv7-unknown-linux-gnueabihf

# Create symlinks (cc crate searches for abbreviated prefix)
cd /opt/homebrew/bin
for tool in gcc g++ cc ar ld; do
  ln -sf "armv7-unknown-linux-gnueabihf-$tool" "arm-linux-gnueabihf-$tool"
done

# Build from project root
cd /path/to/iedb-agent
cargo build --target armv7-unknown-linux-gnueabihf --release
```

Binary: `target/armv7-unknown-linux-gnueabihf/release/iedb-agent`
(ELF 32-bit ARM, dynamically linked, ~8MB, requires GLIBC >= 2.28)

### Method 3: Native build on device

If the ARM32 device has Rust installed:

```bash
# On the device
cargo build --release
```
