# lau-port-v2

Async-native message port system that replaces `blocking_lock()` deadlock risks found in hermes-construct's `port.rs` with proper `tokio::mpsc`-backed ports.

## The Problem

`hermes-construct` uses `blocking_lock()` inside async code (`port.rs:102`), which is a latent deadlock on a current-thread tokio runtime.

## Architecture

| Type | Description |
|------|-------------|
| `AsyncPort` | Async trait — no `blocking_lock()` anywhere |
| `ChannelPort` | `tokio::mpsc` paired ports (the core fix) |
| `BroadcastPort` | Fan-out to multiple subscribers |
| `MultiplexPort` | Unified receive across multiple ports |
| `BufferedPort` | Overflow-protected buffer wrapper |

## Usage

```rust
use lau_port_v2::{ChannelPort, AsyncPort, PortMessage};

#[tokio::main]
async fn main() {
    let (mut client, mut server) = ChannelPort::pair(32);
    
    client.send(PortMessage::new("client", "hello")).await.unwrap();
    let msg = server.receive().await.unwrap().unwrap();
    println!("Got: {}", msg.content);
}
```

## License

MIT
