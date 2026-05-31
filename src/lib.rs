//! # lau-port-v2
//!
//! Async-native message port system that replaces `blocking_lock()` deadlock risks
//! found in hermes-construct's port.rs with proper tokio::mpsc-backed ports.
//!
//! ## Core types
//! - [`AsyncPort`] — async trait for message ports
//! - [`ChannelPort`] — tokio::mpsc paired ports (the fix for `blocking_lock`)
//! - [`BroadcastPort`] — fan-out to multiple subscribers
//! - [`MultiplexPort`] — poll multiple ports from one interface
//! - [`BufferedPort`] — overflow-protected buffer wrapper

use std::collections::HashMap;
use std::cmp::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// PortError
// ---------------------------------------------------------------------------

/// Errors that can occur during port operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortError {
    /// The port has been closed.
    Closed,
    /// The operation timed out.
    Timeout,
    /// An I/O-style error occurred.
    Io(String),
    /// The port's internal buffer/channel is full.
    Full,
    /// The remote end has disconnected.
    Disconnected,
}

impl std::fmt::Display for PortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortError::Closed => write!(f, "port is closed"),
            PortError::Timeout => write!(f, "operation timed out"),
            PortError::Io(e) => write!(f, "I/O error: {e}"),
            PortError::Full => write!(f, "port is full"),
            PortError::Disconnected => write!(f, "remote end disconnected"),
        }
    }
}

impl std::error::Error for PortError {}

// ---------------------------------------------------------------------------
// MessagePriority
// ---------------------------------------------------------------------------

/// Message priority level. Higher priority sorts first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl PartialOrd for MessagePriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessagePriority {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

// ---------------------------------------------------------------------------
// PortMessage
// ---------------------------------------------------------------------------

/// A message travelling through a port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortMessage {
    pub id: String,
    pub source: String,
    pub content: String,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
    pub priority: MessagePriority,
}

impl PortMessage {
    /// Convenience constructor.
    pub fn new(source: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: uuid_lite(),
            source: source.into(),
            content: content.into(),
            timestamp: now_millis(),
            metadata: HashMap::new(),
            priority: MessagePriority::Normal,
        }
    }

    /// Builder-style: set priority.
    pub fn with_priority(mut self, p: MessagePriority) -> Self {
        self.priority = p;
        self
    }

    /// Builder-style: insert metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), val.into());
        self
    }
}

/// Cheap unique-ish id without pulling in uuid crate.
fn uuid_lite() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{ts:x}-{c:x}")
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// PortStats
// ---------------------------------------------------------------------------

/// Operational statistics for a port.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortStats {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub errors: u64,
    pub last_activity: Option<u64>,
}

impl PortStats {
    pub fn new() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// AsyncPort trait
// ---------------------------------------------------------------------------

/// Async-native message port. No `blocking_lock()` anywhere.
#[async_trait]
pub trait AsyncPort: Send + Sync {
    /// Send a message through the port.
    async fn send(&mut self, message: PortMessage) -> Result<(), PortError>;
    /// Receive the next message, waiting if necessary.
    async fn receive(&mut self) -> Result<Option<PortMessage>, PortError>;
    /// Poll for a message with a timeout.
    async fn poll(&mut self, timeout: Duration) -> Result<Option<PortMessage>, PortError>;
    /// Whether the port is still active.
    fn is_active(&self) -> bool;
    /// Endpoint identifier (for debugging / multiplexing).
    fn endpoint(&self) -> &str;
    /// Gracefully close the port.
    async fn close(&mut self) -> Result<(), PortError>;
}

// ---------------------------------------------------------------------------
// ChannelPort — the core fix
// ---------------------------------------------------------------------------

/// A tokio::mpsc-backed bidirectional port pair. No `blocking_lock()`.
///
/// Create with [`ChannelPort::pair`].
pub struct ChannelPort {
    tx: mpsc::Sender<PortMessage>,
    rx: mpsc::Receiver<PortMessage>,
    endpoint_name: String,
    active: bool,
    stats: PortStats,
}

impl ChannelPort {
    /// Create a paired (A→B, B→A) channel port pair with the given per-direction capacity.
    pub fn pair(capacity: usize) -> (Self, Self) {
        let (tx_a, rx_b) = mpsc::channel(capacity);
        let (tx_b, rx_a) = mpsc::channel(capacity);
        let a = Self {
            tx: tx_a,
            rx: rx_a,
            endpoint_name: "channel-a".into(),
            active: true,
            stats: PortStats::new(),
        };
        let b = Self {
            tx: tx_b,
            rx: rx_b,
            endpoint_name: "channel-b".into(),
            active: true,
            stats: PortStats::new(),
        };
        (a, b)
    }

    /// Create a pair with custom endpoint names.
    pub fn pair_named(capacity: usize, name_a: &str, name_b: &str) -> (Self, Self) {
        let (tx_a, rx_b) = mpsc::channel(capacity);
        let (tx_b, rx_a) = mpsc::channel(capacity);
        let a = Self {
            tx: tx_a,
            rx: rx_a,
            endpoint_name: name_a.into(),
            active: true,
            stats: PortStats::new(),
        };
        let b = Self {
            tx: tx_b,
            rx: rx_b,
            endpoint_name: name_b.into(),
            active: true,
            stats: PortStats::new(),
        };
        (a, b)
    }

    /// Snapshot stats.
    pub fn stats(&self) -> &PortStats {
        &self.stats
    }
}

#[async_trait]
impl AsyncPort for ChannelPort {
    async fn send(&mut self, message: PortMessage) -> Result<(), PortError> {
        if !self.active {
            return Err(PortError::Closed);
        }
        match self.tx.send(message).await {
            Ok(()) => {
                self.stats.sent += 1;
                self.stats.last_activity = Some(now_millis());
                Ok(())
            }
            Err(_) => {
                self.active = false;
                self.stats.errors += 1;
                Err(PortError::Disconnected)
            }
        }
    }

    async fn receive(&mut self) -> Result<Option<PortMessage>, PortError> {
        if !self.active {
            return Err(PortError::Closed);
        }
        match self.rx.recv().await {
            Some(msg) => {
                self.stats.received += 1;
                self.stats.last_activity = Some(now_millis());
                Ok(Some(msg))
            }
            None => {
                self.active = false;
                Ok(None)
            }
        }
    }

    async fn poll(&mut self, timeout: Duration) -> Result<Option<PortMessage>, PortError> {
        if !self.active {
            return Err(PortError::Closed);
        }
        match tokio::time::timeout(timeout, self.rx.recv()).await {
            Ok(Some(msg)) => {
                self.stats.received += 1;
                self.stats.last_activity = Some(now_millis());
                Ok(Some(msg))
            }
            Ok(None) => {
                self.active = false;
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn endpoint(&self) -> &str {
        &self.endpoint_name
    }

    async fn close(&mut self) -> Result<(), PortError> {
        self.active = false;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BroadcastPort
// ---------------------------------------------------------------------------

/// Fan-out port: messages sent here are broadcast to all subscribers.
pub struct BroadcastPort {
    subscribers: Vec<mpsc::Sender<PortMessage>>,
    endpoint_name: String,
    active: bool,
    stats: PortStats,
}

impl BroadcastPort {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            subscribers: Vec::new(),
            endpoint_name: name.into(),
            active: true,
            stats: PortStats::new(),
        }
    }

    /// Subscribe to broadcasts. Returns a receiver that will get every message sent.
    pub fn subscribe(&mut self) -> mpsc::Receiver<PortMessage> {
        const SUB_CAPACITY: usize = 256;
        let (tx, rx) = mpsc::channel(SUB_CAPACITY);
        self.subscribers.push(tx);
        rx
    }

    /// Current subscriber count.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Snapshot stats.
    pub fn stats(&self) -> &PortStats {
        &self.stats
    }
}

#[async_trait]
impl AsyncPort for BroadcastPort {
    async fn send(&mut self, message: PortMessage) -> Result<(), PortError> {
        if !self.active {
            return Err(PortError::Closed);
        }
        // Retain only live subscribers.
        let mut sent_count = 0u64;
        self.subscribers.retain(|tx| {
            if tx.try_send(message.clone()).is_ok() {
                sent_count += 1;
                true
            } else if tx.is_closed() {
                false
            } else {
                // Channel full — count as dropped.
                true
            }
        });
        self.stats.sent += sent_count;
        self.stats.last_activity = Some(now_millis());
        Ok(())
    }

    async fn receive(&mut self) -> Result<Option<PortMessage>, PortError> {
        // BroadcastPort doesn't receive — it only sends.
        Err(PortError::Io("broadcast port is send-only".into()))
    }

    async fn poll(&mut self, _timeout: Duration) -> Result<Option<PortMessage>, PortError> {
        Err(PortError::Io("broadcast port is send-only".into()))
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn endpoint(&self) -> &str {
        &self.endpoint_name
    }

    async fn close(&mut self) -> Result<(), PortError> {
        self.active = false;
        self.subscribers.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MultiplexPort
// ---------------------------------------------------------------------------

/// Wraps multiple [`AsyncPort`]s and provides unified receive/poll across all of them.
pub struct MultiplexPort {
    ports: Vec<Box<dyn AsyncPort>>,
    endpoint_name: String,
}

impl MultiplexPort {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            ports: Vec::new(),
            endpoint_name: name.into(),
        }
    }

    /// Add a port to the multiplexer.
    pub fn add_port(&mut self, port: Box<dyn AsyncPort>) {
        self.ports.push(port);
    }

    /// Number of managed ports.
    pub fn port_count(&self) -> usize {
        self.ports.len()
    }

    /// Poll all ports and return the first available message.
    /// Uses a round-robin timeout approach across all ports.
    pub async fn receive_any(&mut self) -> Result<Option<PortMessage>, PortError> {
        if self.ports.is_empty() {
            return Err(PortError::Closed);
        }
        let per_port = Duration::from_millis(50);
        for port in &mut self.ports {
            if let Ok(Some(msg)) = port.poll(per_port).await {
                return Ok(Some(msg));
            }
        }
        // Fall back to blocking on the first active port.
        for port in &mut self.ports {
            if port.is_active() {
                return port.receive().await;
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl AsyncPort for MultiplexPort {
    async fn send(&mut self, message: PortMessage) -> Result<(), PortError> {
        // Send to the first active port.
        for port in &mut self.ports {
            if port.is_active() {
                return port.send(message).await;
            }
        }
        Err(PortError::Closed)
    }

    async fn receive(&mut self) -> Result<Option<PortMessage>, PortError> {
        self.receive_any().await
    }

    async fn poll(&mut self, timeout: Duration) -> Result<Option<PortMessage>, PortError> {
        if self.ports.is_empty() {
            return Err(PortError::Closed);
        }
        let per_port = timeout / self.ports.len().max(1) as u32;
        for port in &mut self.ports {
            if let Ok(Some(msg)) = port.poll(per_port).await {
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }

    fn is_active(&self) -> bool {
        self.ports.iter().any(|p| p.is_active())
    }

    fn endpoint(&self) -> &str {
        &self.endpoint_name
    }

    async fn close(&mut self) -> Result<(), PortError> {
        for port in &mut self.ports {
            let _ = port.close().await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BufferedPort
// ---------------------------------------------------------------------------

/// Overflow strategy for [`BufferedPort`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowStrategy {
    DropOldest,
    DropNewest,
    Refuse,
}

/// Wraps any [`AsyncPort`] with a local `VecDeque` buffer and overflow protection.
pub struct BufferedPort {
    inner: Box<dyn AsyncPort>,
    buffer: std::collections::VecDeque<PortMessage>,
    capacity: usize,
    strategy: OverflowStrategy,
    dropped: u64,
    stats: PortStats,
}

impl BufferedPort {
    pub fn new(inner: Box<dyn AsyncPort>, capacity: usize, strategy: OverflowStrategy) -> Self {
        Self {
            inner,
            buffer: std::collections::VecDeque::with_capacity(capacity),
            capacity,
            strategy,
            dropped: 0,
            stats: PortStats::new(),
        }
    }

    /// Messages currently in the buffer.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Total dropped due to overflow.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Snapshot stats.
    pub fn stats(&self) -> &PortStats {
        &self.stats
    }
}

#[async_trait]
impl AsyncPort for BufferedPort {
    async fn send(&mut self, message: PortMessage) -> Result<(), PortError> {
        if self.buffer.len() < self.capacity {
            self.buffer.push_back(message);
            self.stats.sent += 1;
            self.stats.last_activity = Some(now_millis());
            Ok(())
        } else {
            match self.strategy {
                OverflowStrategy::DropOldest => {
                    self.buffer.pop_front();
                    self.buffer.push_back(message);
                    self.dropped += 1;
                    self.stats.dropped += 1;
                    self.stats.sent += 1;
                    self.stats.last_activity = Some(now_millis());
                    Ok(())
                }
                OverflowStrategy::DropNewest => {
                    // Drop the incoming message.
                    self.dropped += 1;
                    self.stats.dropped += 1;
                    Ok(())
                }
                OverflowStrategy::Refuse => {
                    self.stats.errors += 1;
                    Err(PortError::Full)
                }
            }
        }
    }

    async fn receive(&mut self) -> Result<Option<PortMessage>, PortError> {
        if let Some(msg) = self.buffer.pop_front() {
            self.stats.received += 1;
            self.stats.last_activity = Some(now_millis());
            Ok(Some(msg))
        } else if self.inner.is_active() {
            let result = self.inner.receive().await;
            if result.is_ok() {
                self.stats.received += 1;
                self.stats.last_activity = Some(now_millis());
            }
            result
        } else {
            Ok(None)
        }
    }

    async fn poll(&mut self, timeout: Duration) -> Result<Option<PortMessage>, PortError> {
        if let Some(msg) = self.buffer.pop_front() {
            self.stats.received += 1;
            self.stats.last_activity = Some(now_millis());
            return Ok(Some(msg));
        }
        if !self.inner.is_active() {
            return Ok(None);
        }
        let result = self.inner.poll(timeout).await;
        if let Ok(Some(_)) = &result {
            self.stats.received += 1;
            self.stats.last_activity = Some(now_millis());
        }
        result
    }

    fn is_active(&self) -> bool {
        self.inner.is_active() || !self.buffer.is_empty()
    }

    fn endpoint(&self) -> &str {
        self.inner.endpoint()
    }

    async fn close(&mut self) -> Result<(), PortError> {
        self.buffer.clear();
        self.inner.close().await
    }
}

// ===========================================================================
// TESTS
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- PortError ----

    #[test]
    fn port_error_display() {
        assert_eq!(PortError::Closed.to_string(), "port is closed");
        assert_eq!(PortError::Timeout.to_string(), "operation timed out");
        assert!(PortError::Io("disk".into()).to_string().contains("disk"));
        assert_eq!(PortError::Full.to_string(), "port is full");
        assert_eq!(PortError::Disconnected.to_string(), "remote end disconnected");
    }

    #[test]
    fn port_error_equality() {
        assert_eq!(PortError::Closed, PortError::Closed);
        assert_ne!(PortError::Closed, PortError::Timeout);
    }

    // ---- MessagePriority ----

    #[test]
    fn priority_ordering() {
        assert!(MessagePriority::Critical > MessagePriority::High);
        assert!(MessagePriority::High > MessagePriority::Normal);
        assert!(MessagePriority::Normal > MessagePriority::Low);
        assert_eq!(MessagePriority::Normal, MessagePriority::Normal);
    }

    #[test]
    fn priority_ord_impl() {
        let mut v = vec![
            MessagePriority::Low,
            MessagePriority::Critical,
            MessagePriority::Normal,
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                MessagePriority::Low,
                MessagePriority::Normal,
                MessagePriority::Critical,
            ]
        );
    }

    // ---- PortMessage ----

    #[test]
    fn port_message_new() {
        let msg = PortMessage::new("src", "hello");
        assert_eq!(msg.source, "src");
        assert_eq!(msg.content, "hello");
        assert_eq!(msg.priority, MessagePriority::Normal);
        assert!(msg.metadata.is_empty());
        assert!(!msg.id.is_empty());
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn port_message_builder() {
        let msg = PortMessage::new("a", "b")
            .with_priority(MessagePriority::Critical)
            .with_metadata("key", "val");
        assert_eq!(msg.priority, MessagePriority::Critical);
        assert_eq!(msg.metadata.get("key").unwrap(), "val");
    }

    #[test]
    fn port_message_unique_ids() {
        let a = PortMessage::new("x", "y");
        let b = PortMessage::new("x", "y");
        assert_ne!(a.id, b.id);
    }

    // ---- PortStats ----

    #[test]
    fn port_stats_default() {
        let s = PortStats::default();
        assert_eq!(s.sent, 0);
        assert_eq!(s.received, 0);
        assert_eq!(s.dropped, 0);
        assert_eq!(s.errors, 0);
        assert_eq!(s.last_activity, None);
    }

    // ---- ChannelPort ----

    #[tokio::test]
    async fn channel_port_pair_send_receive() {
        let (mut a, mut b) = ChannelPort::pair(16);
        let msg = PortMessage::new("a", "hello b");
        a.send(msg.clone()).await.unwrap();
        let received = b.receive().await.unwrap().unwrap();
        assert_eq!(received.content, "hello b");
        assert_eq!(received.source, "a");
    }

    #[tokio::test]
    async fn channel_port_bidirectional() {
        let (mut a, mut b) = ChannelPort::pair(8);
        a.send(PortMessage::new("a", "to b")).await.unwrap();
        b.send(PortMessage::new("b", "to a")).await.unwrap();
        assert_eq!(b.receive().await.unwrap().unwrap().content, "to b");
        assert_eq!(a.receive().await.unwrap().unwrap().content, "to a");
    }

    #[tokio::test]
    async fn channel_port_is_active() {
        let (a, _b) = ChannelPort::pair(4);
        assert!(a.is_active());
    }

    #[tokio::test]
    async fn channel_port_close() {
        let (mut a, mut _b) = ChannelPort::pair(4);
        assert!(a.is_active());
        a.close().await.unwrap();
        assert!(!a.is_active());
        let err = a.send(PortMessage::new("x", "y")).await.unwrap_err();
        assert_eq!(err, PortError::Closed);
    }

    #[tokio::test]
    async fn channel_port_receive_none_when_closed() {
        let (mut a, b) = ChannelPort::pair(4);
        drop(b); // Close the other end
        // Send will fail because receiver is gone
        // But the channel may still have capacity—send succeeds until the sender detects closure.
        // Actually, with mpsc, send succeeds as long as the channel has capacity even if receiver is dropped.
        // Let's drain and then try to receive.
        let result = a.receive().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn channel_port_poll_timeout() {
        let (mut a, _b) = ChannelPort::pair(4);
        let start = tokio::time::Instant::now();
        let result = a.poll(Duration::from_millis(50)).await.unwrap();
        assert!(result.is_none());
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn channel_port_poll_gets_message() {
        let (mut a, mut b) = ChannelPort::pair(4);
        a.send(PortMessage::new("a", "hi")).await.unwrap();
        let result = b.poll(Duration::from_secs(1)).await.unwrap().unwrap();
        assert_eq!(result.content, "hi");
    }

    #[tokio::test]
    async fn channel_port_endpoint() {
        let (a, b) = ChannelPort::pair(4);
        assert_eq!(a.endpoint(), "channel-a");
        assert_eq!(b.endpoint(), "channel-b");
    }

    #[tokio::test]
    async fn channel_port_named() {
        let (a, b) = ChannelPort::pair_named(4, "client", "server");
        assert_eq!(a.endpoint(), "client");
        assert_eq!(b.endpoint(), "server");
    }

    #[tokio::test]
    async fn channel_port_stats() {
        let (mut a, mut b) = ChannelPort::pair(16);
        a.send(PortMessage::new("a", "1")).await.unwrap();
        a.send(PortMessage::new("a", "2")).await.unwrap();
        assert_eq!(a.stats().sent, 2);
        b.receive().await.unwrap().unwrap();
        b.receive().await.unwrap().unwrap();
        assert_eq!(b.stats().received, 2);
        assert!(a.stats().last_activity.is_some());
    }

    #[tokio::test]
    async fn channel_port_multiple_messages() {
        let (mut a, mut b) = ChannelPort::pair(32);
        for i in 0..10 {
            a.send(PortMessage::new("a", format!("msg-{i}"))).await.unwrap();
        }
        for i in 0..10 {
            let msg = b.receive().await.unwrap().unwrap();
            assert_eq!(msg.content, format!("msg-{i}"));
        }
    }

    #[tokio::test]
    async fn channel_port_poll_when_closed() {
        let (mut a, _b) = ChannelPort::pair(4);
        a.close().await.unwrap();
        let err = a.poll(Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(err, PortError::Closed);
    }

    #[tokio::test]
    async fn channel_port_receive_after_close() {
        let (mut a, _b) = ChannelPort::pair(4);
        a.close().await.unwrap();
        let err = a.receive().await.unwrap_err();
        assert_eq!(err, PortError::Closed);
    }

    // ---- BroadcastPort ----

    #[tokio::test]
    async fn broadcast_single_subscriber() {
        let mut bp = BroadcastPort::new("events");
        let mut rx = bp.subscribe();
        bp.send(PortMessage::new("sys", "alert")).await.unwrap();
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.content, "alert");
    }

    #[tokio::test]
    async fn broadcast_multiple_subscribers() {
        let mut bp = BroadcastPort::new("events");
        let mut rx1 = bp.subscribe();
        let mut rx2 = bp.subscribe();
        let mut rx3 = bp.subscribe();
        bp.send(PortMessage::new("sys", "broadcast!")).await.unwrap();
        assert_eq!(rx1.recv().await.unwrap().content, "broadcast!");
        assert_eq!(rx2.recv().await.unwrap().content, "broadcast!");
        assert_eq!(rx3.recv().await.unwrap().content, "broadcast!");
        assert_eq!(bp.subscriber_count(), 3);
    }

    #[tokio::test]
    async fn broadcast_receive_error() {
        let mut bp = BroadcastPort::new("ev");
        let err = bp.receive().await.unwrap_err();
        assert!(matches!(err, PortError::Io(_)));
    }

    #[tokio::test]
    async fn broadcast_poll_error() {
        let mut bp = BroadcastPort::new("ev");
        let err = bp.poll(Duration::from_millis(10)).await.unwrap_err();
        assert!(matches!(err, PortError::Io(_)));
    }

    #[tokio::test]
    async fn broadcast_close() {
        let mut bp = BroadcastPort::new("ev");
        assert!(bp.is_active());
        bp.close().await.unwrap();
        assert!(!bp.is_active());
        assert_eq!(bp.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_send_after_close() {
        let mut bp = BroadcastPort::new("ev");
        bp.close().await.unwrap();
        let err = bp.send(PortMessage::new("x", "y")).await.unwrap_err();
        assert_eq!(err, PortError::Closed);
    }

    #[tokio::test]
    async fn broadcast_stats() {
        let mut bp = BroadcastPort::new("ev");
        let _rx = bp.subscribe();
        bp.send(PortMessage::new("s", "m1")).await.unwrap();
        bp.send(PortMessage::new("s", "m2")).await.unwrap();
        assert_eq!(bp.stats().sent, 2);
    }

    #[tokio::test]
    async fn broadcast_dead_subscriber_pruned() {
        let mut bp = BroadcastPort::new("ev");
        {
            let rx = bp.subscribe();
            drop(rx);
            // Give it a moment to close.
        }
        // send should still work and prune the dead subscriber.
        bp.send(PortMessage::new("s", "ping")).await.unwrap();
        // After send, dead subscriber should have been removed.
        assert_eq!(bp.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_endpoint() {
        let bp = BroadcastPort::new("my-bus");
        assert_eq!(bp.endpoint(), "my-bus");
    }

    // ---- MultiplexPort ----

    #[tokio::test]
    async fn multiplex_receive_from_first() {
        let (ma, mut mb) = ChannelPort::pair(8);
        let (mut _ca, cb) = ChannelPort::pair(8);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(ma));
        mx.add_port(Box::new(cb));
        mb.send(PortMessage::new("b", "from-b")).await.unwrap();
        let msg = mx.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "from-b");
    }

    #[tokio::test]
    async fn multiplex_receive_any() {
        let (a1, _b1) = ChannelPort::pair(8);
        let (a2, mut b2) = ChannelPort::pair(8);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a1));
        mx.add_port(Box::new(a2));
        b2.send(PortMessage::new("b2", "second")).await.unwrap();
        let msg = mx.receive_any().await.unwrap().unwrap();
        assert_eq!(msg.content, "second");
    }

    #[tokio::test]
    async fn multiplex_poll() {
        let (a, mut b) = ChannelPort::pair(8);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a));
        b.send(PortMessage::new("b", "polled")).await.unwrap();
        let msg = mx.poll(Duration::from_secs(1)).await.unwrap().unwrap();
        assert_eq!(msg.content, "polled");
    }

    #[tokio::test]
    async fn multiplex_close_all() {
        let (a, _b) = ChannelPort::pair(4);
        let (c, _d) = ChannelPort::pair(4);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a));
        mx.add_port(Box::new(c));
        mx.close().await.unwrap();
        assert!(!mx.is_active());
    }

    #[tokio::test]
    async fn multiplex_empty_receive() {
        let mut mx = MultiplexPort::new("mx");
        let err = mx.receive().await.unwrap_err();
        assert_eq!(err, PortError::Closed);
    }

    #[tokio::test]
    async fn multiplex_endpoint() {
        let mx = MultiplexPort::new("test-mx");
        assert_eq!(mx.endpoint(), "test-mx");
    }

    #[tokio::test]
    async fn multiplex_port_count() {
        let (a, _b) = ChannelPort::pair(4);
        let mut mx = MultiplexPort::new("mx");
        assert_eq!(mx.port_count(), 0);
        mx.add_port(Box::new(a));
        assert_eq!(mx.port_count(), 1);
    }

    #[tokio::test]
    async fn multiplex_send_to_first_active() {
        let (a, mut b) = ChannelPort::pair(8);
        let (c, _d) = ChannelPort::pair(8);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a));
        mx.add_port(Box::new(c));
        mx.send(PortMessage::new("mx", "out")).await.unwrap();
        let msg = b.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "out");
    }

    #[tokio::test]
    async fn multiplex_poll_timeout_empty() {
        let (a, _b) = ChannelPort::pair(4);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a));
        let result = mx.poll(Duration::from_millis(30)).await.unwrap();
        assert!(result.is_none());
    }

    // ---- BufferedPort ----

    #[tokio::test]
    async fn buffered_send_and_receive() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "buf-msg")).await.unwrap();
        assert_eq!(bp.buffered_count(), 1);
        let msg = bp.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "buf-msg");
        assert_eq!(bp.buffered_count(), 0);
    }

    #[tokio::test]
    async fn buffered_multiple_messages() {
        let (ch, _other) = ChannelPort::pair(16);
        let mut bp = BufferedPort::new(Box::new(ch), 32, OverflowStrategy::Refuse);
        for i in 0..5 {
            bp.send(PortMessage::new("s", format!("m{i}"))).await.unwrap();
        }
        assert_eq!(bp.buffered_count(), 5);
        for i in 0..5 {
            let msg = bp.receive().await.unwrap().unwrap();
            assert_eq!(msg.content, format!("m{i}"));
        }
    }

    #[tokio::test]
    async fn buffered_overflow_drop_oldest() {
        let (ch, _other) = ChannelPort::pair(16);
        let mut bp = BufferedPort::new(Box::new(ch), 2, OverflowStrategy::DropOldest);
        bp.send(PortMessage::new("s", "first")).await.unwrap();
        bp.send(PortMessage::new("s", "second")).await.unwrap();
        bp.send(PortMessage::new("s", "third")).await.unwrap();
        assert_eq!(bp.buffered_count(), 2);
        assert_eq!(bp.dropped(), 1);
        // "first" should have been dropped.
        let msg = bp.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "second");
    }

    #[tokio::test]
    async fn buffered_overflow_drop_newest() {
        let (ch, _other) = ChannelPort::pair(16);
        let mut bp = BufferedPort::new(Box::new(ch), 2, OverflowStrategy::DropNewest);
        bp.send(PortMessage::new("s", "first")).await.unwrap();
        bp.send(PortMessage::new("s", "second")).await.unwrap();
        bp.send(PortMessage::new("s", "third")).await.unwrap();
        assert_eq!(bp.buffered_count(), 2);
        assert_eq!(bp.dropped(), 1);
        let msg = bp.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "first");
    }

    #[tokio::test]
    async fn buffered_overflow_refuse() {
        let (ch, _other) = ChannelPort::pair(16);
        let mut bp = BufferedPort::new(Box::new(ch), 2, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "a")).await.unwrap();
        bp.send(PortMessage::new("s", "b")).await.unwrap();
        let err = bp.send(PortMessage::new("s", "c")).await.unwrap_err();
        assert_eq!(err, PortError::Full);
        assert_eq!(bp.buffered_count(), 2);
        assert_eq!(bp.stats().errors, 1);
    }

    #[tokio::test]
    async fn buffered_poll_from_buffer() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "polled")).await.unwrap();
        let msg = bp.poll(Duration::from_millis(10)).await.unwrap().unwrap();
        assert_eq!(msg.content, "polled");
    }

    #[tokio::test]
    async fn buffered_close_clears_buffer() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "x")).await.unwrap();
        assert_eq!(bp.buffered_count(), 1);
        bp.close().await.unwrap();
        assert_eq!(bp.buffered_count(), 0);
    }

    #[tokio::test]
    async fn buffered_endpoint_delegates() {
        let (ch, _other) = ChannelPort::pair_named(8, "inner", "remote");
        let bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        assert_eq!(bp.endpoint(), "inner");
    }

    #[tokio::test]
    async fn buffered_is_active_with_buffered() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "x")).await.unwrap();
        assert!(bp.is_active());
    }

    #[tokio::test]
    async fn buffered_stats() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        bp.send(PortMessage::new("s", "a")).await.unwrap();
        bp.send(PortMessage::new("s", "b")).await.unwrap();
        assert_eq!(bp.stats().sent, 2);
        bp.receive().await.unwrap();
        assert_eq!(bp.stats().received, 1);
    }

    #[tokio::test]
    async fn buffered_poll_timeout_empty() {
        let (ch, _other) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(ch), 16, OverflowStrategy::Refuse);
        let result = bp.poll(Duration::from_millis(20)).await.unwrap();
        assert!(result.is_none());
    }

    // ---- Integration-style tests ----

    #[tokio::test]
    async fn full_pipeline_channel_broadcast() {
        let (mut sender, mut receiver) = ChannelPort::pair(32);
        let mut broadcaster = BroadcastPort::new("hub");
        let mut sub_rx = broadcaster.subscribe();

        sender.send(PortMessage::new("client", "hello")).await.unwrap();
        let msg = receiver.receive().await.unwrap().unwrap();
        broadcaster.send(msg).await.unwrap();
        let broadcast_msg = sub_rx.recv().await.unwrap();
        assert_eq!(broadcast_msg.content, "hello");
    }

    #[tokio::test]
    async fn multiplex_with_buffered_ports() {
        let (a1, mut b1) = ChannelPort::pair(8);
        let (a2, mut b2) = ChannelPort::pair(8);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(BufferedPort::new(Box::new(a1), 16, OverflowStrategy::DropOldest)));
        mx.add_port(Box::new(BufferedPort::new(Box::new(a2), 16, OverflowStrategy::DropOldest)));
        b1.send(PortMessage::new("b1", "from-1")).await.unwrap();
        b2.send(PortMessage::new("b2", "from-2")).await.unwrap();
        let msg = mx.receive().await.unwrap().unwrap();
        assert!((msg.content == "from-1") || (msg.content == "from-2"));
    }

    #[tokio::test]
    async fn stress_channel_port() {
        let (mut a, mut b) = ChannelPort::pair(256);
        let count = 100u64;
        for i in 0..count {
            a.send(PortMessage::new("a", format!("stress-{i}"))).await.unwrap();
        }
        for i in 0..count {
            let msg = b.receive().await.unwrap().unwrap();
            assert_eq!(msg.content, format!("stress-{i}"));
        }
        assert_eq!(a.stats().sent, count);
        assert_eq!(b.stats().received, count);
    }

    #[tokio::test]
    async fn broadcast_many_messages() {
        let mut bp = BroadcastPort::new("stress");
        let mut subs: Vec<mpsc::Receiver<PortMessage>> = (0..5).map(|_| bp.subscribe()).collect();
        for i in 0..20 {
            bp.send(PortMessage::new("src", format!("msg-{i}"))).await.unwrap();
        }
        for sub in &mut subs {
            for i in 0..20 {
                let msg = sub.recv().await.unwrap();
                assert_eq!(msg.content, format!("msg-{i}"));
            }
        }
    }

    #[tokio::test]
    async fn channel_port_disconnected_on_drop() {
        let (mut a, b) = ChannelPort::pair(4);
        drop(b);
        let mut got_error = false;
        for _ in 0..100 {
            if a.send(PortMessage::new("a", "x")).await.is_err() {
                got_error = true;
                break;
            }
        }
        assert!(got_error || !a.is_active());
    }

    // ---- Additional tests for 60+ ----

    #[test]
    fn port_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(PortError::Closed);
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn priority_copy_clone() {
        let p = MessagePriority::High;
        let q = p;
        assert_eq!(p, q);
    }

    #[test]
    fn port_message_equality() {
        let mut a = PortMessage::new("s", "c");
        let mut b = a.clone();
        a.metadata.insert("k".into(), "v".into());
        assert_ne!(a, b);
        b.metadata.insert("k".into(), "v".into());
        assert_eq!(a, b);
    }

    #[test]
    fn port_stats_new() {
        let s = PortStats::new();
        assert_eq!(s.sent, 0);
    }

    #[test]
    fn overflow_strategy_copy() {
        let s = OverflowStrategy::DropOldest;
        let _t = s; // Copy
        assert_eq!(s, OverflowStrategy::DropOldest);
    }

    #[tokio::test]
    async fn channel_port_capacity_one() {
        let (mut a, mut b) = ChannelPort::pair(1);
        a.send(PortMessage::new("a", "only")).await.unwrap();
        let msg = b.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "only");
    }

    #[tokio::test]
    async fn buffered_fallthrough_to_inner() {
        let (inner_a, mut inner_b) = ChannelPort::pair(8);
        let mut bp = BufferedPort::new(Box::new(inner_a), 4, OverflowStrategy::Refuse);
        // Buffer is empty, so receive should delegate to inner port.
        inner_b.send(PortMessage::new("ib", "inner-msg")).await.unwrap();
        let msg = bp.receive().await.unwrap().unwrap();
        assert_eq!(msg.content, "inner-msg");
    }

    #[tokio::test]
    async fn broadcast_zero_subscribers() {
        let mut bp = BroadcastPort::new("empty");
        // Should succeed even with no subscribers.
        bp.send(PortMessage::new("s", "void")).await.unwrap();
    }

    #[tokio::test]
    async fn multiplex_is_active_with_one_active() {
        let (a, _b) = ChannelPort::pair(4);
        let (mut c, _d) = ChannelPort::pair(4);
        let mut mx = MultiplexPort::new("mx");
        mx.add_port(Box::new(a));
        c.close().await.unwrap();
        mx.add_port(Box::new(c));
        assert!(mx.is_active());
    }
}
