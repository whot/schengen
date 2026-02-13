// SPDX-License-Identifier: GPL-3.0-or-later

//! # Schengen Server API
//!
//! The module to build a schengen server.
//!
//! ## Overview
//!
//! This module provides a high-level API for creating a Synergy/Deskflow server that
//! manages multiple client connections. The server automatically handles protocol-level
//! details like handshakes and keepalives, presenting a simple event-based API.
//!
//! ## Example: Server with Three Clients
//!
//! This example shows how to set up a server with three clients in different positions:
//! - A laptop to the left of the server
//! - A desktop to the right of the server
//! - A tablet positioned below the laptop
//!
//! ```text
//!         [laptop] --- [server] --- [desktop]
//!              |
//!          [tablet]
//! ```
//!
//! Positions are validated at build time, it is not possible to assign two clients to the same
//! position. Likewise, client names must be unique.
//!
//! ```no_run
//! use schengen::server::{Builder, ClientBuilder, Position, ServerEvent};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure the first client (laptop) - positioned to the left of the server
//!     let laptop = ClientBuilder::new("laptop")
//!         .position(Position::Left)
//!         .build();
//!
//!     // Configure the second client (desktop) - positioned to the right of the server
//!     let desktop = ClientBuilder::new("desktop")
//!         .position(Position::Right)
//!         .build();
//!
//!     // Configure the third client (tablet) - positioned below the laptop
//!     // Note: This client's position is relative to the laptop, not the server
//!     let tablet = ClientBuilder::new("tablet")
//!         .position(Position::Below)
//!         .relative_to(&laptop)
//!         .build();
//!
//!     // Build and start the server
//!     let server = Builder::new()
//!         .add_client(laptop)?      // Must be added before tablet references it
//!         .add_client(desktop)?
//!         .add_client(tablet)?
//!         .port(24801)
//!         .listen()
//!         .await?;
//!
//!     println!("Server listening on port 24801");
//!     println!("Waiting for clients to connect...\n");
//!
//!     // Main event loop - processes all events from the server
//!     loop {
//!         match server.recv_event().await {
//!             Ok(ServerEvent::ClientConnected { client_id, name, width, height }) => {
//!                 println!("✓ Client '{}' ({:?}) connected ({}x{})", name, client_id, width, height);
//!
//!                 // Get list of all connected clients
//!                 let connected = server.clients().await;
//!                 println!("  Currently connected: {} client(s)", connected.len());
//!                 for client in &connected {
//!                     println!("    - {}", client.name());
//!                 }
//!                 println!();
//!             }
//!
//!             Ok(ServerEvent::ClientDisconnected { client_id, name }) => {
//!                 println!("✗ Client '{}' ({:?}) disconnected\n", name, client_id);
//!             }
//!
//!             Ok(ServerEvent::ClipboardData { client_id, data, .. }) => {
//!                 println!("📋 Clipboard data from {:?}: {} bytes",
//!                          client_id, data.len());
//!             }
//!
//!             Ok(ServerEvent::ScreenSaverChanged { client_id, active }) => {
//!                 let state = if active { "activated" } else { "deactivated" };
//!                 println!("💤 Screen saver {} on {:?}", state, client_id);
//!             }
//!
//!             Ok(ServerEvent::ClientInfoUpdated { .. }) => {
//!                 // Client sent updated info (screen dimensions, etc.)
//!             }
//!
//!             Err(e) => {
//!                 eprintln!("Error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Sending Messages to Clients
//!
//! Once clients are connected, you can send messages to them:
//!
//! ```no_run
//! # use schengen::server::{Builder, ClientBuilder, Position, ServerEvent};
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = Builder::new()
//! #     .add_client(ClientBuilder::new("laptop").position(Position::Left).build())?
//! #     .listen().await?;
//! // Get the list of connected clients
//! let clients = server.clients().await;
//!
//! // Send messages to specific clients
//! for client in &clients {
//!     if client.name() == "laptop" {
//!         // Notify the laptop that the cursor entered at position (100, 200)
//!         server.send_cursor_entered(client.id(), 100, 200, 0, 0).await?;
//!
//!         // Send mouse movement
//!         server.send_mouse_move(client.id(), 150, 250).await?;
//!
//!         // Send key events
//!         server.send_key_down(client.id(), 65, 0, 38).await?; // 'A' key
//!         server.send_key_up(client.id(), 65, 0, 38).await?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::protocol::{
    Message, MessageHelloBarrier, MessageInfoAcknowledgment, MessageKeepAlive,
    MessageLegacySynergy, MessageQueryInfo, MessageResetOptions, MessageSetOptions, ProtocolError,
    parse_message_with_length,
};

const DEFAULT_PORT: u16 = 24801;

/// Unique identifier for a connected client
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

/// Position of a client relative to the server screen(s)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// Client is to the left of the server
    Left,
    /// Client is to the right of the server
    Right,
    /// Client is above the server
    Above,
    /// Client is below the server
    Below,
}

/// A connected client
///
/// Represents a client that is currently connected to the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Client {
    id: ClientId,
    name: String,
    /// Screen width in pixels
    pub width: u16,
    /// Screen height in pixels
    pub height: u16,
}

impl Client {
    /// Get the client's unique ID
    pub fn id(&self) -> ClientId {
        self.id
    }

    /// Get the client's name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Configuration for a client that can connect to the server
///
/// Created using [`ClientBuilder`] and passed to [`Builder::add_client`].
#[derive(Debug, Clone)]
pub struct NewClient {
    name: String,
    position: Position,
    relative_to: Option<String>,
}

impl NewClient {
    /// Get the client's name
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Get the client's position
    #[allow(dead_code)]
    pub(crate) fn position(&self) -> Position {
        self.position
    }

    /// Get the name of the client this position is relative to, if any
    #[allow(dead_code)]
    pub(crate) fn relative_to(&self) -> Option<&str> {
        self.relative_to.as_deref()
    }
}

/// Builder for configuring a client
///
/// Uses a typestate pattern to ensure position is set before building.
///
/// # Example
///
/// ```
/// use schengen::server::{ClientBuilder, Position};
///
/// // Simple positioning
/// let left_client = ClientBuilder::new("laptop")
///     .position(Position::Left)
///     .build();
///
/// // Relative positioning
/// let below_client = ClientBuilder::new("monitor")
///     .position(Position::Below)
///     .relative_to(&left_client)
///     .build();
/// ```
pub struct ClientBuilder<S: client_builder_state::State = client_builder_state::NeedsPosition> {
    _state: std::marker::PhantomData<S>,
    name: String,
    position: Option<Position>,
    relative_to: Option<String>,
}

mod client_builder_state {
    pub trait State {}
    pub struct NeedsPosition;
    pub struct Ready;
    impl State for NeedsPosition {}
    impl State for Ready {}
}

impl ClientBuilder<client_builder_state::NeedsPosition> {
    /// Create a new client builder with the given name
    ///
    /// # Arguments
    ///
    /// * `name` - The name that identifies this client during the Synergy handshake
    pub fn new(name: &str) -> Self {
        Self {
            _state: std::marker::PhantomData,
            name: name.to_string(),
            position: None,
            relative_to: None,
        }
    }

    /// Set the position of this client relative to the server screen
    ///
    /// This transitions the builder to the Ready state, allowing build() and relative_to() to be called.
    pub fn position(self, position: Position) -> ClientBuilder<client_builder_state::Ready> {
        ClientBuilder {
            _state: std::marker::PhantomData,
            name: self.name,
            position: Some(position),
            relative_to: None,
        }
    }
}

impl ClientBuilder<client_builder_state::Ready> {
    /// Specify that this client's position is relative to another client
    ///
    /// This is useful for arranging multiple clients in a specific layout.
    /// For example, if you have a laptop to the left of the server, and a monitor
    /// below the laptop, you would use:
    ///
    /// ```
    /// # use schengen::server::{ClientBuilder, Position};
    /// let laptop = ClientBuilder::new("laptop")
    ///     .position(Position::Left)
    ///     .build();
    ///
    /// let monitor = ClientBuilder::new("monitor")
    ///     .position(Position::Below)
    ///     .relative_to(&laptop)
    ///     .build();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the target client has the same name as this client.
    /// A client cannot be positioned relative to itself.
    ///
    /// Only available after position has been set.
    pub fn relative_to(mut self, client: &NewClient) -> Self {
        if self.name == client.name {
            panic!(
                "Client '{}' cannot be positioned relative to a client with the same name",
                self.name
            );
        }
        self.relative_to = Some(client.name.clone());
        self
    }

    /// Build the client configuration
    ///
    /// Only available after position has been set.
    pub fn build(self) -> NewClient {
        NewClient {
            name: self.name,
            position: self
                .position
                .expect("position should be set in Ready state"),
            relative_to: self.relative_to,
        }
    }
}

/// Events from clients that the server application should handle
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A client successfully connected
    ClientConnected {
        client_id: ClientId,
        name: String,
        width: u16,
        height: u16,
    },

    /// A client disconnected
    ClientDisconnected { client_id: ClientId, name: String },

    /// Clipboard data received from a client
    ClipboardData {
        client_id: ClientId,
        id: u8,
        sequence: u32,
        data: Vec<u8>,
    },

    /// Screen saver state changed on a client
    ScreenSaverChanged { client_id: ClientId, active: bool },

    /// Client info updated (dimensions changed)
    ClientInfoUpdated {
        client_id: ClientId,
        width: u16,
        height: u16,
    },
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Invalid listen address: {0}")]
    InvalidListenAddress(String),

    #[error("Failed to bind server: {0}")]
    BindFailed(#[from] std::io::Error),

    #[error("Client '{0}' is not in the allowed clients list")]
    UnknownClient(String),

    #[error("Protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error(
        "Position {position:?} on the server is already occupied by client '{existing_client}'"
    )]
    ServerPositionOccupied {
        position: Position,
        existing_client: String,
    },

    #[error(
        "Position {position:?} relative to client '{relative_to}' is already occupied by client '{existing_client}'"
    )]
    RelativePositionOccupied {
        position: Position,
        relative_to: String,
        existing_client: String,
    },

    #[error("Client '{0}' referenced in relative_to does not exist")]
    RelativeClientNotFound(String),
}

pub type Result<T> = std::result::Result<T, ServerError>;

/// Internal configuration for a single client
#[derive(Debug, Clone)]
struct ClientConfig {
    #[allow(dead_code)]
    position: Position,
}

/// A connected client
struct ConnectedClient {
    client: Client,
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl ConnectedClient {
    /// Send a protocol message to this client
    async fn send_message(&mut self, message: Message) -> Result<()> {
        let bytes = message.to_bytes();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| ServerError::ConnectionError(e.to_string()))?;
        self.stream
            .flush()
            .await
            .map_err(|e| ServerError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    /// Receive a message from this client
    async fn recv_message(&mut self) -> Result<Option<Message>> {
        loop {
            // Try to parse a message from the buffer
            if !self.buffer.is_empty() {
                match parse_message_with_length(&self.buffer) {
                    Ok((msg, consumed)) => {
                        // Remove consumed bytes from buffer
                        self.buffer.drain(..consumed);
                        return Ok(Some(msg));
                    }
                    Err(ProtocolError::InsufficientData { .. }) => {
                        // Need more data, continue reading
                    }
                    Err(e) => {
                        return Err(ServerError::ProtocolError(e));
                    }
                }
            }

            // Since we're expected to be integrated/combined with some other event loop
            // we have a 1ms timeout here so the caller can go back to what it's doing.
            let mut temp_buf = vec![0u8; 4096];
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(1),
                self.stream.readable(),
            )
            .await
            {
                Ok(Ok(())) => {
                    // Socket is readable, try non-blocking read
                    match self.stream.try_read(&mut temp_buf) {
                        Ok(0) => {
                            // Connection closed
                            return Err(ServerError::ConnectionError(
                                "Connection closed by client".to_string(),
                            ));
                        }
                        Ok(n) => {
                            self.buffer.extend_from_slice(&temp_buf[..n]);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            // No data available right now, loop again
                            continue;
                        }
                        Err(e) => {
                            return Err(ServerError::ConnectionError(e.to_string()));
                        }
                    }
                }
                Ok(Err(e)) => {
                    return Err(ServerError::ConnectionError(format!(
                        "Error waiting for readable: {}",
                        e
                    )));
                }
                Err(_) => {
                    // Timeout - no data available
                    return Ok(None);
                }
            }
        }
    }
}

pub struct Builder {
    port: u16,
    clients: Vec<NewClient>,
}

impl Builder {
    /// Create a new server builder
    pub fn new() -> Self {
        Builder {
            port: DEFAULT_PORT,
            clients: Vec::new(),
        }
    }

    /// Set the port to listen on
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Add a client that is allowed to connect
    ///
    /// Use [`ClientBuilder`] to create a client configuration.
    ///
    /// # Validation
    ///
    /// This method validates that:
    /// - For clients positioned relative to the server (no `relative_to`), only one client
    ///   can occupy each edge (Left, Right, Above, Below)
    /// - For clients positioned relative to another client (`relative_to` set), only one
    ///   client can be on each side of the target client
    /// - The client referenced in `relative_to` must have been added previously
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The position is already occupied on the server
    /// - The position is already occupied relative to the target client
    /// - The target client in `relative_to` doesn't exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// use schengen::server::{Builder, ClientBuilder, Position};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let laptop = ClientBuilder::new("laptop")
    ///     .position(Position::Left)
    ///     .build();
    ///
    /// let monitor = ClientBuilder::new("monitor")
    ///     .position(Position::Below)
    ///     .relative_to(&laptop)
    ///     .build();
    ///
    /// let server = Builder::new()
    ///     .add_client(laptop)?
    ///     .add_client(monitor)?
    ///     .listen()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_client(mut self, client: NewClient) -> Result<Self> {
        // Validate the client's position
        if let Some(relative_to) = &client.relative_to {
            // Check if the referenced client exists
            if !self.clients.iter().any(|c| &c.name == relative_to) {
                return Err(ServerError::RelativeClientNotFound(relative_to.clone()));
            }

            // Check if this position is already occupied relative to the target client
            if let Some(existing) = self.clients.iter().find(|c| {
                c.relative_to.as_ref() == Some(relative_to) && c.position == client.position
            }) {
                return Err(ServerError::RelativePositionOccupied {
                    position: client.position,
                    relative_to: relative_to.clone(),
                    existing_client: existing.name.clone(),
                });
            }
        } else {
            // Client is positioned relative to the server
            // Check if this position is already occupied on the server
            if let Some(existing) = self
                .clients
                .iter()
                .find(|c| c.relative_to.is_none() && c.position == client.position)
            {
                return Err(ServerError::ServerPositionOccupied {
                    position: client.position,
                    existing_client: existing.name.clone(),
                });
            }
        }

        self.clients.push(client);
        Ok(self)
    }

    /// Build and start the server
    ///
    /// This will bind to 0.0.0.0 (IPv4) or :: (IPv6) on the configured port
    /// and start accepting connections.
    /// When this function returns, the server is ready to accept client connections.
    pub async fn listen(self) -> Result<Server> {
        // Try IPv6 first (which accepts IPv4 on most systems), fall back to IPv4
        let listener = match TcpListener::bind(("::".to_string(), self.port)).await {
            Ok(listener) => listener,
            Err(_) => {
                // IPv6 failed, try IPv4
                TcpListener::bind(("0.0.0.0".to_string(), self.port)).await?
            }
        };

        self.build_server_with_listener(listener)
    }

    /// Build and start the server using an existing TCP listener
    ///
    /// This is useful for systemd socket activation where the listener is already bound.
    /// When this function returns, the server is ready to accept client connections.
    ///
    /// # Arguments
    ///
    /// * `listener` - An already-bound TCP listener (e.g., from systemd socket activation)
    pub async fn listen_on_stream(self, listener: TcpListener) -> Result<Server> {
        self.build_server_with_listener(listener)
    }

    /// Internal helper to build a server with a given listener
    fn build_server_with_listener(self, listener: TcpListener) -> Result<Server> {
        // Build the allowed clients map
        let mut allowed_clients = HashMap::new();
        for client in self.clients {
            allowed_clients.insert(
                client.name.clone(),
                ClientConfig {
                    position: client.position,
                },
            );
        }

        let server = Server {
            listener,
            allowed_clients: Arc::new(allowed_clients),
            connected_clients: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        };

        // Spawn a background task to send periodic keepalives (KeepAlive messages) to all clients
        let connected_clients = Arc::clone(&server.connected_clients);
        debug!("Spawning keepalive sender task");
        tokio::spawn(async move {
            debug!("Keepalive sender task started");
            Server::keepalive_sender(connected_clients).await;
        });

        Ok(server)
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// Synergy server
///
/// The server manages multiple client connections and handles the Synergy protocol
/// handshake and message routing.
pub struct Server {
    listener: TcpListener,
    allowed_clients: Arc<HashMap<String, ClientConfig>>,
    connected_clients: Arc<RwLock<HashMap<ClientId, Arc<RwLock<ConnectedClient>>>>>,
    /// Counter for generating unique client IDs
    next_client_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Server {
    /// Send a message to a specific client by ID
    async fn send_to(&self, client_id: ClientId, message: Message) -> Result<()> {
        let client = {
            let clients = self.connected_clients.read().await;
            clients
                .get(&client_id)
                .ok_or_else(|| {
                    ServerError::ConnectionError(format!(
                        "Client with ID {:?} not connected",
                        client_id
                    ))
                })?
                .clone()
        };

        let mut client_lock = client.write().await;
        client_lock.send_message(message).await
    }

    /// Query client information
    ///
    /// Sends a QueryInfo message to the specified client, which will respond
    /// with updated ClientInfo as part of a future recv_event.
    pub async fn query_client_info(&self, client_id: ClientId) -> Result<()> {
        self.send_to(
            client_id,
            Message::QueryInfo(crate::protocol::MessageQueryInfo),
        )
        .await
    }

    /// Remove a client from all tracking structures (internal helper)
    ///
    /// This should be called when a client disconnects, either gracefully or due to an error.
    async fn remove_client(&self, client_id: ClientId) {
        // Remove from connected clients
        self.connected_clients.write().await.remove(&client_id);
    }

    /// Receive the next event from the server
    ///
    /// This automatically handles:
    /// - Accepting new client connections
    /// - Protocol-level messages (keepalives, info acknowledgments, etc.)
    ///
    /// Only returns events that the caller needs to act on, e.g.
    /// - New client connections (`ClientConnected`)
    /// - Client disconnections (`ClientDisconnected`)
    /// - Clipboard data (`ClipboardData`)
    /// - Screen saver changes (`ScreenSaverChanged`)
    ///
    /// This method will block until an event is available.
    pub async fn recv_event(&self) -> Result<ServerEvent> {
        loop {
            // Get a snapshot of connected clients
            let client_list: Vec<Arc<RwLock<ConnectedClient>>> = {
                let clients = self.connected_clients.read().await;
                clients.values().cloned().collect()
            };

            // Check for messages from existing clients first
            for client in &client_list {
                // Try to acquire write lock without blocking too long
                if let Ok(mut client_lock) = client.try_write()
                    && !client_lock.buffer.is_empty()
                {
                    let client_info = client_lock.client.clone();
                    match client_lock.recv_message().await {
                        Ok(Some(msg)) => {
                            drop(client_lock);
                            if let Some(event) =
                                self.handle_client_message(&client_info, msg).await?
                            {
                                return Ok(event);
                            }
                        }
                        Ok(None) => {
                            // Timeout - no message available, try next client
                        }
                        Err(_) => {
                            drop(client_lock);
                            let client_id = client_info.id();
                            let client_name = client_info.name().to_string();
                            self.remove_client(client_id).await;
                            return Ok(ServerEvent::ClientDisconnected {
                                client_id,
                                name: client_name,
                            });
                        }
                    }
                }
            }

            // Use select to handle both new connections and messages from existing clients
            tokio::select! {
                // Accept new client connections
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            // Generate a unique client ID
                            let client_id = ClientId(
                                self.next_client_id
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            );

                            match perform_server_handshake(client_id, stream, &self.allowed_clients).await {
                                Ok(connected_client) => {
                                    let client = connected_client.client.clone();
                                    let connected = Arc::new(RwLock::new(connected_client));

                                    self.connected_clients
                                        .write()
                                        .await
                                        .insert(client_id, connected);

                                    return Ok(ServerEvent::ClientConnected {
                                        client_id,
                                        name: client.name,
                                        width: client.width,
                                        height: client.height,
                                    });
                                }
                                Err(_e) => {
                                    // Handshake failed, continue to accept other clients
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ServerError::BindFailed(e));
                        }
                    }
                }

                // Wait for data from any client (only when there are clients to avoid busy loop)
                result = Self::recv_from_any_client(client_list), if !client_list.is_empty() => {
                    match result {
                        Some(Ok((client, msg))) => {
                            if let Some(event) = self.handle_client_message(&client, msg).await? {
                                return Ok(event);
                            }
                        }
                        Some(Err(client)) => {
                            let client_id = client.id();
                            let client_name = client.name().to_string();
                            self.remove_client(client_id).await;
                            return Ok(ServerEvent::ClientDisconnected {
                                client_id,
                                name: client_name,
                            });
                        }
                        None => {
                            // No messages available, continue
                        }
                    }
                }
            }
        }
    }

    /// Helper to receive a message from any client (internal)
    ///
    /// Returns either:
    /// - Some(Ok((client, msg))) - Successfully received a message
    /// - Some(Err(client)) - Client disconnected with error
    /// - None - No messages available from any client
    async fn recv_from_any_client(
        clients: Vec<Arc<RwLock<ConnectedClient>>>,
    ) -> Option<std::result::Result<(Client, Message), Client>> {
        for client in clients {
            let mut client_lock = client.write().await;
            let client_info = client_lock.client.clone();
            match client_lock.recv_message().await {
                Ok(Some(msg)) => return Some(Ok((client_info, msg))),
                Ok(None) => {
                    continue;
                }
                Err(_) => {
                    return Some(Err(client_info));
                }
            }
        }
        None
    }

    /// Handle a message from a client
    /// Returns Some(event) if the caller should handle this, None if it was auto-handled
    async fn handle_client_message(
        &self,
        client: &Client,
        message: Message,
    ) -> Result<Option<ServerEvent>> {
        let client_id = client.id();
        let client_name = client.name();

        match message {
            // Auto-handle keepalives - just acknowledge, don't respond, this is the
            // reply to the KeepAlive we sent to the client
            Message::KeepAlive(_) => {
                debug!("Received KeepAlive from client '{}'", client_name);
                Ok(None)
            }

            // Auto-handle NoOp
            Message::NoOp(_) => {
                debug!("Received NoOp from client '{}'", client_name);
                Ok(None)
            }

            Message::ClientInfo(info) => {
                debug!(
                    "Received ClientInfo from '{}': {}x{}",
                    client_name, info.width, info.height
                );

                let clients = self.connected_clients.read().await;
                if let Some(client) = clients.get(&client_id) {
                    let mut c = client.write().await;
                    c.client.width = info.width;
                    c.client.height = info.height;
                }
                drop(clients);

                Ok(Some(ServerEvent::ClientInfoUpdated {
                    client_id,
                    width: info.width,
                    height: info.height,
                }))
            }

            Message::ClientClipboard(msg) => Ok(Some(ServerEvent::ClipboardData {
                client_id,
                id: msg.id,
                sequence: msg.sequence,
                data: vec![], // Actual data comes in ClipboardData message
            })),

            Message::ClipboardData(msg) => Ok(Some(ServerEvent::ClipboardData {
                client_id,
                id: msg.id,
                sequence: msg.sequence,
                data: msg.data.0.into_bytes(),
            })),

            Message::ScreenSaverChange(msg) => Ok(Some(ServerEvent::ScreenSaverChanged {
                client_id,
                active: msg.state != 0,
            })),

            Message::InfoAcknowledgment(_) => Ok(None),

            Message::Close(_) => {
                self.remove_client(client_id).await;
                Ok(Some(ServerEvent::ClientDisconnected {
                    client_id,
                    name: client_name.to_string(),
                }))
            }

            // Ignore other messages
            _ => Ok(None),
        }
    }

    /// Get a list of currently connected clients
    ///
    /// Returns a snapshot of the connected clients at the time of the call.
    pub async fn clients(&self) -> Vec<Client> {
        let clients = self.connected_clients.read().await;
        let mut result = Vec::new();
        for (_, client_lock) in clients.iter() {
            let client = client_lock.read().await;
            result.push(client.client.clone());
        }
        result
    }

    /// Send a cursor entered message to a client
    ///
    /// This notifies the client that the cursor has entered their screen.
    pub async fn send_cursor_entered(
        &self,
        client_id: ClientId,
        x: i16,
        y: i16,
        sequence: u32,
        mask: u16,
    ) -> Result<()> {
        let msg = Message::CursorEntered(crate::protocol::MessageCursorEntered {
            x,
            y,
            sequence,
            mask,
        });
        self.send_to(client_id, msg).await
    }

    /// Send a cursor left message to a client
    ///
    /// This notifies the client that the cursor has left their screen.
    pub async fn send_cursor_left(&self, client_id: ClientId) -> Result<()> {
        self.send_to(
            client_id,
            Message::CursorLeft(crate::protocol::MessageCursorLeft),
        )
        .await
    }

    /// Send an absolute mouse move message to a client
    ///
    /// The coordinates must be within the client's width/height.
    pub async fn send_mouse_move(&self, client_id: ClientId, x: i16, y: i16) -> Result<()> {
        let msg = Message::MouseMove(crate::protocol::MessageMouseMove { x, y });
        self.send_to(client_id, msg).await
    }

    /// Send a key down message to a client
    pub async fn send_key_down(
        &self,
        client_id: ClientId,
        keyid: u16,
        mask: u16,
        button: u16,
    ) -> Result<()> {
        let msg = Message::KeyDown(crate::protocol::MessageKeyDown {
            keyid,
            mask,
            button,
        });
        self.send_to(client_id, msg).await
    }

    /// Send a key up message to a client
    pub async fn send_key_up(
        &self,
        client_id: ClientId,
        keyid: u16,
        mask: u16,
        button: u16,
    ) -> Result<()> {
        let msg = Message::KeyUp(crate::protocol::MessageKeyUp {
            keyid,
            mask,
            button,
        });
        self.send_to(client_id, msg).await
    }

    /// Send a mouse button down message to a client
    ///
    /// # Arguments
    ///
    /// * `client_id` - The ID of the client to send to
    /// * `button` - Button number (1=left, 2=right, 3=middle, 4+=additional)
    pub async fn send_mouse_button_down(&self, client_id: ClientId, button: u8) -> Result<()> {
        let msg = Message::MouseButtonDown(crate::protocol::MessageMouseButtonDown { button });
        self.send_to(client_id, msg).await
    }

    /// Send a mouse button up message to a client
    ///
    /// # Arguments
    ///
    /// * `client_id` - The ID of the client to send to
    /// * `button` - Button number (1=left, 2=right, 3=middle, 4+=additional)
    pub async fn send_mouse_button_up(&self, client_id: ClientId, button: u8) -> Result<()> {
        let msg = Message::MouseButtonUp(crate::protocol::MessageMouseButtonUp { button });
        self.send_to(client_id, msg).await
    }

    /// Send a mouse wheel message to a client
    ///
    /// # Arguments
    ///
    /// * `client_id` - The ID of the client to send to
    /// * `xdelta` - Horizontal scroll delta (+120 = right, -120 = left, typically multiples of 120)
    /// * `ydelta` - Vertical scroll delta (+120 = up/away, -120 = down/toward, typically multiples of 120)
    pub async fn send_mouse_wheel(
        &self,
        client_id: ClientId,
        xdelta: i16,
        ydelta: i16,
    ) -> Result<()> {
        let msg = Message::MouseWheel(crate::protocol::MessageMouseWheel { xdelta, ydelta });
        self.send_to(client_id, msg).await
    }

    /// Send an arbitrary message to a client
    ///
    /// This is a low-level method that allows sending any protocol message to a client.
    /// Most users should use the higher-level methods like `send_mouse_move`, etc.
    pub async fn send_message(&self, client_id: ClientId, message: Message) -> Result<()> {
        self.send_to(client_id, message).await
    }

    /// Background task to send periodic keepalive messages to all connected clients
    ///
    /// This prevents clients from timing out and declaring the server dead.
    /// Sends a KeepAlive message every 3 seconds to each connected client.
    async fn keepalive_sender(
        connected_clients: Arc<RwLock<HashMap<ClientId, Arc<RwLock<ConnectedClient>>>>>,
    ) {
        use tokio::time::{Duration, interval};

        let mut interval = interval(Duration::from_secs(3));

        loop {
            interval.tick().await;
            debug!("Keepalive sender tick");

            // Get snapshot of connected clients
            let clients: Vec<Arc<RwLock<ConnectedClient>>> = {
                let clients_map = connected_clients.read().await;
                clients_map.values().cloned().collect()
            };

            if clients.is_empty() {
                continue;
            }

            for client in clients {
                let mut client_lock = client.write().await;
                let client_name = client_lock.client.name().to_string();
                let keepalive = Message::KeepAlive(MessageKeepAlive);
                match client_lock.send_message(keepalive).await {
                    Ok(_) => {
                        debug!("Sent keepalive to client '{}'", client_name);
                    }
                    Err(e) => {
                        debug!(
                            "Failed to send keepalive to client '{}': {}",
                            client_name, e
                        );
                    }
                }
            }
        }
    }
}

/// Perform the server-side handshake with a connecting client
///
/// Returns a fully initialized ConnectedClient
async fn perform_server_handshake(
    client_id: ClientId,
    mut stream: TcpStream,
    allowed_clients: &HashMap<String, ClientConfig>,
) -> Result<ConnectedClient> {
    // Disable Nagle's algorithm to ensure low-latency message delivery
    // Without this, small messages get buffered and delayed
    stream.set_nodelay(true)?;

    let mut buffer = Vec::with_capacity(4096);

    // Send hello to client
    let hello = Message::HelloBarrier(MessageHelloBarrier {
        major: 1,
        minor: 8,
        client_name: None, // Server doesn't send its name
    });
    let hello_bytes = hello.to_bytes();
    stream.write_all(&hello_bytes).await?;
    stream.flush().await?;

    // Wait for client's hello response with client name
    let client_name = loop {
        let mut temp_buf = vec![0u8; 4096];
        let n = stream.read(&mut temp_buf).await?;

        if n == 0 {
            return Err(ServerError::ConnectionError(
                "Client disconnected during handshake".to_string(),
            ));
        }

        buffer.extend_from_slice(&temp_buf[..n]);

        match parse_message_with_length(&buffer) {
            Ok((msg, consumed)) => {
                buffer.drain(..consumed);

                match msg {
                    Message::HelloBarrier(hello) => {
                        if let Some(name) = hello.client_name {
                            break name;
                        } else {
                            return Err(ServerError::ConnectionError(
                                "Client hello missing client name".to_string(),
                            ));
                        }
                    }
                    Message::HelloSynergy(hello) => {
                        if let Some(name) = hello.client_name {
                            break name;
                        } else {
                            return Err(ServerError::ConnectionError(
                                "Client hello missing client name".to_string(),
                            ));
                        }
                    }
                    Message::KeepAlive(_) => {
                        // Handle keepalive during handshake
                        let keepalive = Message::KeepAlive(MessageKeepAlive);
                        stream.write_all(&keepalive.to_bytes()).await?;
                        stream.flush().await?;
                        continue;
                    }
                    _ => {
                        return Err(ServerError::ConnectionError(format!(
                            "Expected hello from client, got: {:?}",
                            msg
                        )));
                    }
                }
            }
            Err(ProtocolError::InsufficientData { .. }) => {
                continue; // Need more data
            }
            Err(e) => {
                return Err(ServerError::ProtocolError(e));
            }
        }
    };

    // Verify client is in allowed list
    if !allowed_clients.contains_key(&client_name) {
        return Err(ServerError::UnknownClient(client_name));
    }

    // Send QINF to query client info
    let qinf = Message::QueryInfo(MessageQueryInfo);
    stream.write_all(&qinf.to_bytes()).await?;
    stream.flush().await?;

    // Wait for client info
    let (width, height) = loop {
        let mut temp_buf = vec![0u8; 4096];
        let n = stream.read(&mut temp_buf).await?;

        if n == 0 {
            return Err(ServerError::ConnectionError(
                "Client disconnected during handshake".to_string(),
            ));
        }

        buffer.extend_from_slice(&temp_buf[..n]);

        match parse_message_with_length(&buffer) {
            Ok((msg, consumed)) => {
                buffer.drain(..consumed);

                match msg {
                    Message::ClientInfo(info) => {
                        break (info.width, info.height);
                    }
                    Message::KeepAlive(_) => {
                        // Handle keepalive
                        let keepalive = Message::KeepAlive(MessageKeepAlive);
                        stream.write_all(&keepalive.to_bytes()).await?;
                        stream.flush().await?;
                        continue;
                    }
                    _ => {
                        // Ignore other messages during handshake
                        continue;
                    }
                }
            }
            Err(ProtocolError::InsufficientData { .. }) => {
                continue; // Need more data
            }
            Err(e) => {
                return Err(ServerError::ProtocolError(e));
            }
        }
    };

    // Complete handshake with LSYN, CIAK, CROP sequence
    let lsyn = Message::LegacySynergy(MessageLegacySynergy { data: "en".into() });
    stream.write_all(&lsyn.to_bytes()).await?;
    stream.flush().await?;

    let ciak = Message::InfoAcknowledgment(MessageInfoAcknowledgment);
    stream.write_all(&ciak.to_bytes()).await?;
    stream.flush().await?;

    let crop = Message::ResetOptions(MessageResetOptions);
    stream.write_all(&crop.to_bytes()).await?;
    stream.flush().await?;

    // Send DSOP with no options (empty key-value pairs)
    let dsop = Message::SetOptions(MessageSetOptions { options: vec![] });
    stream.write_all(&dsop.to_bytes()).await?;
    stream.flush().await?;

    let client = Client {
        id: client_id,
        name: client_name,
        width,
        height,
    };

    Ok(ConnectedClient {
        client,
        stream,
        buffer,
    })
}
