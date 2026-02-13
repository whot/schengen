// SPDX-License-Identifier: GPL-3.0-or-later

//! # Schengen Client API
//!
//! The module to build a schengen client. Construct a [Client] via the [Builder], then
//! process the messages as they come in from the server. Connection with the desktop environment
//! is left as an exercise to the reader.
//!
//! # Example
//!
//! ```no_run
//! use schengen::client::{Builder, ClientEvent};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Build and connect to the server
//!     let mut client = Builder::new()
//!         .server_addr("localhost:24801")?
//!         .name("my-client")
//!         .dimensions(1920, 1080)
//!         .connect()
//!         .await?;
//!
//!     // Process events from the server
//!     loop {
//!         match client.recv_event().await? {
//!             ClientEvent::CursorEntered { x, y, .. } => {
//!                 println!("Cursor entered at ({}, {})", x, y);
//!             }
//!             ClientEvent::CursorLeft => {
//!                 println!("Cursor left");
//!             }
//!             ClientEvent::Close => {
//!                 println!("Server closed connection");
//!                 break;
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use crate::protocol::{
    Message, MessageClientInfo, MessageHelloBarrier, MessageKeepAlive, ProtocolError,
    parse_message_with_length,
};

const DEFAULT_PORT: u16 = 24801;

/// Events from the server that the client application should handle
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// Cursor entered this screen at the given location
    CursorEntered {
        x: i16,
        y: i16,
        sequence_number: u32,
        modifier_mask: u16,
    },

    /// Cursor left this screen
    CursorLeft,

    /// Key pressed down
    KeyDown { key: u16, mask: u16, button: u16 },

    /// Key released
    KeyUp { key: u16, mask: u16, button: u16 },

    /// Key repeated
    KeyRepeat {
        key: u16,
        mask: u16,
        count: u16,
        button: u16,
    },

    /// Mouse moved to absolute position
    MouseMove { x: i16, y: i16 },

    /// Mouse moved relative
    MouseRelativeMove { dx: i16, dy: i16 },

    /// Mouse button pressed
    MouseButtonDown { button: u8 },

    /// Mouse button released
    MouseButtonUp { button: u8 },

    /// Mouse wheel scrolled
    MouseWheel { horiz: i16, vert: i16 },

    /// Clipboard data received
    ClipboardData {
        id: u8,
        sequence: u32,
        data: Vec<u8>,
    },

    /// Screen saver state changed
    ScreenSaverChanged { active: bool },

    /// Reset options
    ResetOptions,

    /// Set options (placeholder - expand as needed)
    SetOptions,

    /// Server closed connection
    Close,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Invalid server address: {0}")]
    InvalidServerAddress(String),

    #[error("Failed to connect to server: {0}")]
    ConnectionFailed(#[from] std::io::Error),

    #[error("Max retry attempts ({0}) reached, could not connect to server")]
    MaxRetriesExceeded(usize),

    #[error("Connection timeout exceeded after {0:?}, could not connect to server")]
    ConnectionTimeoutExceeded(Duration),
}

pub type Result<T> = std::result::Result<T, ClientError>;

#[doc(hidden)]
mod sealed {
    pub trait State {}
}

#[doc(hidden)]
pub struct NeedsAddress;
impl sealed::State for NeedsAddress {}

#[doc(hidden)]
pub struct Ready;
impl sealed::State for Ready {}

/// The builder struct to construct a new client
///
/// Use [`Builder::new()`] to create a new builder, then call [`Builder::server_addr()`] to set the
/// server address, and finally [`Builder::connect()`] to establish the connection.
///
/// # Example
///
/// ```no_run
/// use schengen::client::Builder;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Builder::new()
///     .server_addr("192.168.1.100:24801")?
///     .name("my-workstation")
///     .dimensions(2560, 1440)
///     .retry_count(5)
///     .retry_interval(Duration::from_secs(2))
///     .connect()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Builder<S: sealed::State = NeedsAddress> {
    _state: std::marker::PhantomData<S>,
    host: Option<String>,
    port: u16,
    name: Option<String>,
    retry_interval: Duration,
    retry_count: Option<usize>,
    connection_timeout: Option<Duration>,
    width: u16,
    height: u16,
}

impl<S: sealed::State> Builder<S> {
    /// Set the client name to identify this client with at the server.
    ///
    /// Typically the server has a set of permitted client names and will reject unknown ones.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the screen dimensions for this client.
    ///
    /// These dimensions are sent to the server during the handshake.
    pub fn dimensions(mut self, width: u16, height: u16) -> Self {
        self.width = width;
        self.height = height;
        self
    }

    /// Specify the interval between connection retries if the server is unavailable.
    ///
    /// The default is 1 second.
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// Specify the number of times to try to reconnect if the server is unavailable.
    ///
    /// The default is infinite retries, set this to 1 to only attempt one connection.
    pub fn retry_count(mut self, count: usize) -> Self {
        self.retry_count = Some(count);
        self
    }

    /// Specify the total timeout for the entire connection process.
    ///
    /// If set, the connection attempts will stop after this duration,
    /// even if retry_count hasn't been reached.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }
}

// Methods only available on the initial builder state
impl Default for Builder<NeedsAddress> {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder<NeedsAddress> {
    /// Create a new builder instance
    pub fn new() -> Self {
        Builder {
            _state: std::marker::PhantomData,
            host: None,
            port: DEFAULT_PORT,
            name: None,
            retry_interval: Duration::from_secs(1),
            retry_count: None,
            connection_timeout: None,
            width: 1920,
            height: 1080,
        }
    }

    /// A server address in the format `hostname:port` or as `ip:port`.
    ///
    /// The port is optional, you may also use [Builder::port] to set the port separately.
    /// This overwrites the previously set port (if part of the `host` argument).
    pub fn server_addr(self, host: &str) -> Result<Builder<Ready>> {
        let (host, port) = self.parse_host_port(host)?;
        Ok(Builder {
            _state: std::marker::PhantomData,
            host: Some(host),
            port,
            name: self.name,
            retry_interval: self.retry_interval,
            retry_count: self.retry_count,
            connection_timeout: self.connection_timeout,
            width: self.width,
            height: self.height,
        })
    }

    fn parse_host_port(&self, server: &str) -> Result<(String, u16)> {
        // Check if this starts with a bracket (IPv6 with optional port like "[::1]:8080" or "[::1]")
        if server.starts_with('[') {
            if let Some(bracket_end) = server.find(']') {
                let host = &server[1..bracket_end];
                // Check for port after the bracket
                if bracket_end + 1 < server.len()
                    && server.chars().nth(bracket_end + 1) == Some(':')
                {
                    if let Ok(port) = server[bracket_end + 2..].parse::<u16>() {
                        return Ok((host.to_string(), port));
                    }
                }
                return Ok((host.to_string(), DEFAULT_PORT));
            }
        }

        // Count colons to detect IPv6 without brackets
        let colon_count = server.matches(':').count();

        // If there are multiple colons and no brackets, it's an IPv6 address without port
        if colon_count > 1 {
            return Ok((server.to_string(), DEFAULT_PORT));
        }

        // Single colon or no colon - could be host:port or just host
        if let Some(colon_pos) = server.rfind(':') {
            if let Ok(port) = server[colon_pos + 1..].parse::<u16>() {
                let host = &server[..colon_pos];
                return Ok((host.to_string(), port));
            }
        }

        // No port found, use default
        Ok((server.to_string(), DEFAULT_PORT))
    }

    /// Connect using an existing TCP stream (e.g., from systemd socket activation).
    ///
    /// When this function returns, the client state is mostly through the connection handshake.
    ///
    /// # Arguments
    ///
    /// * `stream` - An established TCP stream to use for the connection
    pub async fn connect_with_stream(self, stream: TcpStream) -> Result<Client> {
        // Create a client with the provided stream
        let mut client = Client {
            stream,
            buffer: Vec::with_capacity(4096),
            width: self.width,
            height: self.height,
        };

        let client_name = self.name.as_deref().unwrap_or("schengen-client");

        // Perform the Synergy protocol handshake
        perform_handshake(&mut client, client_name).await?;

        Ok(client)
    }
}

// Methods only available once server address is set
impl Builder<Ready> {
    /// Set the port to connect to.
    ///
    /// This overwrites the previously set port if it was part of the `host` argument in server_addr.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Attempt to connect to the server with retry logic
    async fn try_connect(&self) -> Result<TcpStream> {
        // Safe to unwrap because Ready state guarantees host is set
        let host = self.host.as_ref().unwrap();

        let server_addr = format!("{}:{}", host, self.port);
        let mut attempt = 0;
        let start_time = Instant::now();

        loop {
            attempt += 1;

            if let Some(conn_timeout) = self.connection_timeout {
                if start_time.elapsed() >= conn_timeout {
                    return Err(ClientError::ConnectionTimeoutExceeded(conn_timeout));
                }
            }

            match TcpStream::connect(&server_addr).await {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(_) => {
                    if let Some(max_retries) = self.retry_count {
                        if attempt >= max_retries {
                            return Err(ClientError::MaxRetriesExceeded(max_retries));
                        }
                    }
                    sleep(self.retry_interval).await;
                }
            }
        }
    }

    /// Connect to the server (and keep retrying if it is unavailable, unless
    /// [Builder::retry_count] was set to 1).
    ///
    /// When this function returns, the client state is mostly through the connection handshake.
    pub async fn connect(self) -> Result<Client> {
        // Establish TCP connection with retries
        let stream = self.try_connect().await?;

        // Create a client with the stream
        let mut client = Client {
            stream,
            buffer: Vec::with_capacity(4096),
            width: self.width,
            height: self.height,
        };

        let client_name = self.name.as_deref().unwrap_or("schengen-client");

        // Perform the Synergy protocol handshake
        perform_handshake(&mut client, client_name).await?;

        Ok(client)
    }
}

/// Send client information to the server
async fn send_client_info(client: &mut Client) -> Result<()> {
    let client_info = Message::ClientInfo(MessageClientInfo {
        x: 0,
        y: 0,
        width: client.width,
        height: client.height,
        current_mouse_x: 0,
        current_mouse_y: 0,
        size: 0,
    });

    client.send_message(client_info).await?;
    Ok(())
}

/// Perform the Synergy protocol handshake on a client
async fn perform_handshake(client: &mut Client, client_name: &str) -> Result<()> {
    // Step 1: Wait for server hello and respond
    loop {
        match client.recv_message().await? {
            Message::HelloBarrier(_hello_msg) => {
                // Respond with our hello including client name
                let hello_response = Message::HelloBarrier(MessageHelloBarrier {
                    major: 1,
                    minor: 8,
                    client_name: Some(client_name.to_string()),
                });
                client.send_message(hello_response).await?;
                break;
            }
            Message::HelloSynergy(_hello_msg) => {
                // Respond with Barrier hello including client name
                let hello_response = Message::HelloBarrier(MessageHelloBarrier {
                    major: 1,
                    minor: 8,
                    client_name: Some(client_name.to_string()),
                });
                client.send_message(hello_response).await?;
                break;
            }
            // Shouldn't really happen here anyway but...
            Message::KeepAlive(_) => {
                let keepalive_response = Message::KeepAlive(MessageKeepAlive);
                client.send_message(keepalive_response).await?;
                continue;
            }
            // Shouldn't really happen here anyway but...
            Message::NoOp(_) => {
                continue;
            }
            msg => {
                return Err(ClientError::InvalidServerAddress(format!(
                    "Expected hello from server, got: {:?}",
                    msg
                )));
            }
        }
    }

    // Step 2: Wait for QueryInfo and respond with ClientInfo
    loop {
        match client.recv_message().await? {
            Message::QueryInfo(_) => {
                send_client_info(client).await?;
                break;
            }
            Message::KeepAlive(_) => {
                let keepalive_response = Message::KeepAlive(MessageKeepAlive);
                client.send_message(keepalive_response).await?;
                continue;
            }
            Message::NoOp(_) => {
                continue;
            }
            msg => {
                return Err(ClientError::InvalidServerAddress(format!(
                    "Expected QueryInfo from server, got: {:?}",
                    msg
                )));
            }
        }
    }

    // Step 3: Wait for the rest of the handshake messages (LSYN, CIAK, CROP, DSOP)
    // We need to receive all of these before the handshake is complete
    let mut received_lsyn = false;
    let mut received_ciak = false;
    let mut received_crop = false;
    let mut received_dsop = false;

    while !received_lsyn || !received_ciak || !received_crop || !received_dsop {
        match client.recv_message().await? {
            Message::LegacySynergy(_) => {
                received_lsyn = true;
            }
            Message::InfoAcknowledgment(_) => {
                received_ciak = true;
            }
            Message::ResetOptions(_) => {
                received_crop = true;
            }
            Message::SetOptions(_) => {
                received_dsop = true;
            }
            Message::KeepAlive(_) => {
                let keepalive_response = Message::KeepAlive(MessageKeepAlive);
                client.send_message(keepalive_response).await?;
                continue;
            }
            Message::NoOp(_) => {
                continue;
            }
            _ => {
                // Ignore other messages during handshake
                continue;
            }
        }
    }

    Ok(())
}

/// A client connected to a schengen server
pub struct Client {
    stream: TcpStream,
    buffer: Vec<u8>,
    width: u16,
    height: u16,
}

impl Client {
    /// Send a protocol message to the server
    async fn send_message(&mut self, message: Message) -> Result<()> {
        let bytes = message.to_bytes();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(ClientError::ConnectionFailed)?;
        self.stream
            .flush()
            .await
            .map_err(ClientError::ConnectionFailed)?;
        Ok(())
    }

    /// Send a raw protocol message to the server (public API)
    ///
    /// This is useful for sending messages that are not automatically handled by the client,
    /// such as clipboard data.
    pub async fn send(&mut self, message: Message) -> Result<()> {
        self.send_message(message).await
    }

    async fn recv_message(&mut self) -> Result<Message> {
        loop {
            // Try to parse a message from the buffer
            if !self.buffer.is_empty() {
                match parse_message_with_length(&self.buffer) {
                    Ok((msg, consumed)) => {
                        // Remove consumed bytes from buffer
                        self.buffer.drain(..consumed);
                        return Ok(msg);
                    }
                    Err(ProtocolError::InsufficientData { .. }) => {
                        // Need more data, continue reading
                    }
                    Err(e) => {
                        return Err(ClientError::InvalidServerAddress(format!(
                            "Protocol error: {}",
                            e
                        )));
                    }
                }
            }

            // Read more data from the stream
            let mut temp_buf = vec![0u8; 4096];
            let n = self
                .stream
                .read(&mut temp_buf)
                .await
                .map_err(ClientError::ConnectionFailed)?;

            if n == 0 {
                return Err(ClientError::InvalidServerAddress(
                    "Connection closed by server".to_string(),
                ));
            }

            self.buffer.extend_from_slice(&temp_buf[..n]);
        }
    }

    /// Receive the next event from the server.
    ///
    /// This automatically handles protocol-level messages like [MessageKeepAlive] internally. Only
    /// returns events that the caller needs to act on.
    pub async fn recv_event(&mut self) -> Result<ClientEvent> {
        loop {
            match self.recv_message().await? {
                // Auto-handle keepalives
                Message::KeepAlive(_) => {
                    self.send_message(Message::KeepAlive(MessageKeepAlive))
                        .await?;
                    continue;
                }

                // Auto-handle QueryInfo
                Message::QueryInfo(_) => {
                    send_client_info(self).await?;
                    continue;
                }

                // Auto-handle NoOp
                Message::NoOp(_) => {
                    continue;
                }

                // Return events caller cares about
                Message::CursorEntered(msg) => {
                    return Ok(ClientEvent::CursorEntered {
                        x: msg.x,
                        y: msg.y,
                        sequence_number: msg.sequence,
                        modifier_mask: msg.mask,
                    });
                }

                Message::CursorLeft(_) => {
                    return Ok(ClientEvent::CursorLeft);
                }

                Message::KeyDown(msg) => {
                    return Ok(ClientEvent::KeyDown {
                        key: msg.keyid,
                        mask: msg.mask,
                        button: msg.button,
                    });
                }

                Message::KeyUp(msg) => {
                    return Ok(ClientEvent::KeyUp {
                        key: msg.keyid,
                        mask: msg.mask,
                        button: msg.button,
                    });
                }

                Message::KeyRepeat(msg) => {
                    return Ok(ClientEvent::KeyRepeat {
                        key: msg.keyid,
                        mask: msg.mask,
                        count: msg.count,
                        button: msg.button,
                    });
                }

                Message::MouseMove(msg) => {
                    return Ok(ClientEvent::MouseMove { x: msg.x, y: msg.y });
                }

                Message::MouseRelativeMove(msg) => {
                    return Ok(ClientEvent::MouseRelativeMove {
                        dx: msg.x,
                        dy: msg.y,
                    });
                }

                Message::MouseButtonDown(msg) => {
                    return Ok(ClientEvent::MouseButtonDown { button: msg.button });
                }

                Message::MouseButtonUp(msg) => {
                    return Ok(ClientEvent::MouseButtonUp { button: msg.button });
                }

                Message::MouseWheel(msg) => {
                    return Ok(ClientEvent::MouseWheel {
                        horiz: msg.xdelta,
                        vert: msg.ydelta,
                    });
                }

                Message::ClipboardData(msg) => {
                    return Ok(ClientEvent::ClipboardData {
                        id: msg.id,
                        sequence: msg.sequence,
                        data: msg.data.0.into_bytes(),
                    });
                }

                Message::ScreenSaverChange(msg) => {
                    return Ok(ClientEvent::ScreenSaverChanged {
                        active: msg.state != 0,
                    });
                }

                Message::ResetOptions(_) => {
                    return Ok(ClientEvent::ResetOptions);
                }

                Message::SetOptions(_) => {
                    return Ok(ClientEvent::SetOptions);
                }

                Message::InfoAcknowledgment(_) => {
                    // Auto-handle InfoAcknowledgment
                    continue;
                }

                Message::Close(_) => {
                    // Shutdown the connection before returning the Close event
                    let _ = self.stream.shutdown().await;
                    return Ok(ClientEvent::Close);
                }

                // Ignore handshake messages (shouldn't happen after connect)
                Message::HelloBarrier(_) | Message::HelloSynergy(_) => {
                    continue;
                }

                // Ignore other messages
                _ => {
                    continue;
                }
            }
        }
    }
}
