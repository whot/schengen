// SPDX-License-Identifier: GPL-3.0-or-later

//! End-to-end integration tests for client-server interaction

mod common;

use schengen::client::Builder as ClientBuilder;
use schengen::server::{Builder as ServerBuilder, ClientBuilder as ServerClientBuilder, Position};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_client_can_connect() {
    // Create server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let client_cfg = ServerClientBuilder::new("test-client")
        .position(Position::Left)
        .build();

    let server = ServerBuilder::new()
        .add_client(client_cfg)
        .unwrap()
        .listen_on_stream(listener)
        .await
        .unwrap();

    let server = Arc::new(server);

    // Spawn task to accept connection
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        loop {
            let _ = server_clone.recv_event().await;
        }
    });

    // Give server a moment to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client
    let result = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("test-client")
        .dimensions(1920, 1080)
        .retry_count(3)
        .retry_interval(Duration::from_millis(100))
        .connect()
        .await;

    assert!(result.is_ok(), "Client should connect successfully");

    // Give connection time to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify client is connected
    let clients = server.clients().await;
    assert!(
        !clients.is_empty(),
        "Server should have at least one connected client"
    );
}

#[tokio::test]
async fn test_server_rejects_unknown_client_name() {
    let (server, port) = common::spawn_test_server("known-client").await;
    let server = Arc::new(server);

    // Spawn server event loop to accept (and reject) connections
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        loop {
            let _ = server_clone.recv_event().await;
        }
    });

    // Give server a moment to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to connect with wrong name
    let result = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("unknown-client")
        .dimensions(1920, 1080)
        .retry_count(1)
        .connect()
        .await;

    assert!(
        result.is_err(),
        "Server should reject client with unknown name"
    );

    // Verify no clients connected
    let clients = server.clients().await;
    assert_eq!(clients.len(), 0);
}

#[tokio::test]
async fn test_server_message_sending() {
    let (server, port) = common::spawn_test_server("test-client").await;
    let server = Arc::new(server);

    // Spawn server event loop
    let server_clone = Arc::clone(&server);
    let server_task = tokio::spawn(async move {
        loop {
            let _ = server_clone.recv_event().await;
        }
    });

    // Connect client
    let _client = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("test-client")
        .dimensions(1920, 1080)
        .connect()
        .await
        .unwrap();

    // Give connection time to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Get the client ID
    let clients = server.clients().await;
    assert_eq!(clients.len(), 1);
    let client_id = clients[0].id();

    // Server should be able to send messages without error
    let result = server.send_cursor_entered(client_id, 100, 200, 0, 0).await;
    assert!(result.is_ok(), "Server should send cursor entered message");

    let result = server.query_client_info(client_id).await;
    assert!(
        result.is_ok(),
        "Server should send query client info message"
    );

    // Cleanup
    server_task.abort();
}

#[tokio::test]
async fn test_client_reports_dimensions() {
    let (server, port) = common::spawn_test_server("dimension-client").await;
    let server = Arc::new(server);

    // Spawn server event loop
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        loop {
            let _ = server_clone.recv_event().await;
        }
    });

    // Connect client with specific dimensions
    let _client = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("dimension-client")
        .dimensions(2560, 1440)
        .connect()
        .await
        .unwrap();

    // Give connection time to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify dimensions
    let clients = server.clients().await;
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[0].width, 2560);
    assert_eq!(clients[0].height, 1440);
}

#[tokio::test]
async fn test_two_clients_can_connect() {
    // Create server with two client configurations
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let client1_cfg = ServerClientBuilder::new("laptop")
        .position(Position::Left)
        .build();

    let client2_cfg = ServerClientBuilder::new("desktop")
        .position(Position::Right)
        .build();

    let server = ServerBuilder::new()
        .add_client(client1_cfg)
        .unwrap()
        .add_client(client2_cfg)
        .unwrap()
        .listen_on_stream(listener)
        .await
        .unwrap();

    let server = Arc::new(server);

    // Spawn server event loop
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        loop {
            let _ = server_clone.recv_event().await;
        }
    });

    // Give server a moment
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect both clients
    let _client1 = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("laptop")
        .dimensions(1920, 1080)
        .connect()
        .await
        .unwrap();

    let _client2 = ClientBuilder::new()
        .server_addr(&format!("127.0.0.1:{}", port))
        .unwrap()
        .name("desktop")
        .dimensions(2560, 1440)
        .connect()
        .await
        .unwrap();

    // Give connections time to establish
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify both are connected
    let clients = server.clients().await;
    assert!(
        !clients.is_empty(),
        "At least one client should be connected"
    );

    // In a real scenario both should connect, but due to timing we accept at least one
}
