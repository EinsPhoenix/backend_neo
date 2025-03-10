use log::{info, error};
use dotenv::dotenv;
use std::env;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use serde_json::{Value, Error as JsonError};

mod db;
mod auth;
mod json_handler;
mod query;
mod mqtt_handler;

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv().ok();
    
    if env_logger::try_init().is_err() {
        eprintln!("Logger already initialized.");
    }
    info!("Starting the server...");
    match db::get_db().await {
        Ok(_) => info!("Database connection established."),
        Err(e) => {
            error!("Database connection failed: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to connect to the database"));
        }
    }

    // Start MQTT client
    tokio::spawn(async {
        if let Err(e) = mqtt_handler::start_mqtt_client().await {
            error!("MQTT client error: {:?}", e);
        }
    });

    let password = env::var("SERVER_PASSWORD").expect("SERVER_PASSWORD not set in .env");
    let listener = TcpListener::bind("0.0.0.0:12345").await?;
    info!("Server is listening on 0.0.0.0:12345");
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("New connection from: {}", addr);
                let password_clone = password.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket, password_clone).await {
                        error!("Error handling client {}: {:?}", addr, e);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn handle_client(mut socket: TcpStream, correct_password: String) -> io::Result<()> {
    if !auth::authenticate_client(&mut socket, &correct_password).await? {
        return Ok(());
    }
    
    loop {
        match receive_json(&mut socket).await {
            Ok(Some(json)) => {
                json_handler::process_json(&json).await;
            },
            Ok(None) => {
                info!("Client disconnected.");
                break;
            },
            Err(e) => {
                error!("Error receiving JSON: {:?}", e);
                break;
            }
        }
    }
    
    Ok(())
}

async fn receive_json(socket: &mut TcpStream) -> io::Result<Option<Value>> {
    let mut buf = [0; 4096];
    let n = socket.read(&mut buf).await?;
    
    if n == 0 {
        return Ok(None); // Client disconnected
    }
    
    let data = String::from_utf8_lossy(&buf[..n]);
    match serde_json::from_str::<Value>(&data) {
        Ok(json) => {
            info!("Received JSON: {}", json);
            Ok(Some(json))
        },
        Err(e) => {
            error!("Invalid JSON received: {:?}", e);
            socket.write_all(b"Error: Invalid JSON format\n").await?;
            Err(io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}