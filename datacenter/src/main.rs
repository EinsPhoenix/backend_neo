use log::{info, error};
use neo4rs::Graph;
use dotenv::dotenv;
use std::env;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use serde_json::{Value, Error as JsonError};
use std::sync::Arc; 

mod db;
mod auth;
mod json_handler;
mod query;
mod mqtt_handler;
mod command_handler;

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv().ok();
    
    if env_logger::try_init().is_err() {
        eprintln!("Logger already initialized.");
    }
    info!("Starting the server...");
    
    // Get the database connection
    let db = match db::get_db().await {
        Ok(db) => db, 
        Err(e) => {
            error!("Failed to get database connection: {}", e);
            return Err(io::Error::new(io::ErrorKind::Other, format!("Database connection failed: {}", e)));
        },
    };

   
    let db_clone = Arc::clone(&db);
    tokio::spawn(async move {
        if let Err(e) = mqtt_handler::start_mqtt_client(db_clone).await {
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
                let db_clone = Arc::clone(&db);
                
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket, password_clone, db_clone).await {
                        error!("Error handling client {}: {:?}", addr, e);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn handle_client(
    mut socket: TcpStream,
    correct_password: String,
    db: Arc<Graph>  
) -> io::Result<()> {
    if !auth::authenticate_client(&mut socket, &correct_password).await? {
        return Ok(());
    }
    
    loop {
        match receive_json(&mut socket).await {
            Ok(Some(json)) => {
                json_handler::process_json(&json, Arc::clone(&db)).await;
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
    let mut buf = vec![0; 11 * 1024 * 1024]; 
    let n = socket.read(&mut buf).await?;
    
    if n == 0 {
        return Ok(None); 
    }
    
    buf.truncate(n);
    
    match String::from_utf8(buf) {
        Ok(data) => {
            info!("Empfangene Datengröße: {} Bytes", data.len());
            match serde_json::from_str::<Value>(&data) {
                Ok(json) => {
                    info!("JSON erfolgreich geparst");
                    Ok(Some(json))
                },
                Err(e) => {
                    error!("Ungültiges JSON-Format: {:?}", e);
                    socket.write_all(b"Fehler: Ungueltiges JSON-Format\n").await?;
                    Err(io::Error::new(io::ErrorKind::InvalidData, e))
                }
            }
        },
        Err(e) => {
            error!("Ungültige UTF-8 Daten empfangen: {:?}", e);
            socket.write_all(b"Fehler: Ungueltige UTF-8 Kodierung\n").await?;
            Err(io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}