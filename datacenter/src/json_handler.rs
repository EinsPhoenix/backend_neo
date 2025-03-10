use log::{info, error};
use serde_json::Value;

use crate::query::create_new_relation;
use crate::db::{get_read_db, get_db};

pub async fn process_json(json: &Value) {
    // For now, just print the received JSON
    info!("Processing JSON: {}", json);
    
    // Here you can add more logic to handle different types of JSON messages
    // based on their structure or content
    if let Some(message_type) = json.get("type") {
        match message_type.as_str() {
            Some("message") => handle_message(json),
            Some("command") => handle_command(json),
            Some("data") => {
                // Await the async function directly
                handle_data(json).await;
            },
            _ => info!("Unknown message type: {:?}", message_type),
        }
    } else {
        info!("JSON has no type field");
    }
}

fn handle_message(json: &Value) {
    if let Some(content) = json.get("content") {
        info!("Received message: {:?}", content);
    }
}

fn handle_command(json: &Value) {
    if let Some(command) = json.get("command") {
        info!("Received command: {:?}", command);
    }
}

async fn handle_data(json: &Value) {
    if let Some(data) = json.get("data") {
        let db = match get_db().await {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to get database connection: {}", e);
                return;
            },
        };
        info!("Received data: {:?}", data);
        
        // Properly await the async function and handle its result
        match create_new_relation(json, &db).await {
            true => info!("Successfully created new relations in Neo4j"),
            false => error!("Failed to create new relations in Neo4j"),
        }
    }
}