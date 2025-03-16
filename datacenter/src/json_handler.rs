use log::{info, error};
use serde_json::Value;
use neo4rs::Graph;
use std::sync::Arc;

use crate::query::create_new_relation;
use crate::command_handler::router;

pub async fn process_json(json: &Value, db: Arc<Graph>) {
    info!("Processing JSON: {}", json);
    
 
    if let Some(message_type) = json.get("type") {
        match message_type.as_str() {
            Some("message") => handle_message(json),
            Some("command") => handle_command(json, db).await,
            Some("data") => {
                
                handle_data(json, db).await;
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

async fn handle_command(json: &Value, db: Arc<Graph>) {
    if let Some(command) = json.get("command") {
        info!("Received command: {:?}", command);
        
        
        if let Some(cmd_str) = command.as_str() {
            match router(cmd_str, db).await {
                Ok(true) => info!("Command '{}' executed successfully", cmd_str),
                Ok(false) => error!("Command '{}' execution failed", cmd_str),
                Err(e) => error!("Error processing command '{}': {}", cmd_str, e),
            }
        } else {
            error!("Command is not a string: {:?}", command);
        }
    } else {
        error!("No 'command' field found in JSON");
    }
}


async fn handle_data(json: &Value, db: Arc<Graph>) {
    if let Some(data) = json.get("data") {
        
        info!("Received data: {:?}", data);
      
        match create_new_relation(json, &db).await {
            true => info!("Successfully created new relations in Neo4j"),
            false => error!("Failed to create new relations in Neo4j"),
        }
    }
}