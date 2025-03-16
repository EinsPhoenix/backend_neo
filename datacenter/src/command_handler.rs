use crate::query::reset_database_and_set_topology;
use log::{error, info};
use std::process::exit;

use neo4rs::Graph;
use std::sync::Arc;


pub async fn router(command: &str, db: Arc<Graph>) -> Result<bool, String> {
    
    match command {
        "exit" => {
            info!("Exiting application...");
            exit(0);
        }
        "reset" => {
            info!("Resetting the server...");
            match reset_database_and_set_topology(&db).await {
                Ok(_) => {
                    info!("Database reset successfully");
                    Ok(true)
                },
                Err(e) => {
                    error!("Failed to reset database: {}", e);
                    Err(format!("Failed to reset database: {}", e))
                }
            }
        }
        "status" => {
            Ok(true)
        }
        _ => {
            error!("Invalid command: {}", command);
            Err(format!("Invalid command: {}", command))
        },
    }
}