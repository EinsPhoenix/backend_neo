use crate::query::reset_database_and_set_topology;
use crate::db::{get_read_db, get_db};
use log::{error, info};
use std::process::exit;



pub async fn router(command: &str) -> Result<bool, String> {
    let db = match get_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to get database connection: {}", e);
            return Err(format!("Failed to get database connection: {}", e));
        },
    };
    
    match command {
        "exit" => {
            info!("Exiting application...");
            exit(0);
        }
        "reset" => {
            info!("Resetting the server...");
            match reset_database_and_set_topology(db).await {
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