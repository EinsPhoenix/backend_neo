use neo4rs::{Graph, query};
use log::{error, info, warn};
use serde_json::{json, Value, Deserializer};
use std::collections::HashMap;


// validate function

async fn validate_data(data: &Value) -> bool {
    let data_array = match data.get("data").and_then(|d| d.as_array()) {
        Some(array) => array,
        _ => {
            error!("Invalid array");
            return false;
        }
    };

    for item in data_array {
        if !validate_item(item).await {
            error!("Invalid item in data array: {:?}", item);
            return false;
        }
    }

    true
}

async fn validate_item(item: &Value) -> bool {
    item.get("uuid").and_then(|v| v.as_str()).is_some()
        && item.get("color").and_then(|v| v.as_str()).is_some()
        && item.get("sensor_data").map(|v| v.is_object()).unwrap_or(false)
        && item.get("sensor_data").and_then(|sd| sd.get("temperature")).and_then(|t| t.as_f64()).is_some()
        && item.get("sensor_data").and_then(|sd| sd.get("humidity")).and_then(|h| h.as_f64()).is_some() 
        && item.get("timestamp").and_then(|v| v.as_str()).is_some()
        && item.get("energy_consume").and_then(|v| v.as_f64()).is_some()
        && item.get("energy_cost").and_then(|v| v.as_f64()).is_some()
}

//create function
pub async fn create_new_relation(data: &Value, graph: &Graph) -> Result<bool, String> {
    if !validate_data(data).await {
        return Err("Data validation failed".to_string());
    }

    let data_array = match data.get("data") {
        Some(data) => match data.as_array() {
            Some(array) => array,
            None => return Err("'data' field is not an array".to_string())
        },
        None => return Err("'data' field is missing".to_string())
    };

    let neo4j_data: Vec<HashMap<String, Value>> = data_array.iter().map(|item| {
        let mut record = HashMap::new();
        if let Some(uuid) = item.get("uuid").and_then(|v| v.as_str()) {
            record.insert("uuid".to_string(), Value::String(uuid.to_string()));
        }
        if let Some(color) = item.get("color").and_then(|v| v.as_str()) {
            record.insert("color".to_string(), Value::String(color.to_string()));
        }
        if let Some(timestamp) = item.get("timestamp").and_then(|v| v.as_str()) {
            record.insert("timestamp".to_string(), Value::String(timestamp.to_string()));
        }
        if let Some(energy_consume) = item.get("energy_consume").and_then(|v| v.as_f64()) {
            if let Some(num) = serde_json::Number::from_f64(energy_consume) {
                record.insert("energy_consume".to_string(), Value::Number(num));
            }
        }
        if let Some(energy_cost) = item.get("energy_cost").and_then(|v| v.as_f64()) {
            if let Some(num) = serde_json::Number::from_f64(energy_cost) {
                record.insert("energy_cost".to_string(), Value::Number(num));
            }
        }
        if let Some(sensor_data) = item.get("sensor_data").and_then(|v| v.as_object()) {
            if let Some(temp) = sensor_data.get("temperature").and_then(|v| v.as_f64()) {
                if let Some(num) = serde_json::Number::from_f64(temp) {
                    record.insert("sensor_data.temperature".to_string(), Value::Number(num));
                }
            }
            if let Some(humidity) = sensor_data.get("humidity").and_then(|v| v.as_f64()) {
                if let Some(num) = serde_json::Number::from_f64(humidity) {
                    record.insert("sensor_data.humidity".to_string(), Value::Number(num));
                }
            }
        }
        record
    }).collect();

    let json_data = match serde_json::to_string(&neo4j_data) {
        Ok(s) => s,
        Err(e) => {
            let error_msg = format!("Failed to serialize data: {}", e);
            error!("{}", error_msg);
            return Err(error_msg);
        }
    };

    let creation_query = query(r#"
        WITH apoc.convert.fromJsonList($data) AS records
        UNWIND records AS record
        OPTIONAL MATCH (existingUuid:UUID {id: record.uuid})
        WITH record, existingUuid
        WHERE existingUuid IS NULL
        MERGE (uuid:UUID {id: record.uuid})
        SET uuid.energy_consume = record.energy_consume,
            uuid.energy_cost = record.energy_cost,
            uuid.color = record.color,
            uuid.timestamp = record.timestamp,
            uuid.temperature = record.`sensor_data.temperature`,
            uuid.humidity = record.`sensor_data.humidity`
        MERGE (color:Color {value: record.color})
        MERGE (uuid)-[:HAS_COLOR]->(color)
        MERGE (temperature:Temperature {value: record.`sensor_data.temperature`})
        MERGE (uuid)-[:HAS_TEMPERATURE]->(temperature)
        MERGE (humidity:Humidity {value: record.`sensor_data.humidity`})
        MERGE (uuid)-[:HAS_HUMIDITY]->(humidity)
        MERGE (timestamp:Timestamp {value: record.timestamp})
        MERGE (uuid)-[:HAS_TIMESTAMP]->(timestamp)
        MERGE (timestamp)-[:SENSOR_DATA]->(temperature)
        MERGE (timestamp)-[:SENSOR_DATA]->(humidity)
        MERGE (energyCost:EnergyCost {value: record.energy_cost})
        MERGE (uuid)-[:HAS_ENERGYCOST]->(energyCost)
        MERGE (timestamp)-[:HAS_PRICE]->(energyCost)
        MERGE (energyConsume:EnergyConsume {value: record.energy_consume})
        MERGE (uuid)-[:HAS_ENERGYCONSUME]->(energyConsume)
        RETURN uuid.id AS processed_uuid
    "#)
    .param("data", json_data);

    match graph.execute(creation_query).await {
        Ok(mut result) => {
            let mut processed_count = 0;
            while let Ok(Some(_)) = result.next().await {
                processed_count += 1;
            }
            if processed_count > 0 {
                info!("Processed: {} Node(s)", processed_count);
                Ok(true)
            } else {
                let warning_msg = "No new Nodes were created (UUIDs might already exist)";
                warn!("{}", warning_msg);
                Ok(false)
            }
        }
        Err(e) => {
            let error_msg = format!("Failed to execute Neo4j query: {}", e);
            error!("{}", error_msg);
            Err(error_msg)
        }
    }
}






pub async fn get_specific_uuid_node(uuid: &str, graph: &Graph) -> Option<Value> {
    let start_time = std::time::Instant::now();
    
  
    let query = query(r#"
        MATCH (uuidNode:UUID {id: $uuid})
        RETURN uuidNode.id AS uuid,
            uuidNode.color AS color,
            uuidNode.temperature AS temperature,
            uuidNode.humidity AS humidity,
            uuidNode.timestamp AS timestamp,
            uuidNode.energy_consume AS energy_consume,
            uuidNode.energy_cost AS energy_cost
        LIMIT 1
    "#)
    .param("uuid", uuid);

    match graph.execute(query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
              
                let uuid_val: String = row.get("uuid").unwrap_or_default();
                let color_val: String = row.get("color").unwrap_or_default();
                let temperature: f64 = row.get("temperature").unwrap_or(0.0);
                let humidity: f64 = row.get("humidity").unwrap_or(0.0);
                let timestamp_val: String = row.get("timestamp").unwrap_or_default();
                let energy_consume: f64 = row.get("energy_consume").unwrap_or(0.0);
                let energy_cost: f64 = row.get("energy_cost").unwrap_or(0.0);

                
                
                
             
                Some(json!({
                    "uuid": uuid_val,
                    "color": color_val,
                    "sensor_data": {
                        "temperature": temperature,
                        "humidity": humidity
                    },
                    "timestamp": timestamp_val,
                    "energy_consume": energy_consume,
                    "energy_cost": energy_cost
                }))
            } else {
                None
            }
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle UUID-Nodes zu bekommen und in JSON umzuwandeln
pub async fn get_all_uuid_nodes(graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuidNode:UUID)
        RETURN uuidNode.id AS uuid,
            uuidNode.color AS color,
            { temperature: uuidNode.temperature, humidity: uuidNode.humidity } AS sensor_data,
            uuidNode.timestamp AS timestamp,
            uuidNode.energy_consume AS energy_consume,
            uuidNode.energy_cost AS energy_cost
        ORDER BY uuidNode.timestamp DESC
    "#);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let uuid_val: String = row.get("uuid").unwrap_or_default();
                let color_val: String = row.get("color").unwrap_or_default();
                let sensor_data: Value = row.get("sensor_data").unwrap_or(json!({}));
                let timestamp_val: String = row.get("timestamp").unwrap_or_default();
                let energy_consume: f64 = row.get("energy_consume").unwrap_or(0.0);
                let energy_cost: f64 = row.get("energy_cost").unwrap_or(0.0);

                uuids.push(json!({
                    "uuid": uuid_val,
                    "color": color_val,
                    "sensor_data": {
                        "temperature": sensor_data["temperature"].as_f64().unwrap_or(0.0),
                        "humidity": sensor_data["humidity"].as_f64().unwrap_or(0.0)
                    },
                    "timestamp": timestamp_val,
                    "energy_consume": energy_consume,
                    "energy_cost": energy_cost
                }));
            }

            
            info!("Returned nodes count: {}", uuids.len());

            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}



pub async fn export_all_with_relationships(graph: &Graph, limit: Option<usize>) -> Option<Value> {
    
    let limit_clause = match limit {
        Some(l) => format!("LIMIT {}", l),
        None => "".to_string()
    };
    
    let query_str = format!(r#"
        MATCH p=()-[]->() 
        {}  
        WITH collect(p) AS paths
        RETURN apoc.convert.toJson(paths) AS json_result;
        
    "#, limit_clause);
    
    let query = query(&query_str);
    
    match graph.execute(query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                if let Ok(json_str) = row.get::<String>("json_result") {
                    match serde_json::from_str(&json_str) {
                        Ok(value) => Some(value),
                        Err(e) => {
                            error!("Failed to parse JSON string: {}", e);
                            None
                        }
                    }
                } else {
                    error!("Failed to get JSON result from row");
                    None
                }
            } else {
                
                Some(serde_json::Value::Array(Vec::new()))
            }
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

pub async fn get_temperature_humidity_at_time(graph: &Graph, timestamp: &str) -> Option<(f64, f64)> {
    let cypher_query = query(r#"
        MATCH (t:Timestamp {value: $timestamp})-[:SENSOR_DATA]->(temp:Temperature),
              (t)-[:SENSOR_DATA]->(hum:Humidity)
        RETURN temp.value AS temperature, hum.value AS humidity
    "#)
    .param("timestamp", timestamp);

    match graph.execute(cypher_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let temperature: f64 = row.get("temperature").unwrap_or_default();
                let humidity: f64 = row.get("humidity").unwrap_or_default();
                return Some((temperature, humidity));
            }
            None
        }
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle Nodes innerhalb eines Zeitraums zu bekommen
pub async fn get_nodes_in_time_range(start: &str, end: &str, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID)-[:HAS_TIMESTAMP]->(timestamp:Timestamp)
        WHERE timestamp.value >= $start AND timestamp.value <= $end
        RETURN uuid
    "#)
    .param("start", start)
    .param("end", end);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle Nodes mit einer bestimmten Temperatur oder Luftfeuchtigkeit zu bekommen
pub async fn get_nodes_with_temperature_or_humidity(temp: f64, humidity: f64, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID)-[:HAS_TEMPERATURE]->(temperature:Temperature {value: $temp}),
              (uuid)-[:HAS_HUMIDITY]->(humidity:Humidity {value: $humidity})
        RETURN uuid
    "#)
    .param("temp", temp)
    .param("humidity", humidity);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle Nodes mit einer bestimmten Energiekosten zu bekommen
pub async fn get_nodes_with_energy_cost(energy_cost: f64, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID)-[:HAS_ENERGYCOST]->(energyCost:EnergyCost {value: $energy_cost})
        RETURN uuid
    "#)
    .param("energy_cost", energy_cost);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle Nodes mit einem bestimmten Energieverbrauch zu bekommen
pub async fn get_nodes_with_energy_consume(energy_consume: f64, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID)-[:HAS_ENERGYCONSUME]->(energyConsume:EnergyConsume {value: $energy_consume})
        RETURN uuid
    "#)
    .param("energy_consume", energy_consume);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Funktion, um alle Nodes mit einer bestimmten Farbe zu bekommen
pub async fn get_nodes_with_color(color: &str, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID)-[:HAS_COLOR]->(color:Color {value: $color})
        RETURN uuid
    "#)
    .param("color", color);

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}


pub async fn get_newest_uuid(graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuidNode:UUID)-[:HAS_TIMESTAMP]->(timestamp:Timestamp)
        WITH uuidNode, timestamp
        ORDER BY timestamp.value DESC
        RETURN uuidNode.id AS uuid
        LIMIT 50
    "#);
    
    match graph.execute(query).await {
        Ok(mut result) => {
            let mut uuids = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                uuids.push(node);
            }
            Some(json!(uuids))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

pub async fn get_paginated_uuids(graph: &Graph, page: usize) -> Option<Value> {
    const PAGE_SIZE: usize = 25;
    
  
    let count_query = query(r#"
        MATCH (uuidNode:UUID)
        RETURN count(uuidNode) AS total
    "#);
    
    let total_count = match graph.execute(count_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let count: i64 = row.get("total").unwrap_or(0);
                count as usize
            } else {
                0
            }
        },
        Err(e) => {
            error!("Failed to execute count query: {}", e);
            return None;
        }
    };
    
    
    let total_pages = if total_count == 0 {
        0
    } else {
        (total_count + PAGE_SIZE - 1) / PAGE_SIZE 
    };
    
    
    let skip = page * PAGE_SIZE;
    
   
    let data_query = query(r#"
        MATCH (uuidNode:UUID)-[:HAS_TIMESTAMP]->(timestamp:Timestamp)
        WITH uuidNode, timestamp
        ORDER BY timestamp.value DESC
        RETURN 
            uuidNode.id AS uuid,
            uuidNode.color AS color,
            uuidNode.temperature AS temperature,
            uuidNode.humidity AS humidity,
            uuidNode.timestamp AS timestamp,
            uuidNode.energy_consume AS energy_consume,
            uuidNode.energy_cost AS energy_cost
        SKIP $skip
        LIMIT $limit
    "#)
    .param("skip", skip as i64)
    .param("limit", PAGE_SIZE as i64);
    
    match graph.execute(data_query).await {
        Ok(mut result) => {
            let mut nodes = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                
                let node = json!({
                    "uuid": row.get::<Value>("uuid").unwrap_or(Value::Null),
                    "color": row.get::<Value>("color").unwrap_or(Value::Null),
                    "temperature": row.get::<Value>("temperature").unwrap_or(Value::Null),
                    "humidity": row.get::<Value>("humidity").unwrap_or(Value::Null),
                    "timestamp": row.get::<Value>("timestamp").unwrap_or(Value::Null),
                    "energy_consume": row.get::<Value>("energy_consume").unwrap_or(Value::Null),
                    "energy_cost": row.get::<Value>("energy_cost").unwrap_or(Value::Null)
                });
                nodes.push(node);
            }
            
            info!("Returning page {} of {} with {} UUIDs", 
                  page, total_pages, nodes.len());
            
            Some(json!({
                "nodes": nodes,
                "pagination": {
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "current_page": page,
                    "page_size": PAGE_SIZE
                }
            }))
        },
        Err(e) => {
            error!("Failed to execute Neo4j query: {}", e);
            None
        }
    }
}

// Here are command functions

pub async fn alter_database(graph: &Graph) -> Result<bool, String> {
    
    let delete_query = query(r#"
       
        ALTER DATABASE neo4j SET TOPOLOGY 1 PRIMARIES 2 SECONDARIES;
      
    "#);

    match graph.execute(delete_query).await {
        Ok(_) => {
            log::info!("Database successfully altered");
            Ok(true)
        },
        Err(e) => {
            let error_msg = format!("There was an issue altering the database {}", e);
            error!("{}", error_msg);
            return Err(error_msg);
        }
    }

}

pub async fn index_database(graph: &Graph) -> Result<bool, String> {
   
    let index_statements = [
        "CREATE INDEX FOR (u:UUID) ON (u.id)",
        "CREATE INDEX FOR (c:Color) ON (c.value)",
        "CREATE INDEX FOR (t:Temperature) ON (t.value)",
        "CREATE INDEX FOR (h:Humidity) ON (h.value)",
        "CREATE INDEX FOR (ts:Timestamp) ON (ts.value)",
        "CREATE INDEX FOR (ec:EnergyCost) ON (ec.value)",
        "CREATE INDEX FOR (e:EnergyConsume) ON (e.value)"
    ];
    
    for statement in index_statements {
        match graph.execute(query(statement)).await {
            Ok(_) => {
                log::info!("Created index: {}", statement);
            },
            Err(e) => {
                let error_msg = format!("Failed to create index '{}': {}", statement, e);
                error!("{}", error_msg);
                return Err(error_msg);
            }
        }
    }
    
    log::info!("Database successfully INDEXED");
    Ok(true)
}

pub async fn reset_database(graph: &Graph) -> Result<bool, String> {
    
    let delete_query = query(r#"
        MATCH (n) DETACH DELETE n
    "#);

    match graph.execute(delete_query).await {
        Ok(_) => {
            log::info!("All nodes succesfully deleted");
            Ok(true)
        },
        Err(e) => {
            let error_msg = format!("There was a misstake: {}", e);
            error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}

// BIG JSON
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::error::Error;


pub async fn process_large_json_file(graph: &Graph) -> Result<bool, Box<dyn Error>> {
    
    let possible_paths = [
        PathBuf::from("data.json"),                    
        PathBuf::from("./data.json"),                  
        PathBuf::from("../data.json"),                 
        PathBuf::from("src/data.json"),               
        PathBuf::from("datacenter/src/data.json"),    
        PathBuf::from("backend_neo/datacenter/src/data.json"), 
        
    ];
    
   
    let mut file_path = None;
    for path in &possible_paths {
        if path.exists() {
            info!("Found data.json at: {}", path.display());
            file_path = Some(path);
            break;
        }
    }
    
    
    let file_path = file_path.ok_or_else(|| {
        let error_msg = format!(
            "data.json not found in any of the expected locations. Checked paths: {:?}",
            possible_paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>()
        );
        error!("{}", &error_msg);
        error_msg
    })?;

    info!("Opening JSON file at: {}", file_path.display());
    let file = File::open(&file_path)?;
    let file_size = file.metadata()?.len();
    info!("File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1_048_576.0);
    
    
    let buffer_size = 1024 * 1024; 
    let reader = BufReader::with_capacity(buffer_size, file);

   
    let stream = Deserializer::from_reader(reader).into_iter::<Value>();
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut processed_count = 0;

 
    info!("Starting JSON processing...");
    for item in stream {
        processed_count += 1;
        if processed_count % 100 == 0 {
            info!("Processed {} items so far...", processed_count);
        }
        
        match item {
            Ok(data) => {
                match create_new_relation(&data, graph).await {
                    Ok(true) => {
                        success_count += 1;
                        if success_count % 100 == 0 {
                            info!("Successfully processed {} records", success_count);
                        }
                    },
                    Ok(false) => {
                      
                    },
                    Err(e) => {
                        error!("Error processing JSON batch: {}", e);
                        failure_count += 1;
                    }
                }
            },
            Err(e) => {
                error!("Error parsing JSON item #{}: {}", processed_count, e);
                failure_count += 1;
                
                
                if failure_count > 10 && processed_count < 20 {
                    return Err("Too many JSON parsing errors. Check if the file format is correct.".into());
                }
            }
        }
    }

    info!("Finished processing JSON file. Total items: {}, Successes: {}, Failures: {}", 
          processed_count, success_count, failure_count);

    if failure_count == 0 {
        Ok(true)
    } else {
        Err(format!("Encountered {} failures while processing {} items", failure_count, processed_count).into())
    }
}


//Tests

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use neo4rs::{query, Graph};
    use serde_json::json;
    use crate::db;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::fs;
   

   async fn write_to_log(message: &str) {
    let mut file = OpenOptions::new()
        .create(true)  
        .append(true)  
        .open("test_log.txt")
        .expect("Konnte test_log.txt nicht öffnen");

    writeln!(file, "{}", message).expect("Konnte nicht in test_log.txt schreiben");
}

    async fn get_graph() -> Arc<neo4rs::Graph> {
        let db_handler = db::get_database().await.unwrap();
        db_handler.get_primary_db().await
    }

    #[tokio::test]
    async fn test_empty_data() {
        let graph = get_graph().await;
        let data = json!({ "data": [] });
        
        let result = create_new_relation(&data, &graph).await;
        
        assert!(matches!(result, Ok(false)), "Should return Ok(false) for empty array");
    }

    #[tokio::test]
    async fn test_invalid_data() {
        let graph = get_graph().await;
        let data = json!({
            "data": [{
                "color": "red",
                "sensor_data": { "temperature": 25.5, "humidity": 30.0 },
                "timestamp": "2023-10-01T00:00:00Z",
                "energy_consume": 100.0,
                "energy_cost": 50.0
                
            }]
        });
        // false
        let result = create_new_relation(&data, &graph).await;
        assert!(result.is_err(), "Should return Err for invalid data");
    }

    #[tokio::test]
    async fn test_valid_data() {
        let graph = get_graph().await;
        let test_uuid = "test-uuid-validate";
        
       
        let data = json!({
            "data": [{
                "uuid": test_uuid,
                "color": "test-color",
                "sensor_data": {
                    "temperature": 25555.5,
                    "humidity": 3000.0  
                },
                "timestamp": "2100-10-01T00:00:00Z",
                "energy_consume": 10000.0,
                "energy_cost": 50000.0
            }]
        });
        // true
        let result = create_new_relation(&data, &graph).await;
        assert!(matches!(result, Ok(true)), "Should return Ok(true) for valid data");

        cleanup_test_data(&graph, test_uuid).await;
    }

    #[tokio::test]
    async fn test_duplicate_uuid() {
        let graph = get_graph().await;
        let test_uuid = "test-uuid-duplicate";
        
        
        let data = json!({
            "data": [{
                "uuid": test_uuid,
                "color": "test-color",
                "sensor_data": {
                    "temperature": 25555.5,
                    "humidity": 3000.0 
                },
                "timestamp": "2023-10-01T00:00:00Z",
                "energy_consume": 10000.0,
                "energy_cost": 50000.0
            }]
        });
        // true
        let first_result = create_new_relation(&data, &graph).await;
        assert!(matches!(first_result, Ok(true)), "First insertion should return Ok(true)");
        
        // false
        let second_result = create_new_relation(&data, &graph).await;
        assert!(matches!(second_result, Ok(false)), "Second insertion should return Ok(false)");

        cleanup_test_data(&graph, test_uuid).await;
    }

    #[tokio::test]
    async fn test_serialization_error() {
        let graph = get_graph().await;
        
       
        let data = json!({
            "data": [{
                "uuid": "test-uuid",
                "color": "test-color",
               
                "sensor_data": {
                    "temperature": f64::NAN,  
                    "humidity": 30.0
                },
                "timestamp": "2023-10-01T00:00:00Z",
                "energy_consume": 100.0,
                "energy_cost": 50.0
            }]
        });
        // error
        let result = create_new_relation(&data, &graph).await;

        let iserror = result.is_err();

        write_to_log(&format!(
            "Test for serialization error returned: {:?}, success: {:?}",
             result, iserror
        ))
        .await;
        assert!(iserror, "Should return Err for serialization error");
    }

    #[tokio::test]
    async fn test_export_all_with_relationships() {
        let graph = get_graph().await;
        
        let result = export_all_with_relationships(&graph, Some(100)).await;
        
    
        assert!(result.is_some(), "Exportfunktion sollte Some(Value) zurückgeben");

        let json_result = result.unwrap();
        
    
        let output = json_result.to_string();
        fs::write("export_all_data.json", &output).expect("Fehler beim Schreiben der JSON-Datei");
        assert!(!output.is_empty(), "Exportierte Datei sollte nicht leer sein");

        
    }

    #[tokio::test]
    async fn test_get_specific_uuid_node() {
        let graph = get_graph().await;
        let test_uuid = "test-uuid-specific";
    
        let data = json!({
            "data": [{
                "uuid": test_uuid,
                "color": "test-color",
                "sensor_data": {
                    "temperature": 25555.5,
                    "humidity": 3000.0
                },
                "timestamp": "2023-10-01T00:00:00Z",
                "energy_consume": 10000.0,
                "energy_cost": 50000.0
            }]
        });
    
        let _ = create_new_relation(&data, &graph).await;
        let start_time = std::time::Instant::now();
        
        let result = get_specific_uuid_node(test_uuid, &graph).await;
        let elapsed = start_time.elapsed();
        
        let test_succeeded = result.is_some();
        
        write_to_log(&format!(
            "Test for getting a node with uuid: execution time: {:?}, returned: {:?}, success: {}",
            elapsed, result, test_succeeded
        ))
        .await;
    
        assert!(test_succeeded, "Should return Some(Value) for specific UUID");
    
        cleanup_test_data(&graph, test_uuid).await;
    }
    
    #[tokio::test]
    async fn test_get_paginated_uuids() {
        let graph = get_graph().await;
        
      
        let first_page_result = get_paginated_uuids(&graph, 0).await;
        assert!(first_page_result.is_some(), "Should return Some(Value) for first page");
        
        let first_page = first_page_result.unwrap();
        let uuids = first_page["uuids"].as_array().unwrap();
        let total_count = first_page["pagination"]["total_count"].as_u64().unwrap() as usize;
        let total_pages = first_page["pagination"]["total_pages"].as_u64().unwrap() as usize;
        let current_page = first_page["pagination"]["current_page"].as_u64().unwrap() as usize;
        let page_size = first_page["pagination"]["page_size"].as_u64().unwrap() as usize;
        
        assert_eq!(current_page, 0, "Current page should be 0");
        assert_eq!(page_size, 50, "Page size should be 50");
        
       
        write_to_log(&format!(
            "Pagination test - Total count: {}, Total pages: {}, First page UUIDs: {}",
            total_count, total_pages, uuids.len()
        )).await;
        
        
        let expected_first_page_count = std::cmp::min(page_size, total_count);
        assert_eq!(uuids.len(), expected_first_page_count, 
                  "First page should contain the expected number of UUIDs");
        
       
        if total_pages > 1 {
            let last_page_index = total_pages - 1;
            let last_page_result = get_paginated_uuids(&graph, last_page_index).await;
            assert!(last_page_result.is_some(), "Should return Some(Value) for last page");
            
            let last_page = last_page_result.unwrap();
            let last_page_uuids = last_page["uuids"].as_array().unwrap();
            let last_page_current = last_page["pagination"]["current_page"].as_u64().unwrap() as usize;
            
            assert_eq!(last_page_current, last_page_index, "Current page should be the last page index");
            
        
            write_to_log(&format!(
                "Pagination test - Last page ({}): UUIDs count: {}",
                last_page_index, last_page_uuids.len()
            )).await;
          
            let expected_last_page_count = if total_count % page_size == 0 {
                page_size
            } else {
                total_count % page_size
            };
            
            assert_eq!(last_page_uuids.len(), expected_last_page_count,
                      "Last page should contain the expected number of UUIDs");
        }
    }
    





    async fn cleanup_test_data(graph: &Graph, uuid: &str) {
        let q = query("MATCH (u:UUID {id: $uuid}) DETACH DELETE u")
            .param("uuid", uuid);
        let _ = graph.run(q).await;
 
        let q = query("MATCH (c:Color {value: $value}) DETACH DELETE c")
            .param("value", "test-color");
        let _ = graph.run(q).await;

        let q = query("MATCH (t:Temperature {value: $value}) DETACH DELETE t")
            .param("value", 25555.5);
        let _ = graph.run(q).await;
    
        let q = query("MATCH (h:Humidity {value: $value}) DETACH DELETE h")
            .param("value", 3000.0);
        let _ = graph.run(q).await;
    
        let q = query("MATCH (ts:Timestamp {value: $value}) DETACH DELETE ts")
            .param("value", "2023-10-01T00:00:00Z");
        let _ = graph.run(q).await;
        
        let q = query("MATCH (ts:Timestamp {value: $value}) DETACH DELETE ts")
            .param("value", "2100-10-01T00:00:00Z");
        let _ = graph.run(q).await;
    
        let q = query("MATCH (ec:EnergyCost {value: $value}) DETACH DELETE ec")
            .param("value", 50000.0);
        let _ = graph.run(q).await;
    
        let q = query("MATCH (eco:EnergyConsume {value: $value}) DETACH DELETE eco")
            .param("value", 10000.0);
        let _ = graph.run(q).await;
    }
}