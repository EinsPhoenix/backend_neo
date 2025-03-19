use neo4rs::{Graph, query};
use log::{error, info, warn};
use serde_json::{json, Value};
use std::collections::HashMap;


//validate function

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
            uuid.energy_cost = record.energy_cost
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
    let query = query(r#"
        MATCH (uuidNode:UUID {id: $uuid})
        OPTIONAL MATCH (uuidNode)-[:HAS_COLOR]->(color:Color)
        OPTIONAL MATCH (uuidNode)-[:HAS_TIMESTAMP]->(timestamp:Timestamp)
        OPTIONAL MATCH (uuidNode)-[:HAS_TEMPERATURE]->(temp:Temperature)
        OPTIONAL MATCH (uuidNode)-[:HAS_HUMIDITY]->(humidity:Humidity)
        WITH uuidNode, color, timestamp, temp, humidity
        ORDER BY timestamp.value DESC
        LIMIT 1
        RETURN uuidNode.id AS uuid,
               color.value AS color,
               { temperature: temp.value, humidity: humidity.value } AS sensor_data,
               timestamp.value AS timestamp,
               uuidNode.energy_consume AS energy_consume,
               uuidNode.energy_cost AS energy_cost

    "#)
    .param("uuid", uuid);

    match graph.execute(query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                
                let uuid_val: String = row.get("uuid").unwrap_or_default();
                let color_val: String = row.get("color").unwrap_or_default();
                let sensor_data: Value = row.get("sensor_data").unwrap_or(json!({}));
                let timestamp_val: String = row.get("timestamp").unwrap_or_default();
                let energy_consume: f64 = row.get("energy_consume").unwrap_or(0.0);
                let energy_cost: f64 = row.get("energy_cost").unwrap_or(0.0);

                Some(json!({
                    "uuid": uuid_val,
                    "color": color_val,
                    "sensor_data": {
                        "temperature": sensor_data["temperature"].as_f64().unwrap_or(0.0),
                        "humidity": sensor_data["humidity"].as_f64().unwrap_or(0.0)
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
        OPTIONAL MATCH (uuidNode)-[:HAS_COLOR]->(color:Color)
        OPTIONAL MATCH (uuidNode)-[:HAS_TIMESTAMP]->(timestamp:Timestamp)
        OPTIONAL MATCH (uuidNode)-[:HAS_TEMPERATURE]->(temp:Temperature)
        OPTIONAL MATCH (uuidNode)-[:HAS_HUMIDITY]->(humidity:Humidity)

        WITH uuidNode, color, timestamp, temp, humidity
        ORDER BY timestamp.value DESC

        WITH uuidNode, 
            color.value AS color, 
            timestamp.value AS latest_timestamp, 
            temp.value AS temperature, 
            humidity.value AS humidity

        RETURN uuidNode.id AS uuid,
            color,
            { temperature: temperature, humidity: humidity } AS sensor_data,
            latest_timestamp AS timestamp,
            uuidNode.energy_consume AS energy_consume,
            uuidNode.energy_cost AS energy_cost
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
        WITH collect(p) AS paths
        RETURN apoc.convert.toJson(paths) AS json_result
        {};
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




//Tests

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread::sleep};

    use super::*;
    use neo4rs::{query, Graph};
    use serde_json::json;
    use crate::db;
    use std::fs;

    use tokio::time::{Duration};

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
        assert!(result.is_err(), "Should return Err for serialization error");
    }

    #[tokio::test]
    async fn test_export_all_with_relationships() {
        let graph = get_graph().await;
        
        let result = export_all_with_relationships(&graph, Some(200000)).await;
        
    
        assert!(result.is_some(), "Exportfunktion sollte Some(Value) zurückgeben");

        let json_result = result.unwrap();
        
    
        let output = json_result.to_string();
        fs::write("export_all_data.json", &output).expect("Fehler beim Schreiben der JSON-Datei");
        assert!(!output.is_empty(), "Exportierte Datei sollte nicht leer sein");

        
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