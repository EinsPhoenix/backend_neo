use neo4rs::{Graph, query};
use log::error;
use serde_json::Value;
use std::collections::HashMap;
use serde_json::json;

pub async fn create_new_relation(data: &Value, graph: &Graph) -> bool {
    
    if let Some(data_array) = data.get("data").and_then(|d| d.as_array()) {
        if data_array.is_empty() {
            return false;
        }
        
        let mut neo4j_data = Vec::new();
        
        for item in data_array {
            let mut record = HashMap::<String, String>::new();

            
            if let Some(uuid) = item.get("uuid").and_then(|v| v.as_str()) {
                record.insert("uuid".to_string(), uuid.to_string());
            }

            if let Some(color) = item.get("color").and_then(|v| v.as_str()) {
                record.insert("color".to_string(), color.to_string());
            }

            if let Some(timestamp) = item.get("timestamp").and_then(|v| v.as_str()) {
                record.insert("timestamp".to_string(), timestamp.to_string());
            }

          
            if let Some(energy_consume) = item.get("energy_consume").and_then(|v| v.as_f64()) {
                let energy_consume_str = energy_consume.to_string();
                record.insert("energy_consume".to_string(), energy_consume_str);
            }

            if let Some(energy_cost) = item.get("energy_cost").and_then(|v| v.as_f64()) {
                let energy_cost_str = energy_cost.to_string();
                record.insert("energy_cost".to_string(), energy_cost_str);
            }

           
            if let Some(sensor_data) = item.get("sensor_data").and_then(|v| v.as_object()) {
                if let Some(temp) = sensor_data.get("temperature").and_then(|v| v.as_f64()) {
                    let temp_str = temp.to_string();
                    record.insert("sensor_data.temperature".to_string(), temp_str);
                }
                
                if let Some(humidity) = sensor_data.get("humidity").and_then(|v| v.as_f64()) {
                    let humidity_str = humidity.to_string();
                    record.insert("sensor_data.humidity".to_string(), humidity_str);
                }
            }
            
            neo4j_data.push(record);
        }
        
        let creation_query = query(r#"
        UNWIND $data AS record
        
        OPTIONAL MATCH (existingUuid:UUID {id: record.uuid})
        WITH record, existingUuid
        WHERE existingUuid IS NULL
        
        MERGE (uuid:UUID {id: record.uuid})
        SET uuid.energy_consume = toFloat(record.energy_consume),
            uuid.energy_cost = toFloat(record.energy_cost)
        
        MERGE (color:Color {value: record.color})
        MERGE (uuid)-[:HAS_COLOR]->(color)
        
        MERGE (temperature:Temperature {value: toFloat(record.`sensor_data.temperature`)})
        MERGE (uuid)-[:HAS_TEMPERATURE]->(temperature)
        
        MERGE (humidity:Humidity {value: toFloat(record.`sensor_data.humidity`)})
        MERGE (uuid)-[:HAS_HUMIDITY]->(humidity)
        
        MERGE (timestamp:Timestamp {value: record.timestamp})
        MERGE (uuid)-[:HAS_TIMESTAMP]->(timestamp)
        
        MERGE (timestamp)-[:SENSOR_DATA]->(temperature)
        MERGE (timestamp)-[:SENSOR_DATA]->(humidity)
        
        MERGE (energyCost:EnergyCost {value: toFloat(record.energy_cost)})
        MERGE (uuid)-[:HAS_ENERGYCOST]->(energyCost)
        MERGE (timestamp)-[:HAS_PRICE]->(energyCost)
        
        MERGE (energyConsume:EnergyConsume {value: toFloat(record.energy_consume)})
        MERGE (uuid)-[:HAS_ENERGYCONSUME]->(energyConsume)
        
        WITH uuid
        RETURN uuid.id AS processed_uuid
        "#)
        .param("data", neo4j_data);

        match graph.execute(creation_query).await {
            Ok(mut result) => {
                match result.next().await {
                    Ok(Some(_)) => true,
                    _ => {
                        error!("No new nodes were created (UUIDs might already exist)");
                        false
                    }
                }
            },
            Err(e) => {
                error!("Failed to execute Neo4j query: {}", e);
                false
            }
        }
    } else {
        error!("Invalid JSON structure: 'data' array not found");
        false
    }
}



pub async fn get_specific_uuid_node(uuid: &str, graph: &Graph) -> Option<Value> {
    let query = query(r#"
        MATCH (uuid:UUID {id: $uuid})
        RETURN uuid
    "#)
    .param("uuid", uuid);

    match graph.execute(query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let node: Value = row.get("uuid").unwrap();
                Some(node)
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
        MATCH (uuid:UUID)
        RETURN uuid
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
            error!("Fehler beim Abrufen der Sensordaten: {}", e);
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