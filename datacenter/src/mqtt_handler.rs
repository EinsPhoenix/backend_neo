use rumqttc::{MqttOptions, AsyncClient, Event, Incoming, QoS, Transport};
use tokio::time::{Duration, Instant};
use log::{info, error, warn};
use serde_json::{Value, Result as JsonResult};
use std::env;
use std::error::Error;
use uuid::Uuid;
use crate::query::get_specific_uuid_node;
use crate::db::get_db;
use neo4rs::Graph;

pub async fn start_mqtt_client() -> Result<(), Box<dyn Error>> {
    // Generate a unique client ID for this connection
    let client_id = format!("rust-mqtt-client-{}", Uuid::new_v4());
    
    let mut mqtt_options = MqttOptions::new(
        &client_id,
        "mosquitto-broker",
        1883
    );
    
    mqtt_options.set_credentials(
        env::var("MQTT_USER").unwrap_or_else(|_| "admin".into()),
        env::var("MQTT_PASSWORD").unwrap_or_else(|_| "admin".into())
    );
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    
    // Verbindungs-Timeout
    let connect_timeout = Duration::from_secs(10);
    let start_time = Instant::now();
    let mut connected = false;
    // Phase 1: Verbindungsherstellung 
    while Instant::now().duration_since(start_time) < connect_timeout {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                info!("✅ Broker-Verbindung hergestellt: {:?}, Client-ID: {}", ack, client_id);
                connected = true;
                break;
            },
            Ok(event) => warn!("Zwischenereignis: {:?}", event),
            Err(e) => {
                error!("❌ Verbindungsfehler: {}", e);
                return Err(e.into());
            }
        }
    }
    if !connected {
        error!("⌛ Timeout: Broker nicht erreichbar!");
        return Err("Broker offline".into());
    }
    // Phase 2: Normalbetrieb
    info!("🚀 Starte normalen Betrieb... Client-ID: {}", client_id);
    
    // Publish our client ID to a central topic so other clients know we exist
    let connection_message = serde_json::json!({
        "type": "client_connect",
        "client_id": client_id
    });
    
    client.publish(
        "rust/clients", 
        QoS::AtLeastOnce, 
        false, 
        serde_json::to_vec(&connection_message)?
    ).await?;
    
    // Subscribe to the general topic
    client.subscribe("rust/topic", QoS::AtMostOnce).await?;
    
    // Also subscribe to our client-specific topic
    let client_topic = format!("rust/topic/{}", client_id);
    client.subscribe(&client_topic, QoS::AtMostOnce).await?;
    
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                let db = match get_db().await {
                    Ok(db) => db,
                    Err(e) => {
                        error!("Database connection failed: {}", e);
                        continue;
                    },
                };
                handle_message(&publish, &client, &db, &client_id).await?;
            },
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                info!("🔌 Verbindung getrennt für Client-ID: {}", client_id);
                break;
            },
            Err(e) => {
                error!("⚠️ Fehler im Eventloop: {}, Client-ID: {}", e, client_id);
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn handle_message(
    publish: &rumqttc::Publish,
    client: &AsyncClient,
    db: &Graph,
    client_id: &str
) -> Result<(), Box<dyn Error>> {
    info!("📨 Nachricht empfangen: {:?} für Client-ID: {}", publish.payload, client_id);
    let payload_str = std::str::from_utf8(&publish.payload)?;
    
    // Check if this message is for us specifically
    let is_client_specific = publish.topic.contains(client_id);
    
    // Parse the message
    parse_and_process_json(payload_str, &db, client, client_id, is_client_specific).await?;
    
    Ok(())
}

async fn parse_and_process_json(
    json_str: &str, 
    db: &Graph,
    client: &AsyncClient,
    client_id: &str,
    is_client_specific: bool
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed: serde_json::Result<Value> = serde_json::from_str(json_str);
    
    match parsed {
        Ok(json_value) => {
            // Check if the message has a target client_id
            let target_client_id = json_value.get("client_id").and_then(Value::as_str);
            
            // Only process if:
            // 1. No specific target is specified, OR
            // 2. We are the specific target, OR
            // 3. Message came in on our specific topic (is_client_specific)
            if target_client_id.is_none() || target_client_id == Some(client_id) || is_client_specific {
                match json_value.get("type").and_then(Value::as_str) {
                    Some("uuid") => {
                        // Process UUID type
                        if let Some(data) = json_value.get("data").and_then(Value::as_str) {
                            info!("Processing UUID: {} for Client-ID: {}", data, client_id);
                            match get_specific_uuid_node(data, &db).await {
                                Some(node) => {
                                    info!("Found node for UUID {}: {:?}", data, node);
                                    // Use client-specific response topic
                                    let response_topic = format!("rust/response/{}/{}", client_id, data);
                                    publish_result(client, &response_topic, &node).await?;
                                },
                                None => {
                                    info!("No node found for UUID: {}", data);
                                    let response_topic = format!("rust/response/{}/{}", client_id, data);
                                    let empty_response = serde_json::json!({
                                        "uuid": data,
                                        "found": false,
                                        "message": "No data found for this UUID"
                                    });
                                    publish_result(client, &response_topic, &empty_response).await?;
                                }
                            }
                        } else {
                            error!("Missing or invalid 'data' field for type 'uuid'. Input: {}", json_str);
                        }
                    },
                    Some("all") => {
                        info!("Processing 'all' request for Client-ID: {}", client_id);
                        match crate::query::get_all_uuid_nodes(&db).await {
                            Some(all_nodes) => {
                                let response_topic = format!("rust/response/{}/all", client_id);
                                publish_result(client, &response_topic, &all_nodes).await?;
                            },
                            None => {
                                error!("Failed to get all UUID nodes for Client-ID: {}", client_id);
                            }
                        }
                    },
                    Some("color") => {
                        info!("Processing 'color' data for Client-ID: {}", client_id);
                        if let Some(color_data) = json_value.get("data").and_then(Value::as_str) {
                            match crate::query::get_nodes_with_color(color_data, &db).await {
                                Some(processed) => {
                                    let response_topic = format!("rust/response/{}/color", client_id);
                                    publish_result(client, &response_topic, &processed).await?;
                                },
                                None => {
                                    error!("Failed to get nodes with color: {} for Client-ID: {}", color_data, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'data' field for type 'color'. Input: {}", json_str);
                        }
                    },
                    Some("time_range") => {
                        info!("Processing 'time_range' data for Client-ID: {}", client_id);
                        let start = json_value.get("start").and_then(Value::as_str);
                        let end = json_value.get("end").and_then(Value::as_str);
                        
                        if let (Some(start_time), Some(end_time)) = (start, end) {
                            match crate::query::get_nodes_in_time_range(start_time, end_time, &db).await {
                                Some(nodes) => {
                                    let response_topic = format!("rust/response/{}/time_range", client_id);
                                    publish_result(client, &response_topic, &nodes).await?;
                                },
                                None => {
                                    error!("Failed to get nodes in time range from {} to {} for Client-ID: {}", 
                                           start_time, end_time, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'start' or 'end' fields for type 'time_range'. Input: {}", json_str);
                        }
                    },
                    Some("temperature_humidity") => {
                        info!("Processing 'temperature_humidity' data for Client-ID: {}", client_id);
                        let temp = json_value.get("temperature").and_then(Value::as_f64);
                        let humidity = json_value.get("humidity").and_then(Value::as_f64);
                        
                        if let (Some(temp_val), Some(humidity_val)) = (temp, humidity) {
                            match crate::query::get_nodes_with_temperature_or_humidity(temp_val, humidity_val, &db).await {
                                Some(nodes) => {
                                    let response_topic = format!("rust/response/{}/temperature_humidity", client_id);
                                    publish_result(client, &response_topic, &nodes).await?;
                                },
                                None => {
                                    error!("Failed to get nodes with temperature {} and humidity {} for Client-ID: {}", 
                                           temp_val, humidity_val, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'temperature' or 'humidity' fields for type 'temperature_humidity'. Input: {}", json_str);
                        }
                    },
                    Some("timestamp") => {
                        info!("Processing 'timestamp' data for Client-ID: {}", client_id);
                        if let Some(timestamp) = json_value.get("data").and_then(Value::as_str) {
                            match crate::query::get_temperature_humidity_at_time(&db, timestamp).await {
                                Some((temp, humidity)) => {
                                    let response = serde_json::json!({
                                        "timestamp": timestamp,
                                        "temperature": temp,
                                        "humidity": humidity
                                    });
                                    let response_topic = format!("rust/response/{}/timestamp", client_id);
                                    publish_result(client, &response_topic, &response).await?;
                                },
                                None => {
                                    error!("Failed to get temperature and humidity at timestamp: {} for Client-ID: {}", 
                                           timestamp, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'data' field for type 'timestamp'. Input: {}", json_str);
                        }
                    },
                    Some("energy_cost") => {
                        info!("Processing 'energy_cost' data for Client-ID: {}", client_id);
                        if let Some(cost) = json_value.get("data").and_then(Value::as_f64) {
                            match crate::query::get_nodes_with_energy_cost(cost, &db).await {
                                Some(nodes) => {
                                    let response_topic = format!("rust/response/{}/energy_cost", client_id);
                                    publish_result(client, &response_topic, &nodes).await?;
                                },
                                None => {
                                    error!("Failed to get nodes with energy cost: {} for Client-ID: {}", cost, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'data' field for type 'energy_cost'. Input: {}", json_str);
                        }
                    },
                    Some("energy_consume") => {
                        info!("Processing 'energy_consume' data for Client-ID: {}", client_id);
                        if let Some(consume) = json_value.get("data").and_then(Value::as_f64) {
                            match crate::query::get_nodes_with_energy_consume(consume, &db).await {
                                Some(nodes) => {
                                    let response_topic = format!("rust/response/{}/energy_consume", client_id);
                                    publish_result(client, &response_topic, &nodes).await?;
                                },
                                None => {
                                    error!("Failed to get nodes with energy consumption: {} for Client-ID: {}", consume, client_id);
                                }
                            }
                        } else {
                            error!("Missing or invalid 'data' field for type 'energy_consume'. Input: {}", json_str);
                        }
                    },
                    Some(other) => {
                        error!("Unknown type '{}' in JSON for Client-ID: {}. Full input: {}", other, client_id, json_str);
                    },
                    None => {
                        error!("Missing 'type' field in JSON for Client-ID: {}: {}", client_id, json_str);
                    }
                }
            } else {
                // Message is for another client, we ignore it
                info!("Skipping message intended for another client: {}", target_client_id.unwrap_or("unknown"));
            }
        },
        Err(e) => {
            error!("Failed to parse JSON for Client-ID: {}: {}", client_id, e);
        }
    }
    Ok(())
}

async fn publish_result(client: &AsyncClient, topic: &str, data: &Value) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_PACKET_SIZE: usize = 5000; // Safe limit below broker's 10240 bytes
    
    let response_json = serde_json::to_vec(data)?;
    
    if response_json.len() <= MAX_PACKET_SIZE {
        // Normal publishing for messages within size limit
        client.publish(topic, QoS::AtMostOnce, false, response_json).await?;
        info!("Result published to topic: {}", topic);
    } else {
        // Split large messages into chunks
        info!("Large message detected ({} bytes). Splitting into chunks...", response_json.len());
        
        // Convert the original Value to a mutable JSON object to add split information
        let mut data_map = if let Value::Object(map) = data.clone() {
            map
        } else {
            // If not an object, wrap it in one
            let mut map = serde_json::Map::new();
            map.insert("data".to_string(), data.clone());
            map
        };
        
        // Calculate number of chunks needed
        let total_chunks = (response_json.len() + MAX_PACKET_SIZE - 1) / MAX_PACKET_SIZE;
        let json_str = serde_json::to_string(data)?;
        
        // Split the JSON string into chunks
        for chunk_index in 0..total_chunks {
            let start = chunk_index * MAX_PACKET_SIZE;
            let end = std::cmp::min(start + MAX_PACKET_SIZE, json_str.len());
            let chunk = &json_str[start..end];
            
            // Create a new JSON object for each chunk
            let mut chunk_data = serde_json::Map::new();
            
            // Add split metadata
            chunk_data.insert("split_index".to_string(), Value::Number(serde_json::Number::from(chunk_index + 1)));
            chunk_data.insert("total_splits".to_string(), Value::Number(serde_json::Number::from(total_chunks)));
            chunk_data.insert("original_size".to_string(), Value::Number(serde_json::Number::from(json_str.len())));
            
            // Add the data chunk
            chunk_data.insert("chunk".to_string(), Value::String(chunk.to_string()));
            
            // Convert to JSON and publish
            let chunk_json = Value::Object(chunk_data);
            let chunk_bytes = serde_json::to_vec(&chunk_json)?;
            
            // Construct split-specific topic
            let split_topic = format!("{}/split/{}/{}", topic, chunk_index + 1, total_chunks);
            
            client.publish(&split_topic, QoS::AtLeastOnce, false, chunk_bytes).await?;
            info!("Published chunk {}/{} to topic: {}", chunk_index + 1, total_chunks, split_topic);
        }
        
        // Publish a summary message to the original topic
        let mut summary = serde_json::Map::new();
        summary.insert("message_split".to_string(), Value::Bool(true));
        summary.insert("total_chunks".to_string(), Value::Number(serde_json::Number::from(total_chunks)));
        summary.insert("original_size".to_string(), Value::Number(serde_json::Number::from(json_str.len())));
        summary.insert("base_topic".to_string(), Value::String(topic.to_string()));
        
        let summary_json = Value::Object(summary);
        let summary_bytes = serde_json::to_vec(&summary_json)?;
        
        client.publish(topic, QoS::AtLeastOnce, false, summary_bytes).await?;
        info!("Published split summary to topic: {}", topic);
    }
    
    Ok(())
}