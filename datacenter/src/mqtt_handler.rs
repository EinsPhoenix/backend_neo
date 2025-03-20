use rumqttc::{MqttOptions, AsyncClient, Event, Incoming, QoS};
use tokio::time::{Duration, Instant};
use log::{info, error, warn};
use serde_json::{Value, json};
use std::env;
use std::error::Error;
use uuid::Uuid;
use crate::query::{export_all_with_relationships,create_new_relation,get_newest_uuid,get_specific_uuid_node,get_all_uuid_nodes,get_nodes_with_color,get_nodes_in_time_range,get_nodes_with_temperature_or_humidity,get_temperature_humidity_at_time,get_nodes_with_energy_cost,get_nodes_with_energy_consume};
use crate::db;



use tokio::time; 
use chrono;

use std::sync::Arc;

// Broker can only 10200
const MAX_MESSAGE_SIZE: usize = 8000;

async fn publish_paginated_results(client: &AsyncClient, topic: &str, payload: &Value) -> Result<(), Box<dyn Error>> {
    let payload_str = serde_json::to_string(payload)?;
    let payload_size = payload_str.len();
    
    
    if payload_size <= MAX_MESSAGE_SIZE {
        info!("Publishing to {}: {} bytes", topic, payload_size);
        client.publish(topic, QoS::AtLeastOnce, false, payload_str).await?;
        return Ok(());
    }
    
   
    info!("Large message detected ({} bytes), using pagination", payload_size);

    
    // Request ID for processing
    let request_id = Uuid::new_v4().to_string();
    
    
    let array_payload = match payload {
        Value::Array(items) => items.clone(),
        _ => {
           
            vec![payload.clone()]
        }
    };
    
    let mut current_page = 1;
    let mut current_chunk = Vec::new();
    let mut current_size = 0;
    
    // Calculate size of Metadata
    let metadata_template = json!({
        "type": "paginated",
        "request_id": request_id,
        "page": 999,
        "total_pages": 999,
        "data": []
    });
    let metadata_overhead = serde_json::to_string(&metadata_template)?.len() - 2; 
    let effective_max_size = MAX_MESSAGE_SIZE - metadata_overhead - 50; 
    
    // estemate of pages
    let total_items = array_payload.len();
    let estimated_total_pages = (payload_size / effective_max_size) + 1;
    
    let mut total_pages = 0;
    
    for item in array_payload {
        let item_json = serde_json::to_string(&item)?;
        let item_size = item_json.len();
        
       
        if !current_chunk.is_empty() && current_size + item_size + 1 > effective_max_size {
            
            let page_payload = json!({
                "type": "paginated",
                "request_id": request_id,
                "page": current_page,
                "total_pages": estimated_total_pages, 
                "data": current_chunk
            });
            
            let page_topic = format!("{}/page/{}", topic, current_page);
            let page_json = serde_json::to_string(&page_payload)?;
            
            info!("Publishing page {}/{} to {} ({} bytes)", 
                  current_page, estimated_total_pages, page_topic, page_json.len());
            client.publish(&page_topic, QoS::AtLeastOnce, false, page_json).await?;
            
           
            current_page += 1;
            total_pages = current_page;
            current_chunk = Vec::new();
            current_size = 0;
        }
        
        
        current_chunk.push(item);
        current_size += item_size + 1; 
    }
    
    // last page when not emty
    if !current_chunk.is_empty() {
        let page_payload = json!({
            "type": "paginated",
            "request_id": request_id,
            "page": current_page,
            "total_pages": current_page, 
            "data": current_chunk
        });
        
        let page_topic = format!("{}/page/{}", topic, current_page);
        let page_json = serde_json::to_string(&page_payload)?;
        
        info!("Publishing final page {}/{} to {} ({} bytes)", 
              current_page, current_page, page_topic, page_json.len());
        client.publish(&page_topic, QoS::AtLeastOnce, false, page_json).await?;
        
        total_pages = current_page;
    }
    
    // Publish Summary
    let summary_payload = json!({
        "type": "summary",
        "request_id": request_id,
        "total_items": total_items,
        "total_pages": total_pages,
        "original_size": payload_size,
        "topic_base": topic
    });
    
    let summary_topic = format!("{}/summary", topic);
    client.publish(&summary_topic, QoS::AtLeastOnce, false, serde_json::to_string(&summary_payload)?).await?;
    info!("Published summary for request {}: {} total items across {} pages", 
          request_id, total_items, total_pages);
    
    Ok(())
}

async fn publish_result(client: &AsyncClient, topic: &str, payload: &Value) -> Result<(), Box<dyn Error>> {
    let payload_str = serde_json::to_string(payload)?;
    let payload_size = payload_str.len();
    
    // info!("Publishing to {}: {} bytes, the response was: {}", topic, payload_size, payload_str);
    if payload_size <= MAX_MESSAGE_SIZE {
       
        info!("Publishing to {}: {} bytes", topic, payload_size);
        client.publish(topic, QoS::AtLeastOnce, false, payload_str).await?;
    } else {
       
        info!("Message too large for direct publish ({} bytes), using pagination", payload_size);
        publish_paginated_results(client, topic, payload).await?;
    }
    
    Ok(())
}


async fn process_request(client: &AsyncClient, payload: &[u8], db_handler: &Arc<db::DatabaseCluster>) -> Result<(), Box<dyn Error>> {
    let payload_str = String::from_utf8_lossy(payload);
    info!("Received request: {}", payload_str);
    
    
    let json_value: Value = match serde_json::from_str(&payload_str) {
        Ok(value) => value,
        Err(e) => {
            error!("Failed to parse JSON: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // extract Clientid
    let requesting_client_id = match json_value.get("client_id").and_then(Value::as_str) {
        Some(id) => id,
        None => {
            error!("Missing client_id in request");
           
            return Err("Missing client_id".into());
        }
    };
    let read_conn_1 = db_handler.get_read_db(0).await;
    let read_conn_2 = db_handler.get_read_db(1).await;
    let write_conn = db_handler.get_primary_db().await;
    // Request-Typ 
    match json_value.get("request").and_then(Value::as_str) {
        
        Some("uuid") => {
            info!("Processing UUID request for client: {}", requesting_client_id);
            
           // Process the request
            if let Some(payload) = json_value.get("payload") {
                if let Some(uuid_array) = payload.as_array() {
                    for uuid_obj in uuid_array {
                        if let Some(uuid) = uuid_obj.get("uuid").and_then(Value::as_str) {
                            info!("Processing UUID: {} for Client-ID: {}", uuid, requesting_client_id);
                            
                         
                            let response_topic = format!("rust/uuid/{}", requesting_client_id);
                            
                            match get_specific_uuid_node(uuid, &read_conn_1).await {
    
                                Some(node) => {
                                    info!("Found node for UUID {}: {:?}", uuid, node);
                                    publish_result(client, &response_topic, &node).await?;
                                   
                                },
                                None => {
                                    error!("No node found for UUID: {}", uuid);
                                    let empty_response = json!({
                                        "uuid": uuid,
                                        "found": false,
                                        "message": "No data found for this UUID"
                                    });
                                    
                                    publish_result(client, &response_topic, &empty_response).await?;
                                }
                            }
                        }
                    }
                } else {
                    error!("Invalid payload format, expected array");
                    return Err("Invalid payload format".into());
                }
            } else {
           
                warn!("No payload provided for UUID request");
            }
        },
        Some("all") => {
            info!("Processing 'all' request for Client-ID: {}", requesting_client_id);
            
            match get_all_uuid_nodes(&read_conn_2).await {
                Some(all_nodes) => {
                    let response_topic = format!("rust/response/{}/all", requesting_client_id);
                    publish_result(client, &response_topic, &all_nodes).await?;
                },
                None => {
                    error!("Failed to get all UUID nodes for Client-ID: {}", requesting_client_id);
                    let response_topic = format!("rust/response/{}/all", requesting_client_id);
                            let response = json!({
                                      "status": "error",
                                      "message": format!("Failed to get all UUID nodes")
                                  });
                            publish_result(client, &response_topic, &response).await?;
                }
            }
        },
        Some("color") => {
            info!("Processing 'color' data for Client-ID: {}", requesting_client_id);
            if let Some(color_data) = json_value.get("data").and_then(Value::as_str) {
                
                match get_nodes_with_color(color_data, &read_conn_2).await {
                    Some(processed) => {
                        let response_topic = format!("rust/response/{}/color", requesting_client_id);
                        publish_result(client, &response_topic, &processed).await?;
                    },
                    None => {
                        error!("Failed to get nodes with color: {} for Client-ID: {}", color_data, requesting_client_id);
                        let response_topic = format!("rust/response/{}/color", requesting_client_id);
                            let response = json!({
                                      "status": "error",
                                      "message": format!("Failed to get nodes with color")
                                  });
                            publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'data' field for type 'color'. Input: {}", payload_str);
            }
        },
        Some("time_range") => {
            info!("Processing 'time_range' data for Client-ID: {}", requesting_client_id);
            let start = json_value.get("start").and_then(Value::as_str);
            let end = json_value.get("end").and_then(Value::as_str);
            
            if let (Some(start_time), Some(end_time)) = (start, end) {
                match get_nodes_in_time_range(start_time, end_time, &read_conn_1).await {
                    Some(nodes) => {
                        let response_topic = format!("rust/response/{}/time_range", requesting_client_id);
                        publish_result(client, &response_topic, &nodes).await?;
                    },
                    None => {
                        error!("Failed to get nodes in time range from {} to {} for Client-ID: {}", 
                               start_time, end_time, requesting_client_id);
                            let response_topic = format!("rust/response/{}/time_range", requesting_client_id);
                            let response = json!({
                                      "status": "error",
                                      "message": format!("Failed to get nodes in time range")
                                  });
                            publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'start' or 'end' fields for type 'time_range'. Input: {}", payload_str);
            }
        },
        Some("temperature_humidity") => {
            info!("Processing 'temperature_humidity' data for Client-ID: {}", requesting_client_id);
            let temp = json_value.get("temperature").and_then(Value::as_f64);
            let humidity = json_value.get("humidity").and_then(Value::as_f64);
            
            if let (Some(temp_val), Some(humidity_val)) = (temp, humidity) {
                match get_nodes_with_temperature_or_humidity(temp_val, humidity_val, &read_conn_1).await {
                    Some(nodes) => {
                        let response_topic = format!("rust/response/{}/temperature_humidity", requesting_client_id);
                        publish_result(client, &response_topic, &nodes).await?;
                    },
                    None => {
                        error!("Failed to get nodes with temperature {} and humidity {} for Client-ID: {}", 
                               temp_val, humidity_val, requesting_client_id);
                            let response_topic = format!("rust/response/{}/temperature_humidity", requesting_client_id);
                            let response = json!({
                                      "status": "error",
                                      "message": format!("Failed to get nodes with temperature and humidity")
                                  });
                            publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'temperature' or 'humidity' fields for type 'temperature_humidity'. Input: {}", payload_str);
            }
        },
        Some("timestamp") => {
            info!("Processing 'timestamp' data for Client-ID: {}", requesting_client_id);
            if let Some(timestamp) = json_value.get("data").and_then(Value::as_str) {
                match get_temperature_humidity_at_time(&read_conn_1, timestamp).await {
                    Some((temp, humidity)) => {
                        let response = json!({
                            "timestamp": timestamp,
                            "temperature": temp,
                            "humidity": humidity
                        });
                        let response_topic = format!("rust/response/{}/timestamp", requesting_client_id);
                        publish_result(client, &response_topic, &response).await?;
                    },
                    None => {
                        error!("Failed to get temperature and humidity at timestamp: {} for Client-ID: {}", 
                               timestamp, requesting_client_id);
                            let response_topic = format!("rust/response/{}/timestamp", requesting_client_id);
                            let response = json!({
                                   "status": "error",
                                   "message": format!("Failed to get temperature and humidity at timestamp")
                               });
                            publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'data' field for type 'timestamp'. Input: {}", payload_str);
            }
        },
        Some("energy_cost") => {
            info!("Processing 'energy_cost' data for Client-ID: {}", requesting_client_id);
            if let Some(cost) = json_value.get("data").and_then(Value::as_f64) {
                match get_nodes_with_energy_cost(cost, &read_conn_2).await {
                    Some(nodes) => {
                        let response_topic = format!("rust/response/{}/energy_cost", requesting_client_id);
                        publish_result(client, &response_topic, &nodes).await?;
                    },
                    None => {
                        error!("Failed to get nodes with energy cost: {} for Client-ID: {}", cost, requesting_client_id);
                        let response_topic = format!("rust/response/{}/energy_cost", requesting_client_id);
                        let response = json!({
                            "status": "error",
                            "message": format!("Failed to get nodes with energy cost")
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'data' field for type 'energy_cost'. Input: {}", payload_str);
            }
        },
        Some("energy_consume") => {
            info!("Processing 'energy_consume' data for Client-ID: {}", requesting_client_id);
            if let Some(consume) = json_value.get("data").and_then(Value::as_f64) {
                match get_nodes_with_energy_consume(consume, &read_conn_2).await {
                    Some(nodes) => {
                        let response_topic = format!("rust/response/{}/energy_consume", requesting_client_id);
                        publish_result(client, &response_topic, &nodes).await?;
                    },
                    None => {
                        error!("Failed to get nodes with energy consumption: {} for Client-ID: {}", consume, requesting_client_id);

                        let response_topic = format!("rust/response/{}/energy_consume", requesting_client_id);
                        let response = json!({
                            "status": "error",
                            "message": format!("Failed to get nodes with energy consumption")
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'data' field for type 'energy_consume'. Input: {}", payload_str);
            }
        },
        Some("newest") => {
            

                match get_newest_uuid(&read_conn_1).await {
                    Some(nodes) => {
                        let response_topic = format!("rust/response/{}/newest", requesting_client_id);
                        publish_result(client, &response_topic, &nodes).await?;
                    },
                    None => {
                        error!("Failed to get newest nodes for Client-ID: {}", requesting_client_id);

                        let response_topic = format!("rust/response/{}/newest", requesting_client_id);
                        let response = json!({
                            "status": "error",
                            "message": format!("Failed to get new nodes ")
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                }
          
        },
        // you can add data here
        Some("add") => {
            info!("Processing 'add' data for Client-ID: {}", requesting_client_id);
        
            if let Some(add_data) = json_value.get("data") {
                match create_new_relation(add_data, &write_conn).await {
                    Ok(true) => {
                        let response_topic = format!("rust/response/{}/add", requesting_client_id);
                        let response = json!({
                            "status": "success",
                            "message": "Nodes successfully added"
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                    Ok(false) => {
                        warn!("No new nodes created (UUIDs might already exist) for Client-ID: {}", requesting_client_id);
                        let response_topic = format!("rust/response/{}/add", requesting_client_id);
                        let response = json!({
                            "status": "warning",
                            "message": "No new nodes were created (UUIDs might already exist)"
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                    Err(err) => {
                        error!("Failed to add nodes: {} for Client-ID: {}. Error: {}", add_data, requesting_client_id, err);
                        let response_topic = format!("rust/response/{}/add", requesting_client_id);
                        let response = json!({
                            "status": "error",
                            "message": format!("Failed to add nodes: {}", err)
                        });
                        publish_result(client, &response_topic, &response).await?;
                    }
                }
            } else {
                error!("Missing or invalid 'data' field for type 'add'. Input: {}", payload_str);
                let response_topic = format!("rust/response/{}/add", requesting_client_id);
                let response = json!({
                    "status": "error",
                    "message": "Missing or invalid 'data' field"
                });
                publish_result(client, &response_topic, &response).await?;
            }
        },

        Some("relation") => {
            
            info!("Processing 'relation' data for Client-ID: {}", requesting_client_id);
            match export_all_with_relationships(&read_conn_1, Some(1000)).await {
                Some(nodes) => {
                    let response_topic = format!("rust/response/{}/relation", requesting_client_id);
                    publish_result(client, &response_topic, &nodes).await?;
                },
                None => {
                    error!("Failed to get newest nodes for Client-ID: {}", requesting_client_id);

                    let response_topic = format!("rust/response/{}/relation", requesting_client_id);
                    let response = json!({
                        "status": "error",
                        "message": format!("Failed to get new nodes ")
                    });
                    publish_result(client, &response_topic, &response).await?;
                }
            }
      
    },
        
        
        Some("topic") => {
            
            info!("Topic request not implemented yet");
            let response_topic = format!("rust/topic/{}", requesting_client_id);
            let response = json!({
                "status": "not_implemented",
                "message": "Topic request handling not implemented yet"
            });
            publish_result(client, &response_topic, &response).await?;
        },
        Some(other) => {
            
            warn!("Unknown request type: {}", other);
            let response_topic = format!("rust/response/{}", requesting_client_id);
            let response = json!({
                "status": "error",
                "message": format!("Unknown request type: {}", other)
            });
            publish_result(client, &response_topic, &response).await?;
        },
        None => {
            error!("Missing request type");
            return Err("Missing request type".into());
        }
    }
    
    Ok(())
}

pub async fn start_mqtt_client(db_handler: Arc<db::DatabaseCluster>) -> Result<(), Box<dyn Error>> {
  
    let client_id = format!("rust-mqtt-server-{}", Uuid::new_v4());
    
    
    // MQTT-Optionen 
    let mut mqtt_options = MqttOptions::new(
        &client_id,
        env::var("MQTT_BROKER").unwrap_or_else(|_| "mosquitto-broker".into()),
        env::var("MQTT_PORT").unwrap_or_else(|_| "1883".into()).parse::<u16>().unwrap_or(1883)
    );
    
    mqtt_options.set_keep_alive(Duration::from_secs(30));
    mqtt_options.set_credentials(
        env::var("MQTT_USER").unwrap_or_else(|_| "admin".into()),
        env::var("MQTT_PASSWORD").unwrap_or_else(|_| "admin".into())
    );
    
   
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    let client = Arc::new(client);
    
   
    let connect_timeout = Duration::from_secs(10);
    let start_time = Instant::now();
    let mut connected = false;
    
    // Phase 1: Connection 
    while Instant::now().duration_since(start_time) < connect_timeout {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                info!("✅ Broker-Connection succesfull: {:?}, Client-ID: {}", ack, client_id);
                connected = true;
                break;
            },
            Ok(event) => warn!("Zwischenereignis: {:?}", event),
            Err(e) => {
                error!("❌ Connection Failed: {}", e);
                return Err(e.into());
            }
        }
    }
    
    if !connected {
        error!("⌛ Timeout: Broker not reachable!");
        return Err("Broker offline".into());
    }
    
    // Phase 2: Normal Service
    info!("🚀 Start normal service... Client-ID: {}", client_id);
    

    let subscribe_client = client.clone();
    subscribe_client.subscribe("rust/request", QoS::AtLeastOnce).await?;
    info!("🔔 Subbed to: rust/request");
    
    // Heartbeat-Timer
    let heartbeat_client = client.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let status = json!({
                "server_id": client_id,
                "status": "running",
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            if let Err(e) = heartbeat_client.publish(
                "rust/status", 
                QoS::AtLeastOnce, 
                false, 
                serde_json::to_string(&status).unwrap()
            ).await {
                error!("❌ Failed to send Heartbeat: {}", e);
            }
        }
    });
    
    // Event-Processing 
    info!("👂 Waiting for request...");
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(msg))) => {
                if msg.topic == "rust/request" {
                    let process_client = client.clone();
                    let payload = msg.payload.to_vec();
                   

                    
                    
                    // Processing it in a seperat Task 
                    let db_handler_clone = Arc::clone(&db_handler); 
               
                    tokio::spawn(async move {
                        if let Err(e) = process_request(&process_client, &payload, &db_handler_clone).await {
                            error!("❌ Failed to process request: {}", e);
                        }
                    });
                }
            },
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                warn!("🔌 Connection to Broker broken, trying to connect...");
               
            },
            Ok(_) => {}, 
            Err(e) => {
                error!("❌ Failed in the Event-Loop: {}", e);
               
                time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
