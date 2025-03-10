use rumqttc::{MqttOptions, AsyncClient, Event, Incoming, QoS, Transport};
use tokio::time::{Duration, Instant};
use log::{info, error, warn};
use serde_json::{Value, Result as JsonResult};
use std::env;
use std::error::Error;
use crate::query::get_specific_uuid_node;
use crate::db::get_db;
use neo4rs::Graph;

pub async fn start_mqtt_client() -> Result<(), Box<dyn Error>> {
    let mut mqtt_options = MqttOptions::new(
        "rust-mqtt-client",
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
                info!("✅ Broker-Verbindung hergestellt: {:?}", ack);
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
    info!("🚀 Starte normalen Betrieb...");
    
    client.subscribe("rust/topic", QoS::AtMostOnce).await?;

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
                handle_message(&publish, &client, &db).await?;
            },
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                info!("🔌 Verbindung getrennt");
                break;
            },
            Err(e) => {
                error!("⚠️ Fehler im Eventloop: {}", e);
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
    db: &Graph
) -> Result<(), Box<dyn Error>> {
    info!("📨 Nachricht empfangen: {:?}", publish.payload);
    let payload_str = std::str::from_utf8(&publish.payload)?;
    parse_and_process_json(payload_str, &db, client).await?;
   

    
    Ok(())
}

async fn parse_and_process_json(
    json_str: &str, 
    db: &Graph,
    client: &AsyncClient
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed: JsonResult<Value> = serde_json::from_str(json_str);

    match parsed {
        Ok(json_value) => {
            if let (Some("uuid"), Some(data)) = (
                json_value.get("type").and_then(Value::as_str),
                json_value.get("data").and_then(Value::as_str)
            ) {
                info!("Processing UUID: {}", data);

               
                match get_specific_uuid_node(data, &db).await {
                    Some(node) => {
                        info!("Found node for UUID {}: {:?}", data, node);
                        let response_topic = format!("rust/response/{}", data);
                        publish_result(client, &response_topic, &node).await?;
                    },
                    None => {
                        info!("No node found for UUID: {}", data);
                        let response_topic = format!("rust/response/{}", data);
                        let empty_response = serde_json::json!({
                            "uuid": data,
                            "found": false,
                            "message": "No data found for this UUID"
                        });
                        publish_result(client, &response_topic, &empty_response).await?;
                    }
                }
            } else {
                error!("JSON does not match expected format. Expected {{\"type\":\"uuid\",\"data\":\"<uuid>\"}}, got: {}", json_str);
            }
        },
        Err(e) => {
            error!("Failed to parse JSON: {}", e);
        }
    }

    Ok(())
}

async fn publish_result(client: &AsyncClient, topic: &str, data: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let response_json = serde_json::to_vec(data)?;
    client.publish(topic, QoS::AtMostOnce, false, response_json).await?;
    info!("Result published to topic: {}", topic);
    Ok(())
}