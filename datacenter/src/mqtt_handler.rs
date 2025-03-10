use rumqttc::{MqttOptions, AsyncClient, Event, Incoming};
use tokio::task;
use log::{info, error};

pub async fn start_mqtt_client() -> Result<(), Box<dyn std::error::Error>> {
    let mqtt_options = MqttOptions::new("rust-mqtt-client", "broker.hivemq.com", 1883);
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    task::spawn(async move {
        if let Err(e) = client.subscribe("rust/topic", rumqttc::QoS::AtMostOnce).await {
            error!("Failed to subscribe to topic: {:?}", e);
        }
    });

    while let Ok(event) = eventloop.poll().await {
        match event {
            Event::Incoming(Incoming::Publish(publish)) => {
                info!("Received message: {:?}", publish.payload);
            },
            Event::Incoming(Incoming::Disconnect) => {
                info!("MQTT client disconnected");
                break;
            },
            _ => {}
        }
    }

    Ok(())
}