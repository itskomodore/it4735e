//! Application client (subscriber): receives telemetry, parses JSON, displays values.
use std::time::Duration;

use mqtt_lab::{BROKER_HOST, BROKER_PORT, TOPIC, Telemetry, client_suffix};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

#[tokio::main]
async fn main() {
    let client_id = format!("group10-app-{}", client_suffix());
    let mut opts = MqttOptions::new(&client_id, BROKER_HOST, BROKER_PORT);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    println!("[app] {client_id} -> {BROKER_HOST}:{BROKER_PORT}");

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                // (Re)subscribe on every connect so reconnects keep working.
                println!("[app] connected, subscribing to {TOPIC}");
                if let Err(e) = client.subscribe(TOPIC, QoS::AtLeastOnce).await {
                    eprintln!("[app] subscribe error: {e}");
                }
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                match serde_json::from_slice::<Telemetry>(&p.payload) {
                    Ok(t) => println!(
                        "[app] Temperature: {} °C | Humidity: {} %",
                        t.temperature, t.humidity
                    ),
                    Err(e) => eprintln!(
                        "[app] bad payload on {}: {} ({e})",
                        p.topic,
                        String::from_utf8_lossy(&p.payload)
                    ),
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[app] connection error: {e}, retrying in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
