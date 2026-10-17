//! Device client (publisher): publishes simulated sensor data every 5 seconds.
use std::time::Duration;

use mqtt_lab::{BROKER_HOST, BROKER_PORT, TOPIC, Telemetry, client_suffix};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

#[tokio::main]
async fn main() {
    let client_id = format!("group10-device-{}", client_suffix());
    let mut opts = MqttOptions::new(&client_id, BROKER_HOST, BROKER_PORT);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    println!("[device] {client_id} -> {BROKER_HOST}:{BROKER_PORT}, topic {TOPIC}");

    tokio::spawn(async move {
        let mut tick: u64 = 0;
        loop {
            // Simple variation around 23 °C / 33 %
            let t = Telemetry {
                temperature: 23.0 + ((tick % 7) as f64 - 3.0) * 0.5,
                humidity: 33.0 + ((tick % 5) as f64 - 2.0),
            };
            let payload = serde_json::to_string(&t).unwrap();
            match client.publish(TOPIC, QoS::AtLeastOnce, false, payload.clone()).await {
                Ok(()) => println!("[device] published {payload}"),
                Err(e) => eprintln!("[device] publish error: {e}"),
            }
            tick += 1;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    // The event loop must be polled for anything to actually be sent.
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => println!("[device] connected"),
            Ok(_) => {}
            Err(e) => {
                eprintln!("[device] connection error: {e}, retrying in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
