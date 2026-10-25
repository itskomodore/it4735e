use std::time::Duration;

use lapin::{options::*, types::FieldTable, ExchangeKind};
use mqtt_lab_9b::{connect, json_properties, sensors_exchange};
use rand::Rng;
use serde_json::json;

const ROUTING_KEYS: [&str; 4] = [
    "sensor.room1.temperature",
    "sensor.room1.humidity",
    "sensor.room2.temperature",
    "sensor.room2.humidity",
];

/// Task d) Topic: the routing key says where the reading comes from; consumers pick by pattern.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    let exchange = sensors_exchange();
    ch.exchange_declare(&exchange, ExchangeKind::Topic, ExchangeDeclareOptions::default(), FieldTable::default())
        .await?;
    println!("Exchange {exchange} declared (topic)");

    for key in ROUTING_KEYS.iter().cycle() {
        let base = if key.ends_with("temperature") { 30.0 } else { 60.0 };
        let value = base + rand::thread_rng().gen_range(-2.0..2.0_f64);
        let payload = json!({ "value": (value * 10.0).round() / 10.0 }).to_string();
        ch.basic_publish(&exchange, key, BasicPublishOptions::default(), payload.as_bytes(), json_properties())
            .await?
            .await?;
        println!(">> {key}: {payload}");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}
