use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use serde_json::json;
use std::time::Duration;

const QUEUE: &str = "it4735e.group10.telemetry";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::connect(
        "amqp://guest:guest@localhost:5672/%2f",
        ConnectionProperties::default(),
    ).await?;
    let ch = conn.create_channel().await?;
    ch.queue_declare(QUEUE, QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()).await?;

    let mut i = 0;
    loop {
        let data = json!({ "temperature": 20 + i % 10, "humidity": 30 + i % 30 });
        ch.basic_publish(
            "",                        // default exchange
            QUEUE,                     // routing key = queue name
            BasicPublishOptions::default(),
            data.to_string().as_bytes(),
            BasicProperties::default(),
        ).await?.await?;               // second await = broker confirm
        println!("Sent: {data}");
        i += 1;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}