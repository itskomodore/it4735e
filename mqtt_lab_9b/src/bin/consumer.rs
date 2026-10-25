use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};

const QUEUE: &str = "it4735e.group10.telemetry";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::connect(
        "amqp://guest:guest@localhost:5672/%2f",
        ConnectionProperties::default(),
    ).await?;
    let ch = conn.create_channel().await?;
    ch.queue_declare(QUEUE, QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()).await?;

    let mut consumer = ch
        .basic_consume(QUEUE, "consumer1", BasicConsumeOptions::default(), FieldTable::default())
        .await?;
    println!("Waiting... CTRL+C to exit");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        match serde_json::from_slice::<serde_json::Value>(&delivery.data) {
            Ok(d) => println!("Temperature: {} °C, Humidity: {}%", d["temperature"], d["humidity"]),
            Err(e) => eprintln!("JSON error: {e}"),
        }
        delivery.ack(BasicAckOptions::default()).await?;
    }
    Ok(())
}