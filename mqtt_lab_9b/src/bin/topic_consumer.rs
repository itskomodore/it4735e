use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, ExchangeKind};
use mqtt_lab_9b::{connect, sensors_exchange};

/// Task d) Topic: `topic_consumer 'sensor.room1.*'` or `topic_consumer 'sensor.*.temperature'`.
/// `*` matches exactly one word, `#` matches zero or more words.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let binding = std::env::args().nth(1).unwrap_or_else(|| "sensor.#".to_string());
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    let exchange = sensors_exchange();
    ch.exchange_declare(&exchange, ExchangeKind::Topic, ExchangeDeclareOptions::default(), FieldTable::default())
        .await?;

    let queue = ch
        .queue_declare("", QueueDeclareOptions { exclusive: true, ..Default::default() }, FieldTable::default())
        .await?;
    let queue = queue.name().as_str();
    ch.queue_bind(queue, &exchange, &binding, QueueBindOptions::default(), FieldTable::default())
        .await?;

    let mut consumer = ch
        .basic_consume(queue, "", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
        .await?;
    println!(" [*] Waiting on {exchange} with binding {binding}. To exit press CTRL+C");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        println!("<< {}: {}", delivery.routing_key, String::from_utf8_lossy(&delivery.data));
    }
    Ok(())
}
