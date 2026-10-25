use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, ExchangeKind};
use mqtt_lab_9b::{broadcast_exchange, connect, Telemetry};

/// Task d) Fanout: run this twice, each run gets its own temporary queue and every message.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    let exchange = broadcast_exchange();
    ch.exchange_declare(&exchange, ExchangeKind::Fanout, ExchangeDeclareOptions::default(), FieldTable::default())
        .await?;

    // Empty name = broker picks one; exclusive = deleted when this connection closes
    let queue = ch
        .queue_declare("", QueueDeclareOptions { exclusive: true, ..Default::default() }, FieldTable::default())
        .await?;
    let queue = queue.name().as_str();
    ch.queue_bind(queue, &exchange, "", QueueBindOptions::default(), FieldTable::default())
        .await?;

    let mut consumer = ch
        .basic_consume(queue, "", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
        .await?;
    println!(" [*] Waiting on {queue} (bound to {exchange}). To exit press CTRL+C");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        match serde_json::from_slice::<Telemetry>(&delivery.data) {
            Ok(t) => println!("<< DeviceId={}, packet_no={}, temperature={:.1}, humidity={:.1}",
                t.device_id, t.packet_no, t.temperature, t.humidity),
            Err(e) => println!("<< JSON error: {e}"),
        }
    }
    Ok(())
}
