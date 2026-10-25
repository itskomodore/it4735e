use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable};
use mqtt_lab_9b::{connect, declare_queue, queue_name, Telemetry};

/// Task b) parse one message. Never unwrap network data: bad JSON must not kill the consumer.
fn handle_message(data: &[u8]) {
    match serde_json::from_slice::<Telemetry>(data) {
        Ok(t) => println!(
            "<< Received ({} bytes): DeviceId={}, packet_no={}, temperature={:.1}, humidity={:.1}",
            data.len(), t.device_id, t.packet_no, t.temperature, t.humidity
        ),
        Err(e) => {
            println!("<< Received ({} bytes): {}", data.len(), String::from_utf8_lossy(data));
            println!("   JSON error: {e}");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    declare_queue(&ch).await?;

    let mut consumer = ch
        .basic_consume(&queue_name(), "consumer", BasicConsumeOptions::default(), FieldTable::default())
        .await?;
    println!(" [*] Waiting for messages on {}. To exit press CTRL+C", queue_name());

    loop {
        tokio::select! {
            delivery = consumer.next() => {
                let Some(delivery) = delivery else { break };
                let delivery = delivery?;
                handle_message(&delivery.data);
                // Ack invalid messages too, otherwise they are redelivered forever
                delivery.ack(BasicAckOptions::default()).await?;
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    conn.close(200, "bye").await?;
    println!("Disconnected");
    Ok(())
}
