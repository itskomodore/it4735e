use std::time::Duration;

use lapin::options::BasicPublishOptions;
use mqtt_lab_9b::{connect, declare_queue, json_properties, make_sample, queue_name};
use tokio::time::{interval, MissedTickBehavior};

const START_PACKET_NO: u64 = 126;

/// Task a) publish one sample every 5 s to the durable queue, packet_no + 1 each time.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    declare_queue(&ch).await?;
    println!("Queue {} declared (durable)", queue_name());

    let mut tick = interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut packet_no = START_PACKET_NO;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let payload = serde_json::to_vec(&make_sample(packet_no))?;
                ch.basic_publish(
                    "",              // default exchange
                    &queue_name(),   // routing key = queue name
                    BasicPublishOptions::default(),
                    &payload,
                    json_properties(),
                ).await?.await?;     // second await = broker confirm
                println!(">> Sent packet_no={packet_no} ({} bytes)", payload.len());
                packet_no += 1;
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    conn.close(200, "bye").await?;
    println!("Disconnected");
    Ok(())
}
