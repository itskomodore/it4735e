use std::time::Duration;

use lapin::{options::*, types::FieldTable, ExchangeKind};
use mqtt_lab_9b::{broadcast_exchange, connect, json_properties, make_sample};

const START_PACKET_NO: u64 = 126;

/// Task d) Fanout: every queue bound to the exchange gets a copy; the routing key is ignored.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect().await?;
    let ch = conn.create_channel().await?;
    let exchange = broadcast_exchange();
    ch.exchange_declare(&exchange, ExchangeKind::Fanout, ExchangeDeclareOptions::default(), FieldTable::default())
        .await?;
    println!("Exchange {exchange} declared (fanout)");

    let mut packet_no = START_PACKET_NO;
    loop {
        let payload = serde_json::to_vec(&make_sample(packet_no))?;
        ch.basic_publish(&exchange, "", BasicPublishOptions::default(), &payload, json_properties())
            .await?
            .await?;
        println!(">> Broadcast packet_no={packet_no} ({} bytes)", payload.len());
        packet_no += 1;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
