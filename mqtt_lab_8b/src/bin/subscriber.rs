use std::time::Duration;

use mqtt_hw::{
    client_id, status_topic, subscribe_filter, telemetry_topic, Telemetry, BROKER_HOST,
    BROKER_PORT,
};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Outgoing, Packet, Publish, QoS};
use tokio::time::{sleep, timeout};

/// Task b) handle one incoming message.
fn handle_message(msg: &Publish) {
    println!("<< Received on {} ({} bytes)", msg.topic, msg.payload.len());
    if msg.topic == telemetry_topic() {
        // Never unwrap network data: bad JSON must not kill the client
        match serde_json::from_slice::<Telemetry>(&msg.payload) {
            Ok(t) => {
                println!("   id={}, name={}, packet_no={}", t.id, t.name, t.packet_no);
                println!(
                    "   temperature={}, humidity={}, tds={}, pH={}",
                    t.temperature, t.humidity, t.tds, t.ph
                );
            }
            Err(e) => println!("   JSON error: {e}"),
        }
    } else if msg.topic == status_topic() {
        // Status is plain text ("online"/"offline"), not JSON.
        // retain=true means the broker replayed an old value, not a live change.
        let note = if msg.retain { " (retained)" } else { "" };
        println!("   status: {}{note}", String::from_utf8_lossy(&msg.payload));
    }
}

/// Ctrl+C: clean DISCONNECT. The loop must keep polling so the packet is actually sent.
async fn shutdown(client: &AsyncClient, eventloop: &mut EventLoop) {
    if let Err(e) = client.try_disconnect() {
        println!("Disconnect error: {e}");
    }
    let flush = async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    };
    if timeout(Duration::from_secs(3), flush).await.is_err() {
        println!("Shutdown timed out");
    }
    println!("Bye.");
}

#[tokio::main]
async fn main() {
    let id = client_id("sub");
    println!("Client ID: {id}");

    let mut opts = MqttOptions::new(id, BROKER_HOST, BROKER_PORT);
    opts.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(opts, 10);

    // Pin once: creating ctrl_c() every loop turn would re-register the signal handler
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                shutdown(&client, &mut eventloop).await;
                break;
            }
            // poll() drives all network I/O and is cancel-safe inside select!
            event = eventloop.poll() => match event {
                // Task d) subscribe after EVERY ConnAck: the clean session forgets
                // subscriptions on reconnect. try_* never waits on the loop's own queue.
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    let filter = subscribe_filter();
                    match client.try_subscribe(&filter, QoS::AtLeastOnce) {
                        Ok(()) => println!("Connected, subscribing to {filter}"),
                        Err(e) => println!("Subscribe error: {e}"),
                    }
                }
                Ok(Event::Incoming(Packet::Publish(msg))) => handle_message(&msg),
                Ok(_) => {}
                Err(e) => {
                    // Polling again after an error makes rumqttc reconnect
                    println!("Connection error: {e}. Retry in 2 s");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}
