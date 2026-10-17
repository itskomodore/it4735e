use std::time::Duration;

use mqtt_hw::{
    client_id, status_topic, telemetry_topic, Telemetry, BROKER_HOST, BROKER_PORT,
};
use rand::Rng;
use rumqttc::{AsyncClient, Event, EventLoop, LastWill, MqttOptions, Outgoing, Packet, QoS};
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};

const START_PACKET_NO: u64 = 126;

/// Round to 1 decimal so the printed JSON stays readable.
fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

/// Task a) build one sample: temperature ~30, humidity ~60, small random jitter.
fn make_sample(packet_no: u64) -> Telemetry {
    let mut rng = rand::thread_rng();
    Telemetry {
        id: 11,
        name: "my-mqtt-client".to_string(),
        packet_no,
        temperature: round1(30.0 + rng.gen_range(-1.0..1.0)),
        humidity: round1(60.0 + rng.gen_range(-2.0..2.0)),
        tds: 1100,
        ph: 5.0,
    }
}

/// Task d) Ctrl+C: retained "offline", then a clean DISCONNECT.
/// A clean disconnect does not fire the Will, so we publish "offline" ourselves.
async fn shutdown(client: &AsyncClient, eventloop: &mut EventLoop) {
    if let Err(e) = client.try_publish(status_topic(), QoS::AtLeastOnce, true, "offline") {
        println!("Offline publish error: {e}");
    }
    if let Err(e) = client.try_disconnect() {
        println!("Disconnect error: {e}");
    }
    // Queued packets only leave the socket while the loop is polled.
    // Stop at the Disconnect packet; the timeout guards against a dead connection.
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
    let id = client_id("pub");
    println!("Client ID: {id}");

    let mut opts = MqttOptions::new(id, BROKER_HOST, BROKER_PORT);
    opts.set_keep_alive(Duration::from_secs(5));
    // Broker publishes this only if we vanish without a clean DISCONNECT
    opts.set_last_will(LastWill::new(status_topic(), "offline", QoS::AtLeastOnce, true));

    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let topic = telemetry_topic();
    let mut packet_no = START_PACKET_NO;

    let mut ticker = interval(Duration::from_secs(5));
    // After the 2 s reconnect sleep, don't fire a burst of missed ticks
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Pin once: creating ctrl_c() every loop turn would re-register the signal handler
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                shutdown(&client, &mut eventloop).await;
                break;
            }
            // Task a) every 5 s. try_publish never waits, so the loop can't block on itself.
            _ = ticker.tick() => match serde_json::to_vec(&make_sample(packet_no)) {
                Ok(payload) => match client.try_publish(&topic, QoS::AtLeastOnce, false, payload) {
                    Ok(()) => {
                        println!(">> Published packet_no={packet_no} to {topic}");
                        // Only advance after a successful publish call
                        packet_no += 1;
                    }
                    Err(e) => println!("Publish error: {e}"),
                },
                Err(e) => println!("JSON encode error: {e}"),
            },
            // poll() drives all network I/O and is cancel-safe inside select!
            event = eventloop.poll() => match event {
                // Task d) announce presence after EVERY (re)connect
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("Connected");
                    if let Err(e) = client.try_publish(status_topic(), QoS::AtLeastOnce, true, "online") {
                        println!("Status publish error: {e}");
                    }
                }
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
