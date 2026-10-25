use lapin::{
    options::QueueDeclareOptions, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

// ---- Config shared by all programs (change STUDENT_ID once, names follow) ----
pub const STUDENT_ID: &str = "202417158";
/// Used when CLOUDAMQP_URL is not set: local Docker broker, guest works on localhost only.
pub const LOCAL_URL: &str = "amqp://guest:guest@localhost:5672/%2f";

/// One telemetry sample. The producer serializes it, the consumer parses it.
#[derive(Debug, Serialize, Deserialize)]
pub struct Telemetry {
    // JSON key is "DeviceId", Rust field names are snake_case
    #[serde(rename = "DeviceId")]
    pub device_id: u32,
    pub packet_no: u64,
    pub temperature: f64,
    pub humidity: f64,
}

pub fn queue_name() -> String {
    format!("it4735e.{STUDENT_ID}.telemetry")
}

pub fn broadcast_exchange() -> String {
    format!("it4735e.{STUDENT_ID}.broadcast")
}

pub fn sensors_exchange() -> String {
    format!("it4735e.{STUDENT_ID}.sensors")
}

/// Round to 1 decimal so the printed JSON stays readable.
fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

/// Temperature ~30, humidity ~60, small random jitter.
pub fn make_sample(packet_no: u64) -> Telemetry {
    let mut rng = rand::thread_rng();
    Telemetry {
        device_id: 11,
        packet_no,
        temperature: round1(30.0 + rng.gen_range(-1.0..1.0)),
        humidity: round1(60.0 + rng.gen_range(-2.0..2.0)),
    }
}

/// Strip "user:password@" so the password never ends up in the terminal or a screenshot.
fn redact(url: &str) -> String {
    match (url.split_once("://"), url.rfind('@')) {
        (Some((scheme, _)), Some(at)) => format!("{scheme}://{}", &url[at + 1..]),
        _ => url.to_string(),
    }
}

/// The password is read from CLOUDAMQP_URL, never written in the source.
/// An amqps:// URL connects over TLS (port 5671).
pub async fn connect() -> lapin::Result<Connection> {
    let url = std::env::var("CLOUDAMQP_URL").unwrap_or_else(|_| LOCAL_URL.to_string());
    let conn = Connection::connect(&url, ConnectionProperties::default()).await?;
    println!("Connected to {}", redact(&url));
    Ok(conn)
}

/// Producer and consumer both call this: the queue must be declared with exactly
/// the same properties, otherwise RabbitMQ refuses it with PRECONDITION_FAILED.
pub async fn declare_queue(ch: &Channel) -> lapin::Result<()> {
    let opts = QueueDeclareOptions { durable: true, ..Default::default() };
    ch.queue_declare(&queue_name(), opts, FieldTable::default()).await?;
    Ok(())
}

/// Persistent (delivery_mode 2) JSON message: survives a broker restart in a durable queue.
pub fn json_properties() -> BasicProperties {
    BasicProperties::default()
        .with_delivery_mode(2)
        .with_content_type("application/json".into())
}
