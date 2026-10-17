use rand::Rng;
use serde::{Deserialize, Serialize};

// ---- Config shared by both programs (change STUDENT_ID once, topics follow) ----
pub const STUDENT_ID: &str = "202417158";
pub const BROKER_HOST: &str = "broker.hivemq.com";
pub const BROKER_PORT: u16 = 1883;

/// One telemetry sample. The publisher serializes it, the subscriber parses it.
#[derive(Debug, Serialize, Deserialize)]
pub struct Telemetry {
    pub id: u32,
    pub name: String,
    pub packet_no: u64,
    pub temperature: f64,
    pub humidity: f64,
    pub tds: u32,
    // JSON key is "pH" (capital H), Rust field names are snake_case
    #[serde(rename = "pH")]
    pub ph: f64,
}

pub fn telemetry_topic() -> String {
    format!("it4735e/{STUDENT_ID}/telemetry")
}

pub fn status_topic() -> String {
    format!("it4735e/{STUDENT_ID}/status")
}

/// Wildcard filter: matches telemetry, status and anything else under our ID.
pub fn subscribe_filter() -> String {
    format!("it4735e/{STUDENT_ID}/#")
}

/// Unique ID per run and per role. Two clients with the same ID kick each other off the broker.
pub fn client_id(role: &str) -> String {
    format!("{STUDENT_ID}-{role}-{:08x}", rand::thread_rng().gen::<u32>())
}
