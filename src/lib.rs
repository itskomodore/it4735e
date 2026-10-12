use serde::{Deserialize, Serialize};

pub const BROKER_HOST: &str = "broker.hivemq.com";
pub const BROKER_PORT: u16 = 1883;
pub const TOPIC: &str = "it4735e/group10/telemetry";

#[derive(Debug, Serialize, Deserialize)]
pub struct Telemetry {
    pub temperature: f64,
    pub humidity: f64,
}

/// Client ID suffix so we don't collide with other clients on the public broker.
pub fn client_suffix() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos()
}
