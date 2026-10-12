# IT4735E – MQTT Lab (Group 10)

Two Rust clients that exchange sensor telemetry through the public MQTT broker
`broker.hivemq.com:1883`.

```
┌──────────────┐  publish (every 5s)  ┌───────────────────┐  deliver  ┌──────────────┐
│ device (pub) │ ───────────────────▶ │ broker.hivemq.com │ ────────▶ │  app (sub)   │
└──────────────┘                      └───────────────────┘           └──────────────┘
                                                │
                                                ▼
                                        MQTTX (verification)
```

- **Topic:** `it4735e/group10/telemetry`
- **Payload:** JSON, e.g. `{"temperature":23.0,"humidity":33.0}`
- **QoS:** 1 (at least once)

## Project layout

| File | Role |
|------|------|
| `src/lib.rs` | Shared broker settings, topic, and the `Telemetry` struct |
| `src/bin/device.rs` | Device client (publisher): generates temperature/humidity values and publishes JSON every 5 seconds |
| `src/bin/app.rs` | Application client (subscriber): subscribes to the topic, parses JSON, and prints the values |

Dependencies: [`rumqttc`](https://crates.io/crates/rumqttc) (MQTT client),
[`tokio`](https://crates.io/crates/tokio) (async runtime),
[`serde`](https://crates.io/crates/serde) + [`serde_json`](https://crates.io/crates/serde_json) (JSON).

## Requirements

- Rust toolchain (edition 2024, Rust 1.85+) — install via [rustup](https://rustup.rs)
- Outbound access to `broker.hivemq.com` on TCP port 1883

## Running

Open two terminals in the project directory. Start the subscriber first —
MQTT does not store messages for subscribers that aren't connected yet.

```sh
# Terminal 1 – application client (subscriber)
cargo run --bin app

# Terminal 2 – device client (publisher)
cargo run --bin device
```

Expected output:

```
[device] connected
[device] published {"temperature":22.0,"humidity":32.0}

[app] connected, subscribing to it4735e/group10/telemetry
[app] Temperature: 22 °C | Humidity: 32 %
```

Stop either client with `Ctrl+C`.

## Verifying with MQTTX

1. Create a new connection:
   - Host: `broker.hivemq.com`
   - Port: `1883`
   - Client ID: any unique value
2. Connect, then add a subscription to `it4735e/group10/telemetry`.
3. With `device` running, a JSON message should arrive every 5 seconds.

You can also publish a test message from MQTTX to the same topic (e.g.
`{"temperature": 30, "humidity": 50}`) and watch it appear in the `app` output.

## Notes

- Each client appends a time-based suffix to its client ID. The broker is public, and two clients with the same ID would disconnect each other.
- Both clients reconnect automatically after network errors; the app re-subscribes on every reconnect.
- `broker.hivemq.com` is public and unauthenticated — anyone can read or write this topic. Don't send real data over it.
