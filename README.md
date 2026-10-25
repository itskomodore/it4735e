# IT4735E – Lab Work (Group 10)

Lab assignments for IT4735E. Each lab lives in its own directory with its own README.

## Labs

| Lab | Description | Language |
|-----|-------------|----------|
| [mqtt_lab_8a](mqtt_lab_8a/) | Device (publisher) and app (subscriber) exchanging temperature/humidity telemetry as JSON over MQTT (`broker.hivemq.com`, topic `it4735e/group10/telemetry`) | Rust |
| [mqtt_lab_8b](mqtt_lab_8b/) | MQTT homework: publisher/subscriber with numbered packets, last-will status topic, shared `mqtt_hw` lib | Rust |
| [mqtt_lab_9a](mqtt_lab_9a/) | Producer/consumer over AMQP (RabbitMQ, `localhost:5672`, durable queue `it4735e.group10.telemetry`) using `lapin` | Rust |
| [mqtt_lab_9b](mqtt_lab_9b/) | Copy of `mqtt_lab_9a`: producer/consumer over AMQP (RabbitMQ, `localhost:5672`, durable queue `it4735e.group10.telemetry`) using `lapin` | Rust |

## Getting started

```sh
git clone https://github.com/itskomodore/it4735e.git
cd it4735e/<lab>
```

Then follow the README in that lab's directory.
