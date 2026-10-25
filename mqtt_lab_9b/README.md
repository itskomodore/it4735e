# mqtt_lab_9b – Exercise 3, Part 2 (AMQP homework)

Producer/consumer pair exchanging telemetry through RabbitMQ using `lapin`.
Student ID `202417158` is set once in `src/lib.rs` (`STUDENT_ID`); all queue and exchange names follow it.

| Program | Task |
|---------|------|
| `producer` | a) Declares durable queue `it4735e.202417158.telemetry`, publishes a persistent `application/json` message every 5 s, `packet_no` starts at 126 and increases by 1 |
| `consumer` | b) Consumes the same queue (same declare properties), prints `DeviceId`, `packet_no`, `temperature`, `humidity`; invalid JSON prints an error and the consumer keeps running |
| `fanout_producer` / `fanout_consumer` | d) Fanout exchange `it4735e.202417158.broadcast`; each consumer gets its own temporary queue |
| `topic_producer` / `topic_consumer <binding>` | d) Topic exchange `it4735e.202417158.sensors` with routing keys `sensor.<room>.<kind>` |

## Broker

The URL is read from the `CLOUDAMQP_URL` environment variable, so the password is never in the source.
Without it, the programs use the local Docker broker (`amqp://guest:guest@localhost:5672/%2f`).

```sh
# Option A – local Docker broker, management UI at http://localhost:15672 (guest / guest)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management

# Option B – CloudAMQP over TLS (port 5671)
export CLOUDAMQP_URL='amqps://<user>:<password>@<host>/<vhost>'
```

The `Connected to ...` line strips the user and password before printing.

## Run

```sh
cargo run --bin producer     # terminal 1
cargo run --bin consumer     # terminal 2
```

```
Connected to amqp://localhost:5672/%2f
Queue it4735e.202417158.telemetry declared (durable)
>> Sent packet_no=126 (66 bytes)
>> Sent packet_no=127 (66 bytes)
```

```
 [*] Waiting for messages on it4735e.202417158.telemetry. To exit press CTRL+C
<< Received (66 bytes): DeviceId=11, packet_no=126, temperature=30.3, humidity=61.6
<< Received (66 bytes): DeviceId=11, packet_no=127, temperature=31.0, humidity=61.4
<< Received (5 bytes): hello
   JSON error: expected value at line 1 column 1
```

## c) Testing with the management UI

1. Open **Queues → it4735e.202417158.telemetry** and watch the message rate while both programs run.
2. Stop the consumer (CTRL+C), let a few messages go out, check that **Ready** grows, start the consumer again: it receives the waiting messages in order with no `packet_no` missing.
3. Use **Publish message** on the queue page: send `{"DeviceId":99,"packet_no":1,"temperature":25,"humidity":40}` (parsed) and then `hello` (prints `JSON error`, the consumer keeps running).

## d) Extensions

Fanout – start two consumers, then the producer; both print every `packet_no`:

```sh
cargo run --bin fanout_consumer   # terminal 1
cargo run --bin fanout_consumer   # terminal 2
cargo run --bin fanout_producer   # terminal 3
```

Topic – the producer cycles through `sensor.room1.temperature`, `sensor.room1.humidity`,
`sensor.room2.temperature` and `sensor.room2.humidity`:

```sh
cargo run --bin topic_consumer 'sensor.room1.*'         # terminal 1
cargo run --bin topic_consumer 'sensor.*.temperature'   # terminal 2
cargo run --bin topic_producer                          # terminal 3
```

`*` matches exactly one word, so:

| Routing key | `sensor.room1.*` | `sensor.*.temperature` |
|-------------|:---:|:---:|
| `sensor.room1.temperature` | ✓ | ✓ |
| `sensor.room1.humidity` | ✓ | |
| `sensor.room2.temperature` | | ✓ |
| `sensor.room2.humidity` | | |

`sensor.room1.temperature` matches both bindings, so each consumer gets its own copy.
`sensor.room2.humidity` matches neither and is dropped (no queue is bound for it).
