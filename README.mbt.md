# moon-amqp

Pure MoonBit implementation of the [AMQP 0-9-1](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf) protocol client, for talking to RabbitMQ — with no non-MoonBit code on the protocol path.

- A hand-written, wire-level codec for frames, methods, content headers, and field tables.
- Method-level API covering the `connection`, `channel` (plus exchange and queue), and `basic` AMQP classes.
- A small session `driver` for real-broker sessions — connect, open a channel, publish, consume, close — on the JavaScript backend.
- Runnable examples (work queues, publish/subscribe, RPC) verified against a real RabbitMQ broker.

The library is under active development for the September 2026 MoonBit hackathon; see [Limitations](#limitations) for what is not there yet.

## Installation

```sh
moon add DDD12345-D/moon-amqp
```

The packages, each importable under the module name `DDD12345-D/moon-amqp`:

| Package | Contents |
| --- | --- |
| `codec` | Binary `Writer`/`Reader` for AMQP payload primitives |
| `frame` | The `Frame` type (method, content header, body, heartbeat) |
| `table` | `FieldTable` encoding and decoding |
| `connection` | Connection-class methods, the handshake exchange, heartbeats |
| `channel` | Channel-class methods plus exchange and queue management |
| `basic` | Basic-class methods, `ContentHeader`, `BasicProperties` |
| `driver` | Session driver, including the socket transport (JS backend) |

The examples in this repository run straight from the clone; see the next section.

## Quick start

The fastest way to see the library work is the RPC example against a local broker:

```sh
docker run -d --name moon-amqp-broker -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest rabbitmq:4

moon build --target js examples/rpc
node _build/js/debug/build/examples/rpc/rpc.js
# rpc ok: fib(30) = 832040 via callback queue "amq.gen-..."
```

`examples/work_queues` and `examples/pubsub` run the same way (replace the package and file names in the two commands above). The broker address and credentials come from the `RABBITMQ_HOST`, `RABBITMQ_USER`, and `RABBITMQ_PASSWORD` environment variables (defaults: `127.0.0.1:5672`, `guest`/`guest`). RabbitMQ only accepts `guest` from localhost, so point the variables at your own user for a remote broker.

## Demo

A recorded terminal session against a real RabbitMQ broker: one build, then the four demo programs (end-to-end check, work queues, publish/subscribe, RPC) running in turn — the actual output of the commands above.

![Animated terminal recording: moon-amqp building and running the e2e, work_queues, pubsub, and rpc programs against a real RabbitMQ 4.3.6 broker](https://raw.githubusercontent.com/DDD12345-D/moon-amqp/master/.assets/demo.gif)

## API

The protocol packages expose one typed struct per AMQP method, each with field names and layout following the spec's §4.2 tables, `derive(Eq, Debug)`, encoding into a `codec.Writer`, and decoding from a `codec.Reader`. Each package also defines a `suberror` type (`FrameError`, `ConnectionError`, `ChannelError`, `BasicError`, and friends) for its failure modes; `moon check --deny-warn` keeps error handling exhaustive.

Sessions are built on the `driver` package:

- `Driver::new` creates a session; `connect_open(host, port, user, password)` performs the protocol handshake (Start/Start-Ok with PLAIN credentials, Tune/Tune-Ok, Open/Open-Ok) and `open_channel` opens channel 1.
- `send_frame`, `send_connection_method`, `send_channel_method`, and `send_basic_method` write protocol data.
- `expect_channel(what, predicate)` and `expect_basic(what, predicate)` read the next method on channel 1 and require it to satisfy the predicate; `next_content_header` and `next_content_body(n)` read the content that follows a delivery.
- `close(reply_code, reply_text)` ends the connection cleanly; `broker_from_env()` reads the configuration variables above; `driver_error_msg` renders any `DriverError`, and `terminate(code)` exits the process.

For complete end-to-end sessions — declaring queues, publishing, consuming, acknowledging, binding to fanout exchanges, correlating RPC replies — read the three programs in `examples/`; they are the documentation.

## Verification

Everything below must pass before a commit lands:

```sh
moon check --deny-warn   # type-check every package; warnings are errors
moon test                # unit tests (142)
moon fmt --check         # formatting
```

The e2e and example runs additionally need a broker on `127.0.0.1:5672` (see the Docker line above):

```sh
moon build --target js e2e
node _build/js/debug/build/e2e/e2e.js

moon build --target js examples/work_queues
node _build/js/debug/build/examples/work_queues/work_queues.js

moon build --target js examples/pubsub
node _build/js/debug/build/examples/pubsub/pubsub.js

moon build --target js examples/rpc
node _build/js/debug/build/examples/rpc/rpc.js
```

CI runs exactly these commands against a RabbitMQ service container — see `.github/workflows/ci.yml`.

## References

moon-amqp is a fresh implementation of the [AMQP 0-9-1 specification](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf), the reference text for every wire format here. The design and the recorded-fixture tests were informed by two mature clients:

- [amqplib](https://github.com/amqp-node/amqplib) (MIT license) — its byte-level output for the same protocol exchanges is the cross-check in the recorded connection/channel/basic tests;
- [amqp091-go](https://github.com/rabbitmq/amqp091-go) (BSD-2-Clause license) — consulted for session-structure and error-handling design decisions.

No code is copied from either project: all source is written in MoonBit against the specification. The server-side fixtures are recordings of protocol traffic from a locally run RabbitMQ broker.

## Limitations

- The socket transport only exists on the JavaScript backend (`driver`). The `wasm` and native targets build the protocol packages, but cannot open sessions yet.
- The JavaScript target needs Node.js ≥ 22.3 (the transport uses `process.getBuiltinModule`).
- No TLS (AMQPS) support.
- The driver runs sessions with heartbeats disabled (`heartbeat = 0`), so a dropped TCP connection is not detected mid-session.
- The `tx` and `confirm` AMQP classes are not implemented, and `basic.qos` has no driver-level prefetch management.
- There is no high-level client wrapper (connection strings, topology helpers, auto-declared consumers): sessions are assembled from the method-level API, as the examples show.
- Each delivery's content is read fully into memory.
- Automatic reconnection and channel-error recovery are left to the application.
