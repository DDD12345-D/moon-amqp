// Copyright 2026 moon-amqp contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
/// Publish/subscribe: one producer, several subscribers. A fanout
/// exchange ignores routing keys and copies every published message
/// to all bound queues. This example declares the exchange, binds
/// two auto-delete queues to it, starts a consumer on each, and
/// publishes a single message that both consumers must receive.
///
/// Run it against a local RabbitMQ with:
///
///     moon build --target js examples/pubsub
///     node _build/js/debug/build/examples/pubsub/pubsub.js
///
/// `RABBITMQ_HOST`, `RABBITMQ_USER` and `RABBITMQ_PASSWORD` pick the
/// broker and credentials (defaults: 127.0.0.1:5672, guest/guest).

///|
/// The fanout exchange and its two queues. Both queues are durable
/// and auto-delete: durable because RabbitMQ 4.x refuses non-durable
/// named queues, auto-delete so the broker removes them once their
/// consumers are cancelled.
let exchange : String = "moon-logs"

///|
let queue_a : String = "moon-pubsub-a"

///|
let queue_b : String = "moon-pubsub-b"

///|
/// The consumer tag the second consumer asks for explicitly.
let tag_b : String = "consumer-b"

///|
/// The published message; both deliveries must reproduce it exactly.
let message_body : Bytes = b"moon-amqp pubsub: one fanout message, two copies"

///|
/// Message properties for a plain, non-persistent message.
fn plain_props() -> @basic.BasicProperties {
  @basic.BasicProperties::{
    content_type: Some("text/plain"),
    content_encoding: None,
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: None,
    reply_to: None,
    expiration: None,
    message_id: None,
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: None,
    cluster_id: None,
  }
}

///|
/// Publish `body` to `exchange` under `routing_key` with the given
/// properties: a Basic.Publish method, a content header frame, and a
/// body frame.
fn publish(
  exchange : String,
  routing_key : String,
  body : Bytes,
  properties : @basic.BasicProperties,
) -> Unit raise @driver.DriverError {
  @driver.send_basic_method(
    @basic.BasicMethod::BasicPublish(@basic.BasicPublish::{
      reserved_1: (0).to_uint16(),
      exchange,
      routing_key,
      mandatory: false,
      immediate: false,
    }),
  )
  let header = @basic.ContentHeader::{
    body_size: body.length().to_uint64(),
    properties,
  }
  let hw = @codec.Writer::new()
  header.encode(hw) catch {
    err => raise @driver.DriverError::Protocol(@driver.basic_error_msg(err))
  }
  @driver.send_frame(@frame.Frame::Header((1).to_uint16(), hw.to_bytes()))
  @driver.send_frame(@frame.Frame::Body((1).to_uint16(), body))
}

///|
/// Declare the durable, auto-delete queue `name`.
fn declare_queue(
  d : @driver.Driver,
  name : String,
) -> Unit raise @driver.DriverError {
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: name,
      passive: false,
      durable: true,
      exclusive: false,
      auto_delete: true,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  let _ = d.expect_channel("queue.declare-ok", fn(m) {
    m is @channel.ChannelMethod::QueueDeclareOk(_)
  })
}

///|
/// Bind `queue` to the fanout exchange; fanout ignores routing keys,
/// so the binding key is the empty string.
fn bind_queue(
  d : @driver.Driver,
  queue : String,
) -> Unit raise @driver.DriverError {
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueBind(@channel.QueueBind::{
      reserved_1: (0).to_uint16(),
      queue,
      exchange,
      routing_key: "",
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  let _ = d.expect_channel("queue.bind-ok", fn(m) {
    m is @channel.ChannelMethod::QueueBindOk
  })
}

///|
/// Start a consumer on `queue` with the requested tag (an empty tag
/// asks the broker to assign one) and return the actual tag.
fn start_consumer(
  d : @driver.Driver,
  queue : String,
  tag : String,
) -> String raise @driver.DriverError {
  @driver.send_basic_method(
    @basic.BasicMethod::BasicConsume(@basic.BasicConsume::{
      reserved_1: (0).to_uint16(),
      queue,
      consumer_tag: tag,
      no_local: false,
      no_ack: false,
      exclusive: false,
      nowait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  match
    d.expect_basic("basic.consume-ok", fn(m) {
      m is @basic.BasicMethod::BasicConsumeOk(_)
    }) {
    @basic.BasicMethod::BasicConsumeOk(ok) => ok.consumer_tag
    _ => raise @driver.DriverError::Protocol("expected basic.consume-ok")
  }
}

///|
/// Read the next delivery (any consumer), require the content to
/// match `message_body`, and return the method.
fn next_matching_delivery(
  d : @driver.Driver,
) -> @basic.BasicDeliver raise @driver.DriverError {
  let deliver = match
    d.expect_basic("basic.deliver", fn(m) {
      m is @basic.BasicMethod::BasicDeliver(_)
    }) {
    @basic.BasicMethod::BasicDeliver(dl) => dl
    _ => raise @driver.DriverError::Protocol("expected basic.deliver")
  }
  let header = d.next_content_header()
  let body = d.next_content_body(header.body_size.to_int())
  if body != message_body {
    raise @driver.DriverError::Protocol(
      "content mismatch: delivered " +
      body.length().to_string() +
      " bytes, expected " +
      message_body.length().to_string(),
    )
  }
  deliver
}

///|
/// The whole session: declare the exchange and queues, bind them,
/// start both consumers, publish one message, check that each
/// consumer receives exactly one copy, and shut down cleanly.
fn run_session(d : @driver.Driver) -> Unit raise @driver.DriverError {
  let (host, port, user, password) = @driver.broker_from_env()
  d.connect_open(host, port, user, password)
  d.open_channel()
  // The fanout exchange, auto-deleted once its bindings are gone.
  @driver.send_channel_method(
    @channel.ChannelMethod::ExchangeDeclare(@channel.ExchangeDeclare::{
      reserved_1: (0).to_uint16(),
      exchange,
      exchange_type: "fanout",
      passive: false,
      durable: false,
      auto_delete: true,
      internal: false,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  let _ = d.expect_channel("exchange.declare-ok", fn(m) {
    m is @channel.ChannelMethod::ExchangeDeclareOk
  })
  // Two queues, both bound to the exchange.
  declare_queue(d, queue_a)
  declare_queue(d, queue_b)
  bind_queue(d, queue_a)
  bind_queue(d, queue_b)
  // Two consumers on this channel: one with a broker-assigned tag,
  // one with an explicit tag.
  let ctag_a = start_consumer(d, queue_a, "")
  let ctag_b = start_consumer(d, queue_b, tag_b)
  // Publish one message; the fanout exchange must route a copy to
  // each bound queue.
  publish(exchange, "", message_body, plain_props())
  let mut seen_a = 0
  let mut seen_b = 0
  for _ in 0..<2 {
    let deliver = next_matching_delivery(d)
    if deliver.consumer_tag == ctag_a {
      seen_a = seen_a + 1
    } else if deliver.consumer_tag == ctag_b {
      seen_b = seen_b + 1
    } else {
      raise @driver.DriverError::Protocol(
        "delivery arrived for an unknown consumer \"" +
        deliver.consumer_tag +
        "\"",
      )
    }
    @driver.send_basic_method(
      @basic.BasicMethod::BasicAck(@basic.BasicAck::{
        delivery_tag: deliver.delivery_tag,
        multiple: false,
      }),
    )
    println(
      "pubsub: \"" +
      deliver.consumer_tag +
      "\" got \"" +
      @utf8.decode_lossy(message_body) +
      "\"",
    )
  }
  if seen_a != 1 || seen_b != 1 {
    raise @driver.DriverError::Protocol(
      "each subscriber must receive exactly one copy",
    )
  }
  // Cancel both consumers and close the connection cleanly.
  @driver.send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: ctag_a,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  @driver.send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: ctag_b,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  d.close((200).to_uint16(), "Goodbye")
  println("pubsub ok: one fanout message, two copies delivered")
}

///|
/// Entry point: run the session and exit 0 on success, 1 on any
/// failure. The transport only exists on the JS backend, so run this
/// with node after `moon build --target js`.
fn main {
  let d = @driver.Driver::new()
  run_session(d) catch {
    err => {
      println("pubsub failed: " + @driver.driver_error_msg(err))
      let _ = @driver.terminate(1)
    }
  }
  let _ = @driver.terminate(0)
}
