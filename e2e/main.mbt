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
/// End-to-end driver: a complete AMQP 0-9-1 session against a real
/// RabbitMQ broker — connect, handshake, open a channel, declare a
/// queue, publish one message, consume it, verify the content byte
/// for byte, ack it, and close the connection.
///
/// The session runs on the [driver](/driver) package, whose JS-target
/// transport is the only backend-specific part. Run it with:
///
///     moon build --target js e2e
///     node _build/js/debug/build/e2e/e2e.js
///
/// The `RABBITMQ_HOST`, `RABBITMQ_USER` and `RABBITMQ_PASSWORD`
/// environment variables pick the broker and credentials (defaults:
/// 127.0.0.1:5672, guest/guest).

///|
/// The queue used by the session. Durable, because RabbitMQ 4.x
/// refuses non-durable named queues, and auto-delete, so the broker
/// removes it once the consumer is cancelled.
let queue_name : String = "moon-amqp-e2e"

///|
/// The published message; the delivery must reproduce it exactly.
let message_body : Bytes = b"moon-amqp e2e: one message, delivered intact"

///|
/// Run the whole session: handshake, topology, one message round
/// trip, and shutdown.
fn run_session(d : @driver.Driver) -> Unit raise @driver.DriverError {
  let (host, port, user, password) = @driver.broker_from_env()
  d.connect_open(host, port, user, password)
  d.open_channel()
  // Declare the queue.
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: queue_name,
      passive: false,
      durable: true,
      exclusive: false,
      auto_delete: true,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  match
    d.expect_channel("queue.declare-ok", fn(m) {
      m is @channel.ChannelMethod::QueueDeclareOk(_)
    }) {
    @channel.ChannelMethod::QueueDeclareOk(q) =>
      if q.queue != queue_name {
        raise @driver.DriverError::Protocol(
          "declare-ok returned an unexpected queue name",
        )
      }
    _ => raise @driver.DriverError::Protocol("expected queue.declare-ok")
  }
  // Publish one message to the default exchange, routed by the
  // queue name.
  @driver.send_basic_method(
    @basic.BasicMethod::BasicPublish(@basic.BasicPublish::{
      reserved_1: (0).to_uint16(),
      exchange: "",
      routing_key: queue_name,
      mandatory: false,
      immediate: false,
    }),
  )
  let header = @basic.ContentHeader::{
    body_size: message_body.length().to_uint64(),
    properties: @basic.BasicProperties::{
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
    },
  }
  let hw = @codec.Writer::new()
  header.encode(hw) catch {
    err => raise @driver.DriverError::Protocol(@driver.basic_error_msg(err))
  }
  @driver.send_frame(@frame.Frame::Header((1).to_uint16(), hw.to_bytes()))
  @driver.send_frame(@frame.Frame::Body((1).to_uint16(), message_body))
  // Consume: ask the broker for a consumer tag, then wait for the
  // delivery.
  @driver.send_basic_method(
    @basic.BasicMethod::BasicConsume(@basic.BasicConsume::{
      reserved_1: (0).to_uint16(),
      queue: queue_name,
      consumer_tag: "",
      no_local: false,
      no_ack: false,
      exclusive: false,
      nowait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  let ctag = match
    d.expect_basic("basic.consume-ok", fn(m) {
      m is @basic.BasicMethod::BasicConsumeOk(_)
    }) {
    @basic.BasicMethod::BasicConsumeOk(ok) => ok.consumer_tag
    _ => raise @driver.DriverError::Protocol("expected basic.consume-ok")
  }
  // Deliver / header / body: the content must match the published
  // message byte for byte.
  let deliver = match
    d.expect_basic("basic.deliver", fn(m) {
      m is @basic.BasicMethod::BasicDeliver(_)
    }) {
    @basic.BasicMethod::BasicDeliver(dl) => dl
    _ => raise @driver.DriverError::Protocol("expected basic.deliver")
  }
  if deliver.consumer_tag != ctag {
    raise @driver.DriverError::Protocol(
      "delivery arrived for a different consumer tag",
    )
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
  // Ack the delivery, cancel the consumer, and close the connection
  // cleanly.
  @driver.send_basic_method(
    @basic.BasicMethod::BasicAck(@basic.BasicAck::{
      delivery_tag: deliver.delivery_tag,
      multiple: false,
    }),
  )
  @driver.send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: ctag,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  d.close((200).to_uint16(), "Goodbye")
  println(
    "e2e ok: declared " +
    queue_name +
    ", published and acked \"" +
    @utf8.decode_lossy(message_body) +
    "\"",
  )
}

///|
/// Entry point: run the session and exit 0 on success, 1 on any
/// failure. The transport only exists on the JS backend, so run this
/// with node after `moon build --target js`.
fn main {
  let d = @driver.Driver::new()
  run_session(d) catch {
    err => {
      println("e2e failed: " + @driver.driver_error_msg(err))
      let _ = @driver.terminate(1)
    }
  }
  let _ = @driver.terminate(0)
}
