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
/// Work queues: distribute tasks over a shared queue so that every
/// task is delivered to exactly one worker. This example runs a
/// single worker end to end — declare the durable queue, publish
/// five persistent tasks, then consume them one by one and
/// acknowledge each after processing.
///
/// Run it against a local RabbitMQ with:
///
///     moon build --target js examples/work_queues
///     node _build/js/debug/build/examples/work_queues/work_queues.js
///
/// `RABBITMQ_HOST`, `RABBITMQ_USER` and `RABBITMQ_PASSWORD` pick the
/// broker and credentials (defaults: 127.0.0.1:5672, guest/guest).

///|
/// The task queue. Durable, because RabbitMQ 4.x refuses non-durable
/// named queues, and auto-delete, so the broker removes it once the
/// consumer is cancelled.
let queue : String = "moon-task-queue"

///|
/// The number of tasks published before consumption starts.
let task_count : Int = 5

///|
/// Message properties for a persistent task (delivery mode 2).
fn persistent_props() -> @basic.BasicProperties {
  @basic.BasicProperties::{
    content_type: Some("text/plain"),
    content_encoding: None,
    headers: None,
    delivery_mode: Some((2).to_byte()),
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
/// Read the next delivery and require it to belong to `ctag`;
/// returns the method, the content header, and the body.
fn next_delivery(
  d : @driver.Driver,
  ctag : String,
) -> (@basic.BasicDeliver, @basic.ContentHeader, Bytes) raise @driver.DriverError {
  let deliver = match
    d.expect_basic("basic.deliver", fn(m) {
      m is @basic.BasicMethod::BasicDeliver(_)
    }) {
    @basic.BasicMethod::BasicDeliver(dl) => dl
    _ => raise @driver.DriverError::Protocol("expected basic.deliver")
  }
  if deliver.consumer_tag != ctag {
    raise @driver.DriverError::Protocol(
      "delivery arrived for consumer \"" +
      deliver.consumer_tag +
      "\", expected \"" +
      ctag +
      "\"",
    )
  }
  let header = d.next_content_header()
  let body = d.next_content_body(header.body_size.to_int())
  (deliver, header, body)
}

///|
/// The whole session: declare the queue, publish the tasks, consume
/// them one by one (acknowledging each), and shut down cleanly.
fn run_session(d : @driver.Driver) -> Unit raise @driver.DriverError {
  let (host, port, user, password) = @driver.broker_from_env()
  d.connect_open(host, port, user, password)
  d.open_channel()
  // Declare the queue.
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue,
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
  // Publish the tasks with delivery mode 2 (persistent).
  for i in 0..<task_count {
    publish(
      "",
      queue,
      @utf8.encode(
        "task " + (i + 1).to_string() + " of " + task_count.to_string(),
      ),
      persistent_props(),
    )
  }
  // Consume: ask the broker for a consumer tag, then process every
  // task, acknowledging each one after it is "done".
  @driver.send_basic_method(
    @basic.BasicMethod::BasicConsume(@basic.BasicConsume::{
      reserved_1: (0).to_uint16(),
      queue,
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
  for _ in 0..<task_count {
    let (deliver, _header, body) = next_delivery(d, ctag)
    @driver.send_basic_method(
      @basic.BasicMethod::BasicAck(@basic.BasicAck::{
        delivery_tag: deliver.delivery_tag,
        multiple: false,
      }),
    )
    println(
      "work-queues: processed \"" +
      @utf8.decode_lossy(body) +
      "\" (delivery tag " +
      deliver.delivery_tag.to_string() +
      ")",
    )
  }
  // Cancel the consumer and close the connection cleanly.
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
    "work-queues ok: " + task_count.to_string() + " tasks published and acked",
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
      println("work-queues failed: " + @driver.driver_error_msg(err))
      let _ = @driver.terminate(1)
    }
  }
  let _ = @driver.terminate(0)
}
