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
/// RPC: request/reply over AMQP. The client publishes a request on
/// a shared request queue and waits on a private callback queue; the
/// server answers on the callback queue named by the request's
/// `reply-to` property, and the `correlation_id` property pairs each
/// reply with its request. This example runs both roles on one
/// channel, so it can be read as a single process talking to itself
/// through the broker — the client half and the server half are
/// separated by comments.
///
/// Run it against a local RabbitMQ with:
///
///     moon build --target js examples/rpc
///     node _build/js/debug/build/examples/rpc/rpc.js
///
/// `RABBITMQ_HOST`, `RABBITMQ_USER` and `RABBITMQ_PASSWORD` pick the
/// broker and credentials (defaults: 127.0.0.1:5672, guest/guest).

///|
/// The request queue, shared by every RPC client. Durable and
/// auto-delete like the queues in the other examples.
let request_queue : String = "moon-rpc"

///|
/// The correlation id the client stamps on the request.
let correlation : String = "1"

///|
/// The Fibonacci index the client asks for; the server computes the
/// answer and the client checks it against its own computation.
let n_request : Int = 30

///|
/// Parse a non-negative integer written in ASCII; -1 on any other
/// input. The example reads the digits itself instead of pulling in
/// a string-conversion helper.
fn parse_uint(s : String) -> Int {
  let mut n = 0
  for b in @utf8.encode(s) {
    let d = b.to_int()
    if d < 48 || d > 57 {
      return -1
    }
    n = n * 10 + (d - 48)
  }
  n
}

///|
/// The `n`-th Fibonacci number, computed iteratively.
fn fib(n : Int) -> Int {
  let mut a = 0
  let mut b = 1
  for _ in 0..<n {
    let t = a + b
    a = b
    b = t
  }
  a
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
/// Start a consumer on `queue` with the requested tag and return the
/// actual tag.
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
/// The whole session: declare the queues, publish the request,
/// answer it on the server half, and check the answer on the client
/// half.
fn run_session(d : @driver.Driver) -> Unit raise @driver.DriverError {
  let (host, port, user, password) = @driver.broker_from_env()
  d.connect_open(host, port, user, password)
  d.open_channel()
  // The request queue.
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: request_queue,
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
  // The client's private callback queue: declaring with an empty
  // name asks the broker to generate one, reported in declare-ok.
  @driver.send_channel_method(
    @channel.ChannelMethod::QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: "",
      passive: false,
      durable: false,
      exclusive: true,
      auto_delete: true,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  let callback = match
    d.expect_channel("queue.declare-ok", fn(m) {
      m is @channel.ChannelMethod::QueueDeclareOk(_)
    }) {
    @channel.ChannelMethod::QueueDeclareOk(q) => q.queue
    _ => raise @driver.DriverError::Protocol("expected queue.declare-ok")
  }
  // The server consumes the request queue, the client consumes the
  // callback queue — both on this channel.
  let server_tag = start_consumer(d, request_queue, "rpc-server")
  let client_tag = start_consumer(d, callback, "rpc-client")
  // Client: publish the request with the correlation id and the
  // reply-to queue.
  publish("", request_queue, @utf8.encode(n_request.to_string()), @basic.BasicProperties::{
    content_type: Some("text/plain"),
    content_encoding: None,
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: Some(correlation),
    reply_to: Some(callback),
    expiration: None,
    message_id: None,
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: None,
    cluster_id: None,
  })
  // Server: receive the request, compute the answer, and reply to
  // the callback queue under the same correlation id.
  let (req_deliver, req_header, req_body) = next_delivery(d, server_tag)
  let n = parse_uint(@utf8.decode_lossy(req_body))
  if n < 0 {
    raise @driver.DriverError::Protocol(
      "request body is not a non-negative integer",
    )
  }
  let reply_to = match req_header.properties.reply_to {
    Some(q) => q
    None => raise @driver.DriverError::Protocol("request carries no reply-to")
  }
  @driver.send_basic_method(
    @basic.BasicMethod::BasicAck(@basic.BasicAck::{
      delivery_tag: req_deliver.delivery_tag,
      multiple: false,
    }),
  )
  publish("", reply_to, @utf8.encode(fib(n).to_string()), @basic.BasicProperties::{
    content_type: Some("text/plain"),
    content_encoding: None,
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: req_header.properties.correlation_id,
    reply_to: None,
    expiration: None,
    message_id: None,
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: None,
    cluster_id: None,
  })
  // Client: receive the reply and check both the correlation id and
  // the answer.
  let (rep_deliver, rep_header, rep_body) = next_delivery(d, client_tag)
  if rep_header.properties.correlation_id != Some(correlation) {
    raise @driver.DriverError::Protocol("reply correlation id mismatch")
  }
  let expected = fib(n_request).to_string()
  if @utf8.decode_lossy(rep_body) != expected {
    raise @driver.DriverError::Protocol(
      "wrong answer: got \"" +
      @utf8.decode_lossy(rep_body) +
      "\", expected \"" +
      expected +
      "\"",
    )
  }
  @driver.send_basic_method(
    @basic.BasicMethod::BasicAck(@basic.BasicAck::{
      delivery_tag: rep_deliver.delivery_tag,
      multiple: false,
    }),
  )
  println(
    "rpc ok: fib(" +
    n_request.to_string() +
    ") = " +
    expected +
    " via callback queue \"" +
    callback +
    "\"",
  )
  // Cancel both consumers and close the connection cleanly.
  @driver.send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: server_tag,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  @driver.send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: client_tag,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  d.close((200).to_uint16(), "Goodbye")
}

///|
/// Entry point: run the session and exit 0 on success, 1 on any
/// failure. The transport only exists on the JS backend, so run this
/// with node after `moon build --target js`.
fn main {
  let d = @driver.Driver::new()
  run_session(d) catch {
    err => {
      println("rpc failed: " + @driver.driver_error_msg(err))
      let _ = @driver.terminate(1)
    }
  }
  let _ = @driver.terminate(0)
}
