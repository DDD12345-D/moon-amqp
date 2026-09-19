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
/// The driver is pure MoonBit on top of the library packages; the
/// only backend-specific part is the byte transport in bridge_js.mbt
/// (JS target) and its aborting stubs in bridge_nonjs.mbt (every
/// other target). Run it with:
///
///     moon build --target js
///     node _build/js/debug/build/e2e/main.js
///
/// The `RABBITMQ_HOST`, `RABBITMQ_USER` and `RABBITMQ_PASSWORD`
/// environment variables pick the broker and credentials (defaults:
/// 127.0.0.1:5672, guest/guest).

///|
/// Anything that can go wrong while driving the session.
suberror E2eError {
  /// The TCP connection to the broker could not be established.
  ConnectFailed(String)
  /// The connection handshake (Start/Tune/Open) went wrong.
  Handshake(String)
  /// A frame or method that does not fit the expected session.
  Protocol(String)
  /// The broker sent nothing while a frame was pending.
  Timeout(String)
  /// The delivered message differs from the published one.
  ContentMismatch(String)
}

///|
/// Receive-buffer budget: larger than any frame in this session
/// (frame-max negotiates to 131072).
let max_buf : Int = 1 << 20

///|
/// One socket-read chunk.
let chunk_size : Int = 8192

///|
/// The queue used by the session. Durable, because RabbitMQ 4.x
/// refuses non-durable named queues, and auto-delete, so the broker
/// removes it once the consumer is cancelled.
let queue_name : String = "moon-amqp-e2e"

///|
/// The published message; the delivery must reproduce it exactly.
let message_body : Bytes = b"moon-amqp e2e: one message, delivered intact"

///|
/// Bytes received from the socket but not yet consumed as frames.
struct Inbuf {
  buf : FixedArray[Byte]
  mut len : Int
}

///|
fn Inbuf::new() -> Inbuf {
  { buf: FixedArray::makei(max_buf, fn(_) { b'\x00' }), len: 0, }
}

///|
fn Inbuf::append(self : Inbuf, data : FixedArray[Byte], n : Int) -> Unit {
  for i in 0..<n {
    self.buf[self.len + i] = data[i]
  }
  self.len = self.len + n
}

///|
fn Inbuf::drain(self : Inbuf, n : Int) -> Unit {
  for i in 0..<(self.len - n) {
    self.buf[i] = self.buf[i + n]
  }
  self.len = self.len - n
}

///|
/// The buffered bytes as an immutable `Bytes` for decoding.
fn Inbuf::to_bytes(self : Inbuf) -> Bytes {
  Bytes::from_array(self.buf[0:self.len])
}

///|
/// The session state: the receive buffer, a scratch chunk for socket
/// reads, and the connection handshake machine.
struct Driver {
  inbuf : Inbuf
  chunk : FixedArray[Byte]
  hs : @connection.Handshake
}

///|
fn Driver::new() -> Driver {
  {
    inbuf: Inbuf::new(),
    chunk: FixedArray::makei(chunk_size, fn(_) { b'\x00' }),
    hs: @connection.Handshake::new(),
  }
}

///|
/// Read one complete frame, buffering socket chunks until a full
/// frame has arrived.
fn Driver::next_frame(self : Driver) -> @frame.Frame raise E2eError {
  while true {
    if self.inbuf.len >= 8 {
      let bytes = self.inbuf.to_bytes()
      let size = be_u32(bytes, 3)
      if self.inbuf.len >= 8 + size {
        let frame = @frame.Frame::decode(@codec.Reader::new(bytes)) catch {
          @frame.FrameError::Truncated =>
            raise E2eError::Protocol("frame decode reported truncation")
          err => raise E2eError::Protocol(frame_error_msg(err))
        }
        self.inbuf.drain(8 + size)
        break frame
      }
    }
    if self.recv_chunk() == 0 {
      raise E2eError::Timeout(
        "no data from the broker while a frame was pending",
      )
    }
  } nobreak {
    raise E2eError::Protocol("frame loop exited without a frame")
  }
}

///|
/// The big-endian unsigned 32-bit integer at `bytes[at..at+4]`.
fn be_u32(bytes : Bytes, at : Int) -> Int {
  (bytes[at].to_int() << 24) |
  (bytes[at + 1].to_int() << 16) |
  (bytes[at + 2].to_int() << 8) |
  bytes[at + 3].to_int()
}

///|
/// Wait up to 5 s for socket data and buffer one chunk of it.
/// Returns the number of bytes buffered (0 when nothing arrived).
fn Driver::recv_chunk(self : Driver) -> Int {
  if js_wait(5000) == 0 {
    return 0
  }
  let n = js_recv(self.chunk)
  if n <= 0 {
    return 0
  }
  self.inbuf.append(self.chunk, n)
  n
}

///|
/// Send one encoded frame over the socket.
fn send_frame(frame : @frame.Frame) -> Unit {
  let w = @codec.Writer::new()
  frame.encode(w)
  js_send(w.to_bytes())
}

///|
/// Send a connection-class method on channel 0.
fn send_connection_method(
  m : @connection.ConnectionMethod,
) -> Unit raise E2eError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise E2eError::Protocol(connection_error_msg(err))
  }
  send_frame(@frame.Frame::Method((0).to_uint16(), w.to_bytes()))
}

///|
/// Send a channel/exchange/queue-class method on channel 1.
fn send_channel_method(m : @channel.ChannelMethod) -> Unit raise E2eError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise E2eError::Protocol(channel_error_msg(err))
  }
  send_frame(@frame.Frame::Method((1).to_uint16(), w.to_bytes()))
}

///|
/// Send a basic-class method on channel 1.
fn send_basic_method(m : @basic.BasicMethod) -> Unit raise E2eError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise E2eError::Protocol(basic_error_msg(err))
  }
  send_frame(@frame.Frame::Method((1).to_uint16(), w.to_bytes()))
}

///|
/// Read the next connection-class method frame on channel 0,
/// skipping heartbeats.
fn Driver::next_connection_method(
  self : Driver,
) -> @connection.ConnectionMethod raise E2eError {
  while true {
    match self.next_frame() {
      @frame.Frame::Method(channel, payload) => {
        if channel != (0).to_uint16() {
          raise E2eError::Protocol(
            "unexpected method frame on channel " + channel.to_int().to_string(),
          )
        }
        let m = @connection.ConnectionMethod::decode(
          @codec.Reader::new(payload),
        ) catch {
          err => raise E2eError::Protocol(connection_error_msg(err))
        }
        break m
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise E2eError::Protocol(
          "expected a connection method frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise E2eError::Protocol("method loop exited without a method")
  }
}

///|
/// A method frame on channel 1: the channel/exchange/queue classes
/// or the basic class.
enum Ch1Method {
  Channel(@channel.ChannelMethod)
  Basic(@basic.BasicMethod)
}

///|
/// Decode a channel-1 method payload, dispatching on the class id:
/// 60 selects the basic-class decoder, anything else the
/// channel/exchange/queue one.
fn decode_ch1_method(payload : Bytes) -> Ch1Method raise E2eError {
  let class_id = (payload[0].to_int() << 8) | payload[1].to_int()
  if class_id == 60 {
    Ch1Method::Basic(
      @basic.BasicMethod::decode(@codec.Reader::new(payload)) catch {
        err => raise E2eError::Protocol(basic_error_msg(err))
      },
    )
  } else {
    Ch1Method::Channel(
      @channel.ChannelMethod::decode(@codec.Reader::new(payload)) catch {
        err => raise E2eError::Protocol(channel_error_msg(err))
      },
    )
  }
}

///|
/// Read the next method frame on channel 1, skipping heartbeats and
/// dispatching on the class id.
fn Driver::next_ch1_method(self : Driver) -> Ch1Method raise E2eError {
  while true {
    match self.next_frame() {
      @frame.Frame::Method(channel, payload) => {
        if channel != (1).to_uint16() {
          raise E2eError::Protocol(
            "unexpected method frame on channel " + channel.to_int().to_string(),
          )
        }
        break decode_ch1_method(payload)
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise E2eError::Protocol(
          "expected a channel method frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise E2eError::Protocol("method loop exited without a method")
  }
}

///|
/// Read one channel-1 method and require it to satisfy `p`; returns
/// the method, used to pin the expected reply of each request.
fn Driver::expect_channel(
  self : Driver,
  what : String,
  p : (@channel.ChannelMethod) -> Bool,
) -> @channel.ChannelMethod raise E2eError {
  match self.next_ch1_method() {
    Ch1Method::Channel(m) =>
      if p(m) {
        m
      } else {
        raise E2eError::Protocol(
          "expected " + what + ", got " + channel_method_name(m),
        )
      }
    Ch1Method::Basic(m) =>
      raise E2eError::Protocol(
        "expected " + what + ", got " + basic_method_name(m),
      )
  }
}

///|
/// Read one basic-class method on channel 1 and require it to
/// satisfy `p`; returns the method.
fn Driver::expect_basic(
  self : Driver,
  what : String,
  p : (@basic.BasicMethod) -> Bool,
) -> @basic.BasicMethod raise E2eError {
  match self.next_ch1_method() {
    Ch1Method::Basic(m) =>
      if p(m) {
        m
      } else {
        raise E2eError::Protocol(
          "expected " + what + ", got " + basic_method_name(m),
        )
      }
    Ch1Method::Channel(m) =>
      raise E2eError::Protocol(
        "expected " + what + ", got " + channel_method_name(m),
      )
  }
}

///|
/// Read the content header that follows a delivery, skipping
/// heartbeats.
fn Driver::next_content_header(
  self : Driver,
) -> @basic.ContentHeader raise E2eError {
  while true {
    match self.next_frame() {
      @frame.Frame::Header(channel, payload) => {
        if channel != (1).to_uint16() {
          raise E2eError::Protocol(
            "unexpected header frame on channel " + channel.to_int().to_string(),
          )
        }
        let h = @basic.ContentHeader::decode(@codec.Reader::new(payload)) catch {
          err => raise E2eError::Protocol(basic_error_msg(err))
        }
        break h
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise E2eError::Protocol(
          "expected a content header frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise E2eError::Protocol("header loop exited without a header")
  }
}

///|
/// Read `total` bytes of content body, split over as many body
/// frames as the broker sends.
fn Driver::next_content_body(
  self : Driver,
  total : Int,
) -> Bytes raise E2eError {
  let body = FixedArray::makei(total, fn(_) { b'\x00' })
  let mut got = 0
  while got < total {
    match self.next_frame() {
      @frame.Frame::Body(channel, payload) => {
        if channel != (1).to_uint16() {
          raise E2eError::Protocol(
            "unexpected body frame on channel " + channel.to_int().to_string(),
          )
        }
        if got + payload.length() > total {
          raise E2eError::Protocol("content body exceeds the announced size")
        }
        for i in 0..<payload.length() {
          body[got + i] = payload[i]
        }
        got = got + payload.length()
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise E2eError::Protocol(
          "expected a content body frame, got " + frame_name(other),
        )
    }
  }
  Bytes::from_array(body[0:total])
}

///|
/// The client-properties table sent with Connection.Start-Ok.
fn client_properties() -> @table.FieldTable {
  @table.FieldTable::from_entries([
    ("product", @table.FieldValue::LongStr("moon-amqp")),
    ("version", @table.FieldValue::LongStr("0.1.0")),
    ("platform", @table.FieldValue::LongStr("MoonBit")),
  ])
}

///|
/// Run the whole session: handshake, topology, one message round
/// trip, and shutdown.
fn run_session(d : Driver) -> Unit raise E2eError {
  let host = match @env.get_env_var("RABBITMQ_HOST") {
    Some(h) => h
    None => "127.0.0.1"
  }
  let user = match @env.get_env_var("RABBITMQ_USER") {
    Some(u) => u
    None => "guest"
  }
  let password = match @env.get_env_var("RABBITMQ_PASSWORD") {
    Some(p) => p
    None => "guest"
  }
  let _ = js_connect(host, 5672)
  if js_wait_connect(5000) != 1 {
    raise E2eError::ConnectFailed("could not connect to " + host + ":5672")
  }
  // Protocol header (§4.2.2).
  let w = @codec.Writer::new()
  @connection.Handshake::write_protocol_header(w)
  js_send(w.to_bytes())
  // Start / Start-Ok.
  let m = d.next_connection_method()
  d.hs.receive(m) catch {
    err => raise E2eError::Handshake(connection_error_msg(err))
  }
  let start_ok = d.hs.start_ok(
    client_properties(),
    "PLAIN",
    "\u0000" + user + "\u0000" + password,
    "en_US",
  ) catch {
    err => raise E2eError::Handshake(connection_error_msg(err))
  }
  send_connection_method(start_ok)
  // Tune / Tune-Ok: echo the server limits, but disable heartbeats
  // so the session timing is deterministic.
  let m = d.next_connection_method()
  match m {
    @connection.ConnectionMethod::Tune(t) => {
      d.hs.receive(m) catch {
        err => raise E2eError::Handshake(connection_error_msg(err))
      }
      let tune_ok = d.hs.tune_ok(t.channel_max, t.frame_max, (0).to_uint16()) catch {
        err => raise E2eError::Handshake(connection_error_msg(err))
      }
      send_connection_method(tune_ok)
    }
    other =>
      raise E2eError::Handshake(
        "expected Connection.Tune, got " + connection_method_name(other),
      )
  }
  // Open / Open-Ok.
  let open = d.hs.open("/") catch {
    err => raise E2eError::Handshake(connection_error_msg(err))
  }
  send_connection_method(open)
  let m = d.next_connection_method()
  match m {
    @connection.ConnectionMethod::OpenOk(_) =>
      d.hs.receive(m) catch {
        err => raise E2eError::Handshake(connection_error_msg(err))
      }
    other =>
      raise E2eError::Handshake(
        "expected Connection.Open-Ok, got " + connection_method_name(other),
      )
  }
  // Channel 1: open it and declare the queue.
  send_channel_method(
    @channel.ChannelMethod::ChannelOpen(@channel.ChannelOpen::{
      reserved_1: "",
    }),
  )
  let _ = d.expect_channel("channel.open-ok", fn(m) {
    m is @channel.ChannelMethod::ChannelOpenOk(_)
  })
  send_channel_method(
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
        raise E2eError::Protocol("declare-ok returned an unexpected queue name")
      }
    _ => raise E2eError::Protocol("expected queue.declare-ok")
  }
  // Publish one message to the default exchange, routed by the
  // queue name.
  send_basic_method(
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
    err => raise E2eError::Protocol(basic_error_msg(err))
  }
  send_frame(@frame.Frame::Header((1).to_uint16(), hw.to_bytes()))
  send_frame(@frame.Frame::Body((1).to_uint16(), message_body))
  // Consume: ask the broker for a consumer tag, then wait for the
  // delivery.
  send_basic_method(
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
    _ => raise E2eError::Protocol("expected basic.consume-ok")
  }
  // Deliver / header / body: the content must match the published
  // message byte for byte.
  let deliver = match
    d.expect_basic("basic.deliver", fn(m) {
      m is @basic.BasicMethod::BasicDeliver(_)
    }) {
    @basic.BasicMethod::BasicDeliver(dl) => dl
    _ => raise E2eError::Protocol("expected basic.deliver")
  }
  if deliver.consumer_tag != ctag {
    raise E2eError::Protocol("delivery arrived for a different consumer tag")
  }
  let header = d.next_content_header()
  let body = d.next_content_body(header.body_size.to_int())
  if body != message_body {
    raise E2eError::ContentMismatch(
      "delivered " +
      body.length().to_string() +
      " bytes, expected " +
      message_body.length().to_string(),
    )
  }
  // Ack the delivery, cancel the consumer, and close the connection
  // cleanly.
  send_basic_method(
    @basic.BasicMethod::BasicAck(@basic.BasicAck::{
      delivery_tag: deliver.delivery_tag,
      multiple: false,
    }),
  )
  send_basic_method(
    @basic.BasicMethod::BasicCancel(@basic.BasicCancel::{
      consumer_tag: ctag,
      nowait: false,
    }),
  )
  let _ = d.expect_basic("basic.cancel-ok", fn(m) {
    m is @basic.BasicMethod::BasicCancelOk(_)
  })
  let close = d.hs.close((200).to_uint16(), "Goodbye") catch {
    err => raise E2eError::Handshake(connection_error_msg(err))
  }
  send_connection_method(close)
  let m = d.next_connection_method()
  match m {
    @connection.ConnectionMethod::CloseOk(_) =>
      d.hs.receive(m) catch {
        err => raise E2eError::Handshake(connection_error_msg(err))
      }
    other =>
      raise E2eError::Handshake(
        "expected Connection.Close-Ok, got " + connection_method_name(other),
      )
  }
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
/// failure. The transport (bridge_js.mbt) only exists on the JS
/// backend, so run this with node after `moon build --target js`.
fn main {
  let d = Driver::new()
  run_session(d) catch {
    err => {
      println("e2e failed: " + e2e_error_msg(err))
      js_close()
      let _ = js_exit(1)
    }
  }
  js_close()
  let _ = js_exit(0)
}

///|
/// Human-readable name of a frame, for error messages.
fn frame_name(f : @frame.Frame) -> String {
  match f {
    @frame.Frame::Method(_, _) => "a method frame"
    @frame.Frame::Header(_, _) => "a content header frame"
    @frame.Frame::Body(_, _) => "a content body frame"
    @frame.Frame::Heartbeat => "a heartbeat frame"
  }
}

///|
/// Human-readable name of a connection method, for error messages.
fn connection_method_name(m : @connection.ConnectionMethod) -> String {
  match m {
    @connection.ConnectionMethod::Start(_) => "Connection.Start"
    @connection.ConnectionMethod::StartOk(_) => "Connection.Start-Ok"
    @connection.ConnectionMethod::Tune(_) => "Connection.Tune"
    @connection.ConnectionMethod::TuneOk(_) => "Connection.Tune-Ok"
    @connection.ConnectionMethod::Open(_) => "Connection.Open"
    @connection.ConnectionMethod::OpenOk(_) => "Connection.Open-Ok"
    @connection.ConnectionMethod::Close(_) => "Connection.Close"
    @connection.ConnectionMethod::CloseOk(_) => "Connection.Close-Ok"
  }
}

///|
/// Human-readable name of a channel method, for error messages.
fn channel_method_name(m : @channel.ChannelMethod) -> String {
  match m {
    @channel.ChannelMethod::ChannelOpen(_) => "channel.open"
    @channel.ChannelMethod::ChannelOpenOk(_) => "channel.open-ok"
    @channel.ChannelMethod::ChannelFlow(_) => "channel.flow"
    @channel.ChannelMethod::ChannelFlowOk(_) => "channel.flow-ok"
    @channel.ChannelMethod::ChannelClose(_) => "channel.close"
    @channel.ChannelMethod::ChannelCloseOk => "channel.close-ok"
    @channel.ChannelMethod::ExchangeDeclare(_) => "exchange.declare"
    @channel.ChannelMethod::ExchangeDeclareOk => "exchange.declare-ok"
    @channel.ChannelMethod::ExchangeDelete(_) => "exchange.delete"
    @channel.ChannelMethod::ExchangeDeleteOk => "exchange.delete-ok"
    @channel.ChannelMethod::ExchangeBind(_) => "exchange.bind"
    @channel.ChannelMethod::ExchangeBindOk => "exchange.bind-ok"
    @channel.ChannelMethod::ExchangeUnbind(_) => "exchange.unbind"
    @channel.ChannelMethod::ExchangeUnbindOk => "exchange.unbind-ok"
    @channel.ChannelMethod::QueueDeclare(_) => "queue.declare"
    @channel.ChannelMethod::QueueDeclareOk(_) => "queue.declare-ok"
    @channel.ChannelMethod::QueueBind(_) => "queue.bind"
    @channel.ChannelMethod::QueueBindOk => "queue.bind-ok"
    @channel.ChannelMethod::QueuePurge(_) => "queue.purge"
    @channel.ChannelMethod::QueuePurgeOk(_) => "queue.purge-ok"
    @channel.ChannelMethod::QueueDelete(_) => "queue.delete"
    @channel.ChannelMethod::QueueDeleteOk(_) => "queue.delete-ok"
    @channel.ChannelMethod::QueueUnbind(_) => "queue.unbind"
    @channel.ChannelMethod::QueueUnbindOk => "queue.unbind-ok"
  }
}

///|
/// Human-readable name of a basic method, for error messages.
fn basic_method_name(m : @basic.BasicMethod) -> String {
  match m {
    @basic.BasicMethod::BasicQos(_) => "basic.qos"
    @basic.BasicMethod::BasicQosOk(_) => "basic.qos-ok"
    @basic.BasicMethod::BasicConsume(_) => "basic.consume"
    @basic.BasicMethod::BasicConsumeOk(_) => "basic.consume-ok"
    @basic.BasicMethod::BasicCancel(_) => "basic.cancel"
    @basic.BasicMethod::BasicCancelOk(_) => "basic.cancel-ok"
    @basic.BasicMethod::BasicPublish(_) => "basic.publish"
    @basic.BasicMethod::BasicReturn(_) => "basic.return"
    @basic.BasicMethod::BasicDeliver(_) => "basic.deliver"
    @basic.BasicMethod::BasicGet(_) => "basic.get"
    @basic.BasicMethod::BasicGetOk(_) => "basic.get-ok"
    @basic.BasicMethod::BasicGetEmpty(_) => "basic.get-empty"
    @basic.BasicMethod::BasicAck(_) => "basic.ack"
    @basic.BasicMethod::BasicReject(_) => "basic.reject"
    @basic.BasicMethod::BasicRecoverAsync(_) => "basic.recover-async"
    @basic.BasicMethod::BasicRecover(_) => "basic.recover"
    @basic.BasicMethod::BasicRecoverOk(_) => "basic.recover-ok"
    @basic.BasicMethod::BasicNack(_) => "basic.nack"
  }
}

///|
/// Describe a frame decode failure, for error messages.
fn frame_error_msg(e : @frame.FrameError) -> String {
  match e {
    @frame.FrameError::Truncated => "truncated frame"
    @frame.FrameError::InvalidFrameEnd(b) =>
      "bad frame-end octet " + b.to_int().to_string()
    @frame.FrameError::BadFrameType(t) =>
      "bad frame type " + t.to_int().to_string()
    @frame.FrameError::InvalidHeartbeat => "invalid heartbeat frame"
  }
}

///|
/// Describe a connection decode failure, for error messages.
fn connection_error_msg(e : @connection.ConnectionError) -> String {
  match e {
    @connection.ConnectionError::Truncated => "connection method truncated"
    @connection.ConnectionError::InvalidUtf8 =>
      "connection method is not valid UTF-8"
    @connection.ConnectionError::ValueOutOfRange(m) =>
      "connection value out of range: " + m
    @connection.ConnectionError::InvalidTable(m) =>
      "connection field table invalid: " + m
    @connection.ConnectionError::UnexpectedClass(c) =>
      "unexpected class id " + c.to_int().to_string()
    @connection.ConnectionError::UnknownMethod(m) =>
      "unknown connection method " + m.to_int().to_string()
    @connection.ConnectionError::UnexpectedMethod(m) =>
      "unexpected connection method: " + m
  }
}

///|
/// Describe a channel decode failure, for error messages.
fn channel_error_msg(e : @channel.ChannelError) -> String {
  match e {
    @channel.ChannelError::Truncated => "channel method truncated"
    @channel.ChannelError::InvalidUtf8 => "channel method is not valid UTF-8"
    @channel.ChannelError::ValueOutOfRange(m) =>
      "channel value out of range: " + m
    @channel.ChannelError::InvalidTable(m) =>
      "channel field table invalid: " + m
    @channel.ChannelError::UnexpectedClass(c) =>
      "unexpected class id " + c.to_int().to_string()
    @channel.ChannelError::UnknownMethod(c, m) =>
      "unknown method " +
      m.to_int().to_string() +
      " in class " +
      c.to_int().to_string()
  }
}

///|
/// Describe a basic decode failure, for error messages.
fn basic_error_msg(e : @basic.BasicError) -> String {
  match e {
    @basic.BasicError::Truncated => "basic method truncated"
    @basic.BasicError::InvalidUtf8 => "basic method is not valid UTF-8"
    @basic.BasicError::ValueOutOfRange(m) => "basic value out of range: " + m
    @basic.BasicError::InvalidTable(m) => "basic field table invalid: " + m
    @basic.BasicError::UnexpectedClass(c) =>
      "unexpected class id " + c.to_int().to_string()
    @basic.BasicError::UnknownMethod(c, m) =>
      "unknown method " +
      m.to_int().to_string() +
      " in class " +
      c.to_int().to_string()
    @basic.BasicError::UnknownConsumer(m) => "unknown consumer " + m
    @basic.BasicError::ConsumerExists(m) => "consumer already exists: " + m
    @basic.BasicError::UnknownDelivery(t) =>
      "unknown delivery tag " + t.to_string()
    @basic.BasicError::DuplicateDelivery(t) =>
      "duplicate delivery tag " + t.to_string()
    @basic.BasicError::PrefetchExceeded => "prefetch window exceeded"
  }
}

///|
/// Describe an e2e failure, for the final error line.
fn e2e_error_msg(e : E2eError) -> String {
  match e {
    E2eError::ConnectFailed(m) => "connect failed: " + m
    E2eError::Handshake(m) => "handshake failed: " + m
    E2eError::Protocol(m) => "protocol: " + m
    E2eError::Timeout(m) => "timeout: " + m
    E2eError::ContentMismatch(m) => "content mismatch: " + m
  }
}
