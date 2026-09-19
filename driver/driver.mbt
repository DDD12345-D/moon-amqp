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
/// Session-driving primitives for MoonBit programs that speak AMQP
/// 0-9-1 through the library packages: connect and open a channel
/// against a real broker, then send and receive frames with the
/// delivery flow (deliver → header → body → ack) spelled out.
///
/// The byte transport lives in bridge_js.mbt (JS backend only) and
/// its panicking stubs in bridge_nonjs.mbt (every other backend); a
/// `Driver` compiles on any backend but only runs on the JS one.

///|
/// Anything that can go wrong while driving a session.
pub(all) suberror DriverError {
  /// The TCP connection to the broker could not be established.
  ConnectFailed(String)
  /// The connection handshake (Start/Tune/Open) went wrong.
  Handshake(String)
  /// A frame or method that does not fit the expected session.
  Protocol(String)
  /// The broker sent nothing while a frame was pending.
  Timeout(String)
}

///|
/// Receive-buffer budget: larger than any frame in a session that
/// negotiates the default frame-max.
let max_buf : Int = 1 << 20

///|
/// One socket-read chunk.
let chunk_size : Int = 8192

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
pub struct Driver {
  inbuf : Inbuf
  chunk : FixedArray[Byte]
  hs : @connection.Handshake
}

///|
pub fn Driver::new() -> Driver {
  {
    inbuf: Inbuf::new(),
    chunk: FixedArray::makei(chunk_size, fn(_) { b'\x00' }),
    hs: @connection.Handshake::new(),
  }
}

///|
/// Connect to `host:port`, run the handshake (protocol header,
/// Start/Start-Ok with PLAIN credentials, Tune/Tune-Ok with
/// heartbeats disabled for deterministic timing, Open/Open-Ok), and
/// leave the driver ready for channel traffic.
pub fn Driver::connect_open(
  self : Driver,
  host : String,
  port : Int,
  user : String,
  password : String,
) -> Unit raise DriverError {
  let _ = js_connect(host, port)
  if js_wait_connect(5000) != 1 {
    raise DriverError::ConnectFailed(
      "could not connect to " + host + ":" + port.to_string(),
    )
  }
  // Protocol header (§4.2.2).
  let w = @codec.Writer::new()
  @connection.Handshake::write_protocol_header(w)
  js_send(w.to_bytes())
  // Start / Start-Ok.
  let m = self.next_connection_method()
  self.hs.receive(m) catch {
    err => raise DriverError::Handshake(connection_error_msg(err))
  }
  let start_ok = self.hs.start_ok(
    client_properties(),
    "PLAIN",
    "\u0000" + user + "\u0000" + password,
    "en_US",
  ) catch {
    err => raise DriverError::Handshake(connection_error_msg(err))
  }
  send_connection_method(start_ok)
  // Tune / Tune-Ok: echo the server limits, but disable heartbeats
  // so the session timing is deterministic.
  let m = self.next_connection_method()
  match m {
    @connection.ConnectionMethod::Tune(t) => {
      self.hs.receive(m) catch {
        err => raise DriverError::Handshake(connection_error_msg(err))
      }
      let tune_ok = self.hs.tune_ok(t.channel_max, t.frame_max, (0).to_uint16()) catch {
        err => raise DriverError::Handshake(connection_error_msg(err))
      }
      send_connection_method(tune_ok)
    }
    other =>
      raise DriverError::Handshake(
        "expected Connection.Tune, got " + connection_method_name(other),
      )
  }
  // Open / Open-Ok.
  let open = self.hs.open("/") catch {
    err => raise DriverError::Handshake(connection_error_msg(err))
  }
  send_connection_method(open)
  let m = self.next_connection_method()
  match m {
    @connection.ConnectionMethod::OpenOk(_) =>
      self.hs.receive(m) catch {
        err => raise DriverError::Handshake(connection_error_msg(err))
      }
    other =>
      raise DriverError::Handshake(
        "expected Connection.Open-Ok, got " + connection_method_name(other),
      )
  }
}

///|
/// Open channel 1 and wait for channel.open-ok.
pub fn Driver::open_channel(self : Driver) -> Unit raise DriverError {
  send_channel_method(
    @channel.ChannelMethod::ChannelOpen(@channel.ChannelOpen::{
      reserved_1: "",
    }),
  )
  let _ = self.expect_channel("channel.open-ok", fn(m) {
    m is @channel.ChannelMethod::ChannelOpenOk(_)
  })
}

///|
/// Close the connection gracefully with the given reply code and
/// text, and wait for Connection.Close-Ok.
pub fn Driver::close(
  self : Driver,
  reply_code : UInt16,
  reply_text : String,
) -> Unit raise DriverError {
  let close = self.hs.close(reply_code, reply_text) catch {
    err => raise DriverError::Handshake(connection_error_msg(err))
  }
  send_connection_method(close)
  let m = self.next_connection_method()
  match m {
    @connection.ConnectionMethod::CloseOk(_) =>
      self.hs.receive(m) catch {
        err => raise DriverError::Handshake(connection_error_msg(err))
      }
    other =>
      raise DriverError::Handshake(
        "expected Connection.Close-Ok, got " + connection_method_name(other),
      )
  }
}

///|
/// Close the transport and exit the process with `code`.
pub fn terminate(code : Int) -> Int {
  js_close()
  js_exit(code)
}

///|
/// The broker address and credentials from the environment, with
/// localhost guest/guest defaults: `RABBITMQ_HOST` (default
/// 127.0.0.1), port 5672, `RABBITMQ_USER` and `RABBITMQ_PASSWORD`
/// (default guest/guest).
pub fn broker_from_env() -> (String, Int, String, String) {
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
  (host, 5672, user, password)
}

///|
/// Read one complete frame, buffering socket chunks until a full
/// frame has arrived.
pub fn Driver::next_frame(self : Driver) -> @frame.Frame raise DriverError {
  while true {
    if self.inbuf.len >= 8 {
      let bytes = self.inbuf.to_bytes()
      let size = be_u32(bytes, 3)
      if self.inbuf.len >= 8 + size {
        let frame = @frame.Frame::decode(@codec.Reader::new(bytes)) catch {
          @frame.FrameError::Truncated =>
            raise DriverError::Protocol("frame decode reported truncation")
          err => raise DriverError::Protocol(frame_error_msg(err))
        }
        self.inbuf.drain(8 + size)
        break frame
      }
    }
    if self.recv_chunk() == 0 {
      raise DriverError::Timeout(
        "no data from the broker while a frame was pending",
      )
    }
  } nobreak {
    raise DriverError::Protocol("frame loop exited without a frame")
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
pub fn send_frame(frame : @frame.Frame) -> Unit {
  let w = @codec.Writer::new()
  frame.encode(w)
  js_send(w.to_bytes())
}

///|
/// Send a connection-class method on channel 0.
pub fn send_connection_method(
  m : @connection.ConnectionMethod,
) -> Unit raise DriverError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise DriverError::Protocol(connection_error_msg(err))
  }
  send_frame(@frame.Frame::Method((0).to_uint16(), w.to_bytes()))
}

///|
/// Send a channel/exchange/queue-class method on channel 1.
pub fn send_channel_method(
  m : @channel.ChannelMethod,
) -> Unit raise DriverError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise DriverError::Protocol(channel_error_msg(err))
  }
  send_frame(@frame.Frame::Method((1).to_uint16(), w.to_bytes()))
}

///|
/// Send a basic-class method on channel 1.
pub fn send_basic_method(m : @basic.BasicMethod) -> Unit raise DriverError {
  let w = @codec.Writer::new()
  m.encode(w) catch {
    err => raise DriverError::Protocol(basic_error_msg(err))
  }
  send_frame(@frame.Frame::Method((1).to_uint16(), w.to_bytes()))
}

///|
/// Read the next connection-class method frame on channel 0,
/// skipping heartbeats.
pub fn Driver::next_connection_method(
  self : Driver,
) -> @connection.ConnectionMethod raise DriverError {
  while true {
    match self.next_frame() {
      @frame.Frame::Method(channel, payload) => {
        if channel != (0).to_uint16() {
          raise DriverError::Protocol(
            "unexpected method frame on channel " + channel.to_int().to_string(),
          )
        }
        let m = @connection.ConnectionMethod::decode(
          @codec.Reader::new(payload),
        ) catch {
          err => raise DriverError::Protocol(connection_error_msg(err))
        }
        break m
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise DriverError::Protocol(
          "expected a connection method frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise DriverError::Protocol("method loop exited without a method")
  }
}

///|
/// A method frame on channel 1: the channel/exchange/queue classes
/// or the basic class.
pub enum Ch1Method {
  Channel(@channel.ChannelMethod)
  Basic(@basic.BasicMethod)
}

///|
/// Decode a channel-1 method payload, dispatching on the class id:
/// 60 selects the basic-class decoder, anything else the
/// channel/exchange/queue one.
fn decode_ch1_method(payload : Bytes) -> Ch1Method raise DriverError {
  let class_id = (payload[0].to_int() << 8) | payload[1].to_int()
  if class_id == 60 {
    Ch1Method::Basic(
      @basic.BasicMethod::decode(@codec.Reader::new(payload)) catch {
        err => raise DriverError::Protocol(basic_error_msg(err))
      },
    )
  } else {
    Ch1Method::Channel(
      @channel.ChannelMethod::decode(@codec.Reader::new(payload)) catch {
        err => raise DriverError::Protocol(channel_error_msg(err))
      },
    )
  }
}

///|
/// Read the next method frame on channel 1, skipping heartbeats and
/// dispatching on the class id.
pub fn Driver::next_ch1_method(self : Driver) -> Ch1Method raise DriverError {
  while true {
    match self.next_frame() {
      @frame.Frame::Method(channel, payload) => {
        if channel != (1).to_uint16() {
          raise DriverError::Protocol(
            "unexpected method frame on channel " + channel.to_int().to_string(),
          )
        }
        break decode_ch1_method(payload)
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise DriverError::Protocol(
          "expected a channel method frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise DriverError::Protocol("method loop exited without a method")
  }
}

///|
/// Read one channel-1 method and require it to satisfy `p`; returns
/// the method, used to pin the expected reply of each request.
pub fn Driver::expect_channel(
  self : Driver,
  what : String,
  p : (@channel.ChannelMethod) -> Bool,
) -> @channel.ChannelMethod raise DriverError {
  match self.next_ch1_method() {
    Ch1Method::Channel(m) =>
      if p(m) {
        m
      } else {
        raise DriverError::Protocol(
          "expected " + what + ", got " + channel_method_name(m),
        )
      }
    Ch1Method::Basic(m) =>
      raise DriverError::Protocol(
        "expected " + what + ", got " + basic_method_name(m),
      )
  }
}

///|
/// Read one basic-class method on channel 1 and require it to
/// satisfy `p`; returns the method.
pub fn Driver::expect_basic(
  self : Driver,
  what : String,
  p : (@basic.BasicMethod) -> Bool,
) -> @basic.BasicMethod raise DriverError {
  match self.next_ch1_method() {
    Ch1Method::Basic(m) =>
      if p(m) {
        m
      } else {
        raise DriverError::Protocol(
          "expected " + what + ", got " + basic_method_name(m),
        )
      }
    Ch1Method::Channel(m) =>
      raise DriverError::Protocol(
        "expected " + what + ", got " + channel_method_name(m),
      )
  }
}

///|
/// Read the content header that follows a delivery, skipping
/// heartbeats.
pub fn Driver::next_content_header(
  self : Driver,
) -> @basic.ContentHeader raise DriverError {
  while true {
    match self.next_frame() {
      @frame.Frame::Header(channel, payload) => {
        if channel != (1).to_uint16() {
          raise DriverError::Protocol(
            "unexpected header frame on channel " + channel.to_int().to_string(),
          )
        }
        let h = @basic.ContentHeader::decode(@codec.Reader::new(payload)) catch {
          err => raise DriverError::Protocol(basic_error_msg(err))
        }
        break h
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise DriverError::Protocol(
          "expected a content header frame, got " + frame_name(other),
        )
    }
  } nobreak {
    raise DriverError::Protocol("header loop exited without a header")
  }
}

///|
/// Read `total` bytes of content body, split over as many body
/// frames as the broker sends.
pub fn Driver::next_content_body(
  self : Driver,
  total : Int,
) -> Bytes raise DriverError {
  let body = FixedArray::makei(total, fn(_) { b'\x00' })
  let mut got = 0
  while got < total {
    match self.next_frame() {
      @frame.Frame::Body(channel, payload) => {
        if channel != (1).to_uint16() {
          raise DriverError::Protocol(
            "unexpected body frame on channel " + channel.to_int().to_string(),
          )
        }
        if got + payload.length() > total {
          raise DriverError::Protocol("content body exceeds the announced size")
        }
        for i in 0..<payload.length() {
          body[got + i] = payload[i]
        }
        got = got + payload.length()
      }
      @frame.Frame::Heartbeat => continue
      other =>
        raise DriverError::Protocol(
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
/// Human-readable name of a frame, for error messages.
pub fn frame_name(f : @frame.Frame) -> String {
  match f {
    @frame.Frame::Method(_, _) => "a method frame"
    @frame.Frame::Header(_, _) => "a content header frame"
    @frame.Frame::Body(_, _) => "a content body frame"
    @frame.Frame::Heartbeat => "a heartbeat frame"
  }
}

///|
/// Human-readable name of a connection method, for error messages.
pub fn connection_method_name(m : @connection.ConnectionMethod) -> String {
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
pub fn channel_method_name(m : @channel.ChannelMethod) -> String {
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
pub fn basic_method_name(m : @basic.BasicMethod) -> String {
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
pub fn frame_error_msg(e : @frame.FrameError) -> String {
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
pub fn connection_error_msg(e : @connection.ConnectionError) -> String {
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
pub fn channel_error_msg(e : @channel.ChannelError) -> String {
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
pub fn basic_error_msg(e : @basic.BasicError) -> String {
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
/// Describe a driver failure, for the final error line.
pub fn driver_error_msg(e : DriverError) -> String {
  match e {
    DriverError::ConnectFailed(m) => "connect failed: " + m
    DriverError::Handshake(m) => "handshake failed: " + m
    DriverError::Protocol(m) => "protocol: " + m
    DriverError::Timeout(m) => "timeout: " + m
  }
}
