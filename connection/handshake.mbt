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
/// Handshake phase (§4.2.2): the states a connecting client passes
/// through until the connection is open, and the terminal `Closed`
/// state after either peer ends the connection (§4.2.3).
pub(all) enum HandshakeState {
  /// Waiting for the server's Connection.Start.
  AwaitStart
  /// Start received; waiting for Connection.Tune.
  AwaitTune
  /// Tune received; waiting for Connection.Open-Ok.
  AwaitOpenOk
  /// Open-Ok received; the connection is open.
  Open
  /// A Connection.Close was exchanged; the connection is over.
  /// Every method is rejected from this terminal state.
  Closed
} derive(Eq, @debug.Debug)

///|
/// Client-side handshake driver (§4.2.2). Feed it the methods the
/// server sends with `receive`, and build the client's replies with
/// `start_ok`, `tune_ok`, `open`, `close` and `close_ok`. Every call
/// validates that the current phase allows it, so programming errors
/// surface as `ConnectionError::UnexpectedMethod` instead of protocol
/// violations.
pub struct Handshake {
  mut state : HandshakeState
  /// The client has sent Connection.Close and awaits Close-Ok.
  mut pending_close : Bool
  /// The server's Connection.Close, when the server closed first.
  mut close_reason : Close?
}

///|
/// A fresh handshake, waiting for the server's Connection.Start.
pub fn Handshake::new() -> Handshake {
  {
    state: HandshakeState::AwaitStart,
    pending_close: false,
    close_reason: None,
  }
}

///|
/// The current phase.
pub fn Handshake::state(self : Handshake) -> HandshakeState {
  self.state
}

///|
/// Write the protocol header (§4.2.2): `"AMQP"`, protocol id `0`, then
/// `0x00 0x09 0x01` (AMQP 0-9-1). This is the first thing a client
/// sends on a new connection.
pub fn Handshake::write_protocol_header(w : @codec.Writer) -> Unit {
  w.write_bytes(b"AMQP\x00\x00\x09\x01")
}

///|
/// Feed a method received from the server; advances the phase when the
/// method is the one the current phase expects, and raises
/// `ConnectionError::UnexpectedMethod` otherwise.
///
/// A Connection.Close from the server is accepted from any phase
/// (the server may refuse the handshake) and ends the connection: the
/// state becomes `Closed`, the reason is recorded in `close_reason`,
/// and the client must answer with `close_ok`. A Connection.Close-Ok
/// ends the connection when the client itself closed first. In the
/// terminal `Closed` state every method is rejected.
pub fn Handshake::receive(
  self : Handshake,
  m : ConnectionMethod,
) -> Unit raise ConnectionError {
  match (self.state, m) {
    (HandshakeState::Closed, _) =>
      raise ConnectionError::UnexpectedMethod(
        "got " + method_name(m) + " on a closed connection",
      )
    (_, ConnectionMethod::Close(c)) => {
      self.state = HandshakeState::Closed
      self.close_reason = Some(c)
    }
    (HandshakeState::AwaitStart, ConnectionMethod::Start(_)) =>
      self.state = HandshakeState::AwaitTune
    (HandshakeState::AwaitTune, ConnectionMethod::Tune(_)) =>
      self.state = HandshakeState::AwaitOpenOk
    (HandshakeState::AwaitOpenOk, ConnectionMethod::OpenOk(_)) =>
      self.state = HandshakeState::Open
    (HandshakeState::Open, ConnectionMethod::CloseOk(_)) => {
      if !self.pending_close {
        raise ConnectionError::UnexpectedMethod(
          "got Connection.Close-Ok without having sent Connection.Close",
        )
      }
      self.state = HandshakeState::Closed
    }
    (state, other) =>
      raise ConnectionError::UnexpectedMethod(
        "got " + method_name(other) + " while " + state_name(state),
      )
  }
}

///|
/// Build the client's Start-Ok reply. Raises unless Connection.Start
/// has been received and Tune has not yet arrived.
pub fn Handshake::start_ok(
  self : Handshake,
  client_properties : @table.FieldTable,
  mechanism : String,
  response : String,
  locale : String,
) -> ConnectionMethod raise ConnectionError {
  if self.state != HandshakeState::AwaitTune {
    raise ConnectionError::UnexpectedMethod(
      "Start-Ok is sent after Connection.Start has been received",
    )
  }
  ConnectionMethod::StartOk(StartOk::{
    client_properties,
    mechanism,
    response,
    locale,
  })
}

///|
/// Build the client's Tune-Ok reply. Raises unless Connection.Tune
/// has been received.
pub fn Handshake::tune_ok(
  self : Handshake,
  channel_max : UInt16,
  frame_max : UInt,
  heartbeat : UInt16,
) -> ConnectionMethod raise ConnectionError {
  if self.state != HandshakeState::AwaitOpenOk {
    raise ConnectionError::UnexpectedMethod(
      "Tune-Ok is sent after Connection.Tune has been received",
    )
  }
  ConnectionMethod::TuneOk(TuneOk::{ channel_max, frame_max, heartbeat, })
}

///|
/// Build the client's Open request, with the reserved fields fixed to
/// the spec values (§4.3.11.5/6). Raises unless Connection.Tune has
/// been received.
pub fn Handshake::open(
  self : Handshake,
  virtual_host : String,
) -> ConnectionMethod raise ConnectionError {
  if self.state != HandshakeState::AwaitOpenOk {
    raise ConnectionError::UnexpectedMethod(
      "Open is sent after Connection.Tune has been received",
    )
  }
  ConnectionMethod::Open(Open::{
    virtual_host,
    reserved_1: "",
    reserved_2: false,
  })
}

///|
/// Build the client's Close request, the normal shutdown path: the
/// class and method ids are 0, as the close is not provoked by a
/// method failure (§4.3.12.6/7). Raises unless the connection is
/// open. Once this Close is sent, `receive` accepts the server's
/// Close-Ok and moves to `Closed`.
pub fn Handshake::close(
  self : Handshake,
  reply_code : UInt16,
  reply_text : String,
) -> ConnectionMethod raise ConnectionError {
  if self.state != HandshakeState::Open {
    raise ConnectionError::UnexpectedMethod(
      "Connection.Close is sent on an open connection",
    )
  }
  self.pending_close = true
  ConnectionMethod::Close(Close::{
    reply_code,
    reply_text,
    class_id: (0).to_uint16(),
    method_id: (0).to_uint16(),
  })
}

///|
/// Build the client's Close-Ok, which answers a Connection.Close from
/// the server (§4.2.3). Raises unless the server has closed the
/// connection first (`receive` recorded a `close_reason`).
pub fn Handshake::close_ok(
  self : Handshake,
) -> ConnectionMethod raise ConnectionError {
  if self.state != HandshakeState::Closed || self.close_reason == None {
    raise ConnectionError::UnexpectedMethod(
      "Connection.Close-Ok answers a Connection.Close from the server",
    )
  }
  ConnectionMethod::CloseOk(())
}

///|
/// The server's Connection.Close, when the server ended the connection
/// (e.g. a refused handshake or an administrative shutdown); `None`
/// when the client closed first or the connection is still open.
pub fn Handshake::close_reason(self : Handshake) -> Close? {
  self.close_reason
}

///|
/// Protocol name of a method, for error messages.
fn method_name(m : ConnectionMethod) -> String {
  match m {
    ConnectionMethod::Start(_) => "Connection.Start"
    ConnectionMethod::StartOk(_) => "Connection.Start-Ok"
    ConnectionMethod::Tune(_) => "Connection.Tune"
    ConnectionMethod::TuneOk(_) => "Connection.Tune-Ok"
    ConnectionMethod::Open(_) => "Connection.Open"
    ConnectionMethod::OpenOk(_) => "Connection.Open-Ok"
    ConnectionMethod::Close(_) => "Connection.Close"
    ConnectionMethod::CloseOk(_) => "Connection.Close-Ok"
  }
}

///|
/// Human-readable phase name, for error messages.
fn state_name(s : HandshakeState) -> String {
  match s {
    HandshakeState::AwaitStart => "awaiting Connection.Start"
    HandshakeState::AwaitTune => "awaiting Connection.Tune"
    HandshakeState::AwaitOpenOk => "awaiting Connection.Open-Ok"
    HandshakeState::Open => "open"
    HandshakeState::Closed => "closed"
  }
}
