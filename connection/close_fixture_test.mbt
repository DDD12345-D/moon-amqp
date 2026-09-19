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
/// Connection.Close payloads recorded from a real RabbitMQ 4.3.6
/// broker (Erlang/OTP 27.3.4.17) on 2026-09-19 by connecting the
/// amqplib 2.0.1 Node.js client through a TCP proxy. Each literal is
/// the payload of one method frame: everything after the 7-byte
/// frame header and before the frame-end byte.

///|
/// The client-initiated shutdown amqplib sends for `conn.close()`:
/// reply-code 200, reply-text "Cheers, thanks", class and method ids
/// 0 (a connection-level close, §4.3.12.6/7).
let client_close : Bytes = b"\x00\x0a\x00\x32\x00\xc8\x0e\x43\x68\x65\x65\x72\x73\x2c\x20\x74\x68\x61\x6e\x6b\x73\x00\x00\x00\x00"

///|
/// RabbitMQ's Connection.Close-Ok: the class and method ids only.
let server_close_ok : Bytes = b"\x00\x0a\x00\x33"

///|
/// The server-initiated shutdown RabbitMQ sends on
/// `rabbitmqctl close_all_user_connections guest recorded-batch7`:
/// reply-code 320 (CONNECTION-FORCED), the explanation as
/// reply-text, class and method ids 0.
let server_close : Bytes = b"\x00\x0a\x00\x32\x01\x40\x23\x43\x4f\x4e\x4e\x45\x43\x54\x49\x4f\x4e\x5f\x46\x4f\x52\x43\x45\x44\x20\x2d\x20\x72\x65\x63\x6f\x72\x64\x65\x64\x2d\x62\x61\x74\x63\x68\x37\x00\x00\x00\x00"

///|
/// The client's Connection.Close-Ok answering the server shutdown.
let client_close_ok : Bytes = b"\x00\x0a\x00\x33"

///|
/// The real client shutdown decodes to the expected Close and
/// re-encodes byte-identically.
test "real client Close decodes and re-encodes identically" {
  @test.assert_eq(
    fixture_dec(client_close),
    ConnectionMethod::Close(Close::{
      reply_code: (200).to_uint16(),
      reply_text: "Cheers, thanks",
      class_id: (0).to_uint16(),
      method_id: (0).to_uint16(),
    }),
  )
  @test.assert_eq(fixture_enc(fixture_dec(client_close)), client_close)
}

///|
/// The real server shutdown (an administrative force-close) decodes
/// to the expected Close and re-encodes byte-identically.
test "real server Close decodes and re-encodes identically" {
  @test.assert_eq(
    fixture_dec(server_close),
    ConnectionMethod::Close(Close::{
      reply_code: (320).to_uint16(),
      reply_text: "CONNECTION_FORCED - recorded-batch7",
      class_id: (0).to_uint16(),
      method_id: (0).to_uint16(),
    }),
  )
  @test.assert_eq(fixture_enc(fixture_dec(server_close)), server_close)
}

///|
/// The real Close-Ok (identical in both directions) decodes and
/// re-encodes byte-identically.
test "real Close-Ok decodes and re-encodes identically" {
  @test.assert_eq(fixture_dec(server_close_ok), ConnectionMethod::CloseOk(()))
  @test.assert_eq(fixture_enc(ConnectionMethod::CloseOk(())), server_close_ok)
}

///|
/// The recorded server shutdown drives the state machine to Closed
/// and is answered with the recorded client Close-Ok.
test "recorded server shutdown drives the state machine to Closed" {
  let h = open_handshake()
  h.receive(fixture_dec(server_close))
  @test.assert_eq(h.state(), HandshakeState::Closed)
  match h.close_reason() {
    Some(c) => {
      @test.assert_eq(c.reply_code, (320).to_uint16())
      @test.assert_eq(c.reply_text, "CONNECTION_FORCED - recorded-batch7")
      @test.assert_eq(c.class_id, (0).to_uint16())
      @test.assert_eq(c.method_id, (0).to_uint16())
    }
    None => @test.assert_eq(0, 1)
  }
  let close_ok = h.close_ok()
  @test.assert_eq(fixture_enc(close_ok), client_close_ok)
}

///|
/// The recorded client shutdown: the state machine's Close matches
/// the recorded amqplib bytes, and the server's Close-Ok closes the
/// connection.
test "recorded client shutdown closes the connection" {
  let h = open_handshake()
  let close = h.close((200).to_uint16(), "Cheers, thanks")
  @test.assert_eq(fixture_enc(close), client_close)
  h.receive(fixture_dec(server_close_ok))
  @test.assert_eq(h.state(), HandshakeState::Closed)
  @test.assert_eq(h.close_reason(), None)
}

///|
/// The negotiated heartbeat of the recorded idle session was 4
/// seconds (Tune-Ok), and RabbitMQ sent heartbeat frames every half
/// interval on the dot; the Heartbeat bookkeeping with a 4-second
/// interval agrees with that cadence on both sides of the recorded
/// timeline.
test "recorded 4-second heartbeat cadence" {
  // The idle session: opened at t=0, RabbitMQ heartbeated at 2, 4
  // and 6 seconds (every interval/2), the client closed at t=8.
  let h = Heartbeat::new((4).to_uint16(), 0)
  @test.assert_eq(h.poll(1), HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(2), HeartbeatAction::Send)
  h.note_sent(2)
  h.note_received(2)
  @test.assert_eq(h.poll(3), HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(4), HeartbeatAction::Send)
  h.note_sent(4)
  h.note_received(4)
  @test.assert_eq(h.poll(5), HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(6), HeartbeatAction::Send)
  h.note_sent(6)
  h.note_received(6)
  @test.assert_eq(h.poll(7), HeartbeatAction::Nothing)
  // Two seconds since the last send: due again; the receive clock is
  // two seconds old, nowhere near the two-interval (8s) timeout.
  @test.assert_eq(h.poll(8), HeartbeatAction::Send)
}
