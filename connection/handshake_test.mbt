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
/// The protocol header is exactly the eight §4.2.2 bytes.
test "protocol header golden" {
  let w = @codec.Writer::new()
  Handshake::write_protocol_header(w)
  @test.assert_eq(w.to_bytes(), b"AMQP\x00\x00\x09\x01")
}

///|
/// The full happy path: Start, Start-Ok, Tune, Tune-Ok, Open, Open-Ok
/// advance the phases in order and the builders return the right
/// methods.
test "handshake happy path" {
  let h = Handshake::new()
  @test.assert_eq(h.state(), HandshakeState::AwaitStart)
  h.receive(
    ConnectionMethod::Start(Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  @test.assert_eq(h.state(), HandshakeState::AwaitTune)

  let client = @table.FieldTable::new()
  client.set("product", @table.FieldValue::ShortStr("moon-amqp"))
  let start_ok = h.start_ok(client, "PLAIN", "guest", "en_US")
  match start_ok {
    ConnectionMethod::StartOk(s) => {
      @test.assert_eq(s.mechanism, "PLAIN")
      @test.assert_eq(s.response, "guest")
      @test.assert_eq(s.locale, "en_US")
      @test.assert_eq(
        s.client_properties.get("product"),
        Some(@table.FieldValue::ShortStr("moon-amqp")),
      )
    }
    _ => @test.assert_eq(0, 1)
  }

  h.receive(
    ConnectionMethod::Tune(Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(h.state(), HandshakeState::AwaitOpenOk)

  let tune_ok = h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16())
  @test.assert_eq(
    tune_ok,
    ConnectionMethod::TuneOk(TuneOk::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  let open = h.open("/")
  @test.assert_eq(
    open,
    ConnectionMethod::Open(Open::{
      virtual_host: "/",
      reserved_1: "",
      reserved_2: false,
    }),
  )

  h.receive(ConnectionMethod::OpenOk(OpenOk::{ reserved_1: "", }))
  @test.assert_eq(h.state(), HandshakeState::Open)
}

///|
/// A server method out of order is an `UnexpectedMethod` error and
/// leaves the phase unchanged.
test "unexpected server methods raise" {
  let h = Handshake::new()
  let raised = try {
    h.receive(
      ConnectionMethod::Tune(Tune::{
        channel_max: (2047).to_uint16(),
        frame_max: 131072,
        heartbeat: (60).to_uint16(),
      }),
    )
    false
  } catch {
    ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  @test.assert_eq(h.state(), HandshakeState::AwaitStart)

  h.receive(
    ConnectionMethod::Start(Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  let raised = try {
    h.receive(
      ConnectionMethod::Start(Start::{
        version_major: b'\x00',
        version_minor: b'\x09',
        server_properties: @table.FieldTable::new(),
        mechanisms: "PLAIN",
        locales: "en_US",
      }),
    )
    false
  } catch {
    ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  @test.assert_eq(h.state(), HandshakeState::AwaitTune)
}

///|
/// The reply builders refuse to run before the server method they
/// answer has arrived.
test "builders require their phase" {
  let h = Handshake::new()
  let raised = try {
    let _ = h.start_ok(@table.FieldTable::new(), "PLAIN", "", "en_US")
    false
  } catch {
    ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)

  h.receive(
    ConnectionMethod::Start(Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  let raised = try {
    let _ = h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16())
    false
  } catch {
    ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.open("/")
    false
  } catch {
    ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
