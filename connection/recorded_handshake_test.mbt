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
/// Method payloads recorded from a real RabbitMQ 4.3.6 broker (Erlang/OTP
/// 27.3.4.17) on 2026-09-18 by connecting the amqplib Node.js client
/// through a TCP proxy. Each literal is the payload of one method frame:
/// everything after the 7-byte frame header and before the frame-end byte.
let server_start : Bytes = b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x01\xdf\x0c\x63\x61\x70\x61\x62\x69\x6c\x69\x74\x69\x65\x73\x46\x00\x00\x00\xc7\x12\x70\x75\x62\x6c\x69\x73\x68\x65\x72\x5f\x63\x6f\x6e\x66\x69\x72\x6d\x73\x74\x01\x1a\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x62\x69\x6e\x64\x69\x6e\x67\x73\x74\x01\x0a\x62\x61\x73\x69\x63\x2e\x6e\x61\x63\x6b\x74\x01\x16\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x63\x61\x6e\x63\x65\x6c\x5f\x6e\x6f\x74\x69\x66\x79\x74\x01\x12\x63\x6f\x6e\x6e\x65\x63\x74\x69\x6f\x6e\x2e\x62\x6c\x6f\x63\x6b\x65\x64\x74\x01\x13\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x70\x72\x69\x6f\x72\x69\x74\x69\x65\x73\x74\x01\x1c\x61\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x69\x6f\x6e\x5f\x66\x61\x69\x6c\x75\x72\x65\x5f\x63\x6c\x6f\x73\x65\x74\x01\x10\x70\x65\x72\x5f\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x71\x6f\x73\x74\x01\x0f\x64\x69\x72\x65\x63\x74\x5f\x72\x65\x70\x6c\x79\x5f\x74\x6f\x74\x01\x0c\x63\x6c\x75\x73\x74\x65\x72\x5f\x6e\x61\x6d\x65\x53\x00\x00\x00\x16\x72\x61\x62\x62\x69\x74\x40\x44\x45\x53\x4b\x54\x4f\x50\x2d\x33\x44\x33\x50\x56\x32\x4b\x09\x63\x6f\x70\x79\x72\x69\x67\x68\x74\x53\x00\x00\x00\x3c\x43\x6f\x70\x79\x72\x69\x67\x68\x74\x20\x28\x63\x29\x20\x32\x30\x30\x37\x2d\x32\x30\x32\x36\x20\x42\x72\x6f\x61\x64\x63\x6f\x6d\x20\x49\x6e\x63\x20\x61\x6e\x64\x2f\x6f\x72\x20\x69\x74\x73\x20\x73\x75\x62\x73\x69\x64\x69\x61\x72\x69\x65\x73\x0b\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69\x6f\x6e\x53\x00\x00\x00\x39\x4c\x69\x63\x65\x6e\x73\x65\x64\x20\x75\x6e\x64\x65\x72\x20\x74\x68\x65\x20\x4d\x50\x4c\x20\x32\x2e\x30\x2e\x20\x57\x65\x62\x73\x69\x74\x65\x3a\x20\x68\x74\x74\x70\x73\x3a\x2f\x2f\x72\x61\x62\x62\x69\x74\x6d\x71\x2e\x63\x6f\x6d\x08\x70\x6c\x61\x74\x66\x6f\x72\x6d\x53\x00\x00\x00\x14\x45\x72\x6c\x61\x6e\x67\x2f\x4f\x54\x50\x20\x32\x37\x2e\x33\x2e\x34\x2e\x31\x37\x07\x70\x72\x6f\x64\x75\x63\x74\x53\x00\x00\x00\x08\x52\x61\x62\x62\x69\x74\x4d\x51\x07\x76\x65\x72\x73\x69\x6f\x6e\x53\x00\x00\x00\x05\x34\x2e\x33\x2e\x36\x00\x00\x00\x18\x41\x4d\x51\x50\x4c\x41\x49\x4e\x20\x41\x4e\x4f\x4e\x59\x4d\x4f\x55\x53\x20\x50\x4c\x41\x49\x4e\x00\x00\x00\x05\x65\x6e\x5f\x55\x53"

///|
/// Connection.Start-Ok as sent by amqplib. Its client-properties table
/// arrives in insertion order (not sorted), unlike RabbitMQ's.
let client_start_ok : Bytes = b"\x00\x0a\x00\x0b\x00\x00\x01\x17\x07\x70\x72\x6f\x64\x75\x63\x74\x53\x00\x00\x00\x07\x61\x6d\x71\x70\x6c\x69\x62\x07\x76\x65\x72\x73\x69\x6f\x6e\x53\x00\x00\x00\x05\x32\x2e\x30\x2e\x31\x08\x70\x6c\x61\x74\x66\x6f\x72\x6d\x53\x00\x00\x00\x10\x4e\x6f\x64\x65\x2e\x4a\x53\x20\x76\x32\x34\x2e\x32\x31\x2e\x30\x0b\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69\x6f\x6e\x53\x00\x00\x00\x24\x68\x74\x74\x70\x73\x3a\x2f\x2f\x61\x6d\x71\x70\x2d\x6e\x6f\x64\x65\x2e\x67\x69\x74\x68\x75\x62\x2e\x69\x6f\x2f\x61\x6d\x71\x70\x6c\x69\x62\x2f\x0c\x63\x61\x70\x61\x62\x69\x6c\x69\x74\x69\x65\x73\x46\x00\x00\x00\x8c\x12\x70\x75\x62\x6c\x69\x73\x68\x65\x72\x5f\x63\x6f\x6e\x66\x69\x72\x6d\x73\x74\x01\x1a\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x62\x69\x6e\x64\x69\x6e\x67\x73\x74\x01\x0a\x62\x61\x73\x69\x63\x2e\x6e\x61\x63\x6b\x74\x01\x16\x63\x6f\x6e\x73\x75\x6d\x65\x72\x5f\x63\x61\x6e\x63\x65\x6c\x5f\x6e\x6f\x74\x69\x66\x79\x74\x01\x12\x63\x6f\x6e\x6e\x65\x63\x74\x69\x6f\x6e\x2e\x62\x6c\x6f\x63\x6b\x65\x64\x74\x01\x1c\x61\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x69\x6f\x6e\x5f\x66\x61\x69\x6c\x75\x72\x65\x5f\x63\x6c\x6f\x73\x65\x74\x01\x05\x50\x4c\x41\x49\x4e\x00\x00\x00\x0c\x00\x67\x75\x65\x73\x74\x00\x67\x75\x65\x73\x74\x05\x65\x6e\x5f\x55\x53"

///|
/// Connection.Tune as sent by RabbitMQ: channel-max 2047, frame-max
/// 131072, heartbeat 60.
let server_tune : Bytes = b"\x00\x0a\x00\x1e\x07\xff\x00\x02\x00\x00\x00\x3c"

///|
/// Connection.Tune-Ok as sent by amqplib, accepting every server limit.
let client_tune_ok : Bytes = b"\x00\x0a\x00\x1f\x07\xff\x00\x02\x00\x00\x00\x3c"

///|
/// Connection.Open for the default virtual host, as sent by amqplib.
let client_open : Bytes = b"\x00\x0a\x00\x28\x01\x2f\x00\x00"

///|
/// Connection.Open-Ok as sent by RabbitMQ.
let server_open_ok : Bytes = b"\x00\x0a\x00\x29\x00"

///|
/// The protocol header the real client sent to the real server.
let recorded_protocol_header : Bytes = b"\x41\x4d\x51\x50\x00\x00\x09\x01"

///|
/// Decode one method payload.
fn fixture_dec(payload : Bytes) -> ConnectionMethod raise ConnectionError {
  ConnectionMethod::decode(@codec.Reader::new(payload))
}

///|
/// Encode one method to its payload bytes.
fn fixture_enc(m : ConnectionMethod) -> Bytes raise ConnectionError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// The PLAIN response amqplib sent: `\0guest\0guest`.
fn guest_response() -> String {
  @utf8.decode(b"\x00guest\x00guest") catch {
    _ => "unreachable"
  }
}

///|
/// Whether two field values are semantically equal, ignoring entry order
/// inside nested tables: they are equal when their canonical (sorted)
/// encodings are byte-identical.
fn same_value(a : @table.FieldValue, b : @table.FieldValue) -> Bool {
  let wa = @codec.Writer::new()
  let wb = @codec.Writer::new()
  let ok = try {
    a.encode(wa)
    b.encode(wb)
    true
  } catch {
    _ => false
  }
  ok && wa.to_bytes() == wb.to_bytes()
}

///|
/// The real RabbitMQ Connection.Start decodes to the advertised server
/// properties, mechanisms and locales.
test "real RabbitMQ Start decodes" {
  match fixture_dec(server_start) {
    ConnectionMethod::Start(s) => {
      @test.assert_eq(s.version_major, b'\x00')
      @test.assert_eq(s.version_minor, b'\x09')
      @test.assert_eq(s.mechanisms, "AMQPLAIN ANONYMOUS PLAIN")
      @test.assert_eq(s.locales, "en_US")
      @test.assert_eq(
        s.server_properties.get("product"),
        Some(@table.FieldValue::LongStr("RabbitMQ")),
      )
      @test.assert_eq(
        s.server_properties.get("version"),
        Some(@table.FieldValue::LongStr("4.3.6")),
      )
      @test.assert_eq(
        s.server_properties.get("platform"),
        Some(@table.FieldValue::LongStr("Erlang/OTP 27.3.4.17")),
      )
      @test.assert_eq(
        s.server_properties.get("cluster_name"),
        Some(@table.FieldValue::LongStr("rabbit@DESKTOP-3D3PV2K")),
      )
    }
    _ => @test.assert_eq(0, 1)
  }
}

///|
/// Real server methods without field tables re-encode byte-identically.
/// Connection.Start carries a nested capabilities table that RabbitMQ
/// writes unsorted; this encoder sorts every table, so that re-encoding
/// is canonical but not byte-identical — it is, however, stable.
test "real server method bytes re-encode identically" {
  for payload in [server_tune, server_open_ok] {
    @test.assert_eq(fixture_enc(fixture_dec(payload)), payload)
  }
  let once = fixture_enc(fixture_dec(server_start))
  @test.assert_eq(fixture_enc(fixture_dec(once)), once)
}

///|
/// The real client methods without field tables (Tune-Ok, Open) also
/// re-encode byte-identically.
test "real client Tune-Ok and Open re-encode identically" {
  @test.assert_eq(fixture_enc(fixture_dec(client_tune_ok)), client_tune_ok)
  @test.assert_eq(fixture_enc(fixture_dec(client_open)), client_open)
}

///|
/// The real client Start-Ok decodes to the chosen SASL credentials.
/// amqplib writes its client-properties table in insertion order, so
/// re-encoding sorts the keys and the bytes differ; the sorted encoding
/// is stable and decodes back to the same key-value pairs.
test "real client Start-Ok decodes and roundtrips" {
  match fixture_dec(client_start_ok) {
    ConnectionMethod::StartOk(s) => {
      @test.assert_eq(s.mechanism, "PLAIN")
      @test.assert_eq(s.response, guest_response())
      @test.assert_eq(s.locale, "en_US")
      @test.assert_eq(
        s.client_properties.get("product"),
        Some(@table.FieldValue::LongStr("amqplib")),
      )
      @test.assert_eq(
        s.client_properties.get("version"),
        Some(@table.FieldValue::LongStr("2.0.1")),
      )
      @test.assert_eq(
        s.client_properties.get("platform"),
        Some(@table.FieldValue::LongStr("Node.JS v24.21.0")),
      )
      let once = fixture_enc(ConnectionMethod::StartOk(s))
      @test.assert_eq(fixture_enc(fixture_dec(once)), once)
      match fixture_dec(once) {
        ConnectionMethod::StartOk(s2) =>
          for (k, v) in s.client_properties.iter() {
            match s2.client_properties.get(k) {
              Some(v2) => @test.assert_eq(same_value(v, v2), true)
              None => @test.assert_eq(0, 1)
            }
          }
        _ => @test.assert_eq(0, 1)
      }
    }
    _ => @test.assert_eq(0, 1)
  }
}

///|
/// The six recorded methods drive the handshake to `Open`, and the
/// negotiated values match the real Tune: channel-max 2047, frame-max
/// 131072, heartbeat 60.
test "recorded handshake drives the state machine to Open" {
  let h = Handshake::new()
  let w = @codec.Writer::new()
  Handshake::write_protocol_header(w)
  @test.assert_eq(w.to_bytes(), recorded_protocol_header)

  h.receive(fixture_dec(server_start))
  @test.assert_eq(h.state(), HandshakeState::AwaitTune)
  match
    h.start_ok(@table.FieldTable::new(), "PLAIN", guest_response(), "en_US") {
    ConnectionMethod::StartOk(s) => {
      @test.assert_eq(s.mechanism, "PLAIN")
      @test.assert_eq(s.response, guest_response())
    }
    _ => @test.assert_eq(0, 1)
  }

  h.receive(fixture_dec(server_tune))
  @test.assert_eq(h.state(), HandshakeState::AwaitOpenOk)
  @test.assert_eq(
    fixture_dec(server_tune),
    ConnectionMethod::Tune(Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(
    h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16()),
    ConnectionMethod::TuneOk(TuneOk::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(
    h.open("/"),
    ConnectionMethod::Open(Open::{
      virtual_host: "/",
      reserved_1: "",
      reserved_2: false,
    }),
  )

  h.receive(fixture_dec(server_open_ok))
  @test.assert_eq(h.state(), HandshakeState::Open)
}
