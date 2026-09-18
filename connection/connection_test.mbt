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
/// Encode one method and return its payload bytes.
fn enc(m : ConnectionMethod) -> Bytes raise ConnectionError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// Golden encode: each method maps to the exact wire bytes from
/// AMQP 0-9-1 §4.3 (class id 10, method id, arguments).
test "connection method golden encode" {
  @test.assert_eq(
    enc(
      ConnectionMethod::Start(Start::{
        version_major: b'\x00',
        version_minor: b'\x09',
        server_properties: @table.FieldTable::new(),
        mechanisms: "PLAIN",
        locales: "en_US",
      }),
    ),
    b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US",
  )
  @test.assert_eq(
    enc(
      ConnectionMethod::StartOk(StartOk::{
        client_properties: @table.FieldTable::new(),
        mechanism: "PLAIN",
        response: "guest",
        locale: "en_US",
      }),
    ),
    b"\x00\x0a\x00\x0b\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05guest\x05en_US",
  )
  @test.assert_eq(
    enc(
      ConnectionMethod::Tune(Tune::{
        channel_max: (2047).to_uint16(),
        frame_max: 131072,
        heartbeat: (60).to_uint16(),
      }),
    ),
    b"\x00\x0a\x00\x1e\x07\xff\x00\x02\x00\x00\x00\x3c",
  )
  @test.assert_eq(
    enc(
      ConnectionMethod::TuneOk(TuneOk::{
        channel_max: (2047).to_uint16(),
        frame_max: 131072,
        heartbeat: (60).to_uint16(),
      }),
    ),
    b"\x00\x0a\x00\x1f\x07\xff\x00\x02\x00\x00\x00\x3c",
  )
  @test.assert_eq(
    enc(
      ConnectionMethod::Open(Open::{
        virtual_host: "/",
        reserved_1: "",
        reserved_2: false,
      }),
    ),
    b"\x00\x0a\x00\x28\x01/\x00\x00",
  )
  @test.assert_eq(
    enc(ConnectionMethod::OpenOk(OpenOk::{ reserved_1: "", })),
    b"\x00\x0a\x00\x29\x00",
  )
}

///|
/// A Start whose server-properties table has entries: the table is
/// written sorted, exactly as the table package encodes it.
test "start with server properties golden" {
  let props = @table.FieldTable::new()
  props.set("product", @table.FieldValue::ShortStr("RabbitMQ"))
  props.set("version", @table.FieldValue::LongStr("4.3.6"))
  @test.assert_eq(
    enc(
      ConnectionMethod::Start(Start::{
        version_major: b'\x00',
        version_minor: b'\x09',
        server_properties: props,
        mechanisms: "PLAIN",
        locales: "en_US",
      }),
    ),
    b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x24\x07product\x73\x08RabbitMQ\x07version\x53\x00\x00\x00\x054.3.6\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US",
  )
}

///|
/// The reserved Open/OpenOk fields are fixed by the spec; non-fixed
/// values are rejected at encode.
test "reserved fields raise" {
  for
    bad in [
      ConnectionMethod::Open(Open::{
        virtual_host: "/",
        reserved_1: "x",
        reserved_2: false,
      }),
      ConnectionMethod::Open(Open::{
        virtual_host: "/",
        reserved_1: "",
        reserved_2: true,
      }),
      ConnectionMethod::OpenOk(OpenOk::{ reserved_1: "x", }),
    ] {
    let raised = try {
      let _ = enc(bad)
      false
    } catch {
      ConnectionError::ValueOutOfRange(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// A short string over 255 bytes is rejected at encode.
test "oversized short strings raise" {
  let raised = try {
    let _ = enc(
      ConnectionMethod::StartOk(StartOk::{
        client_properties: @table.FieldTable::new(),
        mechanism: "x".repeat(256),
        response: "",
        locale: "en_US",
      }),
    )
    false
  } catch {
    ConnectionError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Golden decode: the six golden payloads read back to their methods,
/// one after another from a single reader.
test "connection method golden decode" {
  let r = @codec.Reader::new(
    b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US\x00\x0a\x00\x0b\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05guest\x05en_US\x00\x0a\x00\x1e\x07\xff\x00\x02\x00\x00\x00\x3c\x00\x0a\x00\x1f\x07\xff\x00\x02\x00\x00\x00\x3c\x00\x0a\x00\x28\x01/\x00\x00\x00\x0a\x00\x29\x00",
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::Start(Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::StartOk(StartOk::{
      client_properties: @table.FieldTable::new(),
      mechanism: "PLAIN",
      response: "guest",
      locale: "en_US",
    }),
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::Tune(Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::TuneOk(TuneOk::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::Open(Open::{
      virtual_host: "/",
      reserved_1: "",
      reserved_2: false,
    }),
  )
  @test.assert_eq(
    ConnectionMethod::decode(r),
    ConnectionMethod::OpenOk(OpenOk::{ reserved_1: "", }),
  )
  @test.assert_eq(r.is_empty(), true)
}

///|
/// Every method survives an encode/decode roundtrip, including a
/// Start with a non-empty server-properties table.
test "connection method roundtrip" {
  let props = @table.FieldTable::new()
  props.set("product", @table.FieldValue::ShortStr("RabbitMQ"))
  props.set("version", @table.FieldValue::LongStr("4.3.6"))
  let client = @table.FieldTable::new()
  client.set("product", @table.FieldValue::ShortStr("moon-amqp"))
  let methods = [
    ConnectionMethod::Start(Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: props,
      mechanisms: "AMQPLAIN PLAIN",
      locales: "en_US",
    }),
    ConnectionMethod::StartOk(StartOk::{
      client_properties: client,
      mechanism: "PLAIN",
      response: "guest",
      locale: "en_US",
    }),
    ConnectionMethod::Tune(Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
    ConnectionMethod::TuneOk(TuneOk::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
    ConnectionMethod::Open(Open::{
      virtual_host: "/",
      reserved_1: "",
      reserved_2: false,
    }),
    ConnectionMethod::OpenOk(OpenOk::{ reserved_1: "", }),
  ]
  let w = @codec.Writer::new()
  for m in methods {
    m.encode(w)
  }
  let r = @codec.Reader::new(w.to_bytes())
  for m in methods {
    @test.assert_eq(ConnectionMethod::decode(r), m)
  }
  @test.assert_eq(r.is_empty(), true)
}

///|
/// Malformed payloads raise the expected variants: a foreign class-id
/// is `UnexpectedClass`, an unknown method-id is `UnknownMethod`,
/// cut-off input is `Truncated`, and a bad field tag is `InvalidTable`.
test "malformed methods raise" {
  let err = try {
    let _ = ConnectionMethod::decode(@codec.Reader::new(b"\x00\x14\x00\x0a"))
    None
  } catch {
    ConnectionError::UnexpectedClass(c) => Some(c)
    _ => None
  }
  @test.assert_eq(err, Some((20).to_uint16()))
  let err = try {
    let _ = ConnectionMethod::decode(@codec.Reader::new(b"\x00\x0a\x00\x63"))
    None
  } catch {
    ConnectionError::UnknownMethod(m) => Some(m)
    _ => None
  }
  @test.assert_eq(err, Some((99).to_uint16()))
  for
    bad in [
      b"", b"\x00\x0a\x00", b"\x00\x0a\x00\x0a\x00", b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x10\x04bool",
    ] {
    let raised = try {
      let _ = ConnectionMethod::decode(@codec.Reader::new(bad))
      false
    } catch {
      ConnectionError::Truncated => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
  let err = try {
    let _ = ConnectionMethod::decode(
      @codec.Reader::new(
        b"\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x03\x01\x5a\x5a",
      ),
    )
    None
  } catch {
    ConnectionError::InvalidTable(_) => Some(0)
    _ => None
  }
  @test.assert_eq(err, Some(0))
}
