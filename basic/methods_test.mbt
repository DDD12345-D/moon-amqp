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
/// Encode one basic-class method to its payload bytes.
fn basic_enc(m : BasicMethod) -> Bytes raise BasicError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// Golden encode for basic.publish (60, 40) and basic.return (60, 50):
/// the vectors are hand-derived from §3.2.2 — class id, method id,
/// arguments. The two bit fields pack into one octet, least
/// significant first (§4.2.5.2).
test "basic methods golden encode" {
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        routing_key: "info",
        mandatory: true,
        immediate: false,
      }),
    ),
    b"\x00\x3c\x00\x28\x00\x00\x04logs\x04info\x01",
  )
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        routing_key: "info",
        mandatory: false,
        immediate: true,
      }),
    ),
    b"\x00\x3c\x00\x28\x00\x00\x04logs\x04info\x02",
  )
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "",
        routing_key: "jobs",
        mandatory: false,
        immediate: false,
      }),
    ),
    b"\x00\x3c\x00\x28\x00\x00\x00\x04jobs\x00",
  )
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicReturn(BasicReturn::{
        reply_code: (312).to_uint16(),
        reply_text: "NO_ROUTE",
        exchange: "logs",
        routing_key: "info",
      }),
    ),
    b"\x00\x3c\x00\x32\x01\x38\x08NO_ROUTE\x04logs\x04info",
  )
}

///|
/// The reserved ticket field in basic.publish is fixed to 0.
test "basic.publish reserved-1 must be zero" {
  let raised = try {
    let _ = basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (1).to_uint16(),
        exchange: "e",
        routing_key: "k",
        mandatory: false,
        immediate: false,
      }),
    )
    false
  } catch {
    BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A routing key longer than 255 UTF-8 bytes does not fit its short
/// string wire type.
test "publish routing key over 255 bytes raises" {
  let raised = try {
    let _ = basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "",
        routing_key: "x".repeat(256),
        mandatory: false,
        immediate: false,
      }),
    )
    false
  } catch {
    BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Decode one basic-class method payload.
fn basic_dec(payload : Bytes) -> BasicMethod raise BasicError {
  BasicMethod::decode(@codec.Reader::new(payload))
}

///|
/// Golden decode: the golden encode vectors read back to their values.
test "basic methods golden decode" {
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x28\x00\x00\x04logs\x04info\x01"),
    BasicMethod::BasicPublish(BasicPublish::{
      reserved_1: (0).to_uint16(),
      exchange: "logs",
      routing_key: "info",
      mandatory: true,
      immediate: false,
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x28\x00\x00\x04logs\x04info\x02"),
    BasicMethod::BasicPublish(BasicPublish::{
      reserved_1: (0).to_uint16(),
      exchange: "logs",
      routing_key: "info",
      mandatory: false,
      immediate: true,
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x28\x00\x00\x00\x04jobs\x00"),
    BasicMethod::BasicPublish(BasicPublish::{
      reserved_1: (0).to_uint16(),
      exchange: "",
      routing_key: "jobs",
      mandatory: false,
      immediate: false,
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x32\x01\x38\x08NO_ROUTE\x04logs\x04info"),
    BasicMethod::BasicReturn(BasicReturn::{
      reply_code: (312).to_uint16(),
      reply_text: "NO_ROUTE",
      exchange: "logs",
      routing_key: "info",
    }),
  )
}

///|
/// Every basic-class method survives an encode/decode roundtrip.
test "basic methods roundtrip" {
  for
    m in [
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        routing_key: "route.#",
        mandatory: true,
        immediate: true,
      }),
      BasicMethod::BasicReturn(BasicReturn::{
        reply_code: (312).to_uint16(),
        reply_text: "NO_ROUTE",
        exchange: "amq.topic",
        routing_key: "route.#",
      }),
    ] {
    @test.assert_eq(basic_dec(basic_enc(m)), m)
  }
}

///|
/// A payload whose class id is not 60 is rejected with the offending
/// id — this lets a connection-level reader route methods to the
/// right package (connection vs channel vs basic).
test "unexpected class raises" {
  let err = try {
    let _ = basic_dec(b"\x00\x14\x00\x0a\x00") // channel class
    None
  } catch {
    BasicError::UnexpectedClass(c) => Some(c)
    _ => None
  }
  @test.assert_eq(err, Some((20).to_uint16()))
}

///|
/// A method id outside this package's methods reports both ids.
test "unknown method raises" {
  let err = try {
    let _ = basic_dec(b"\x00\x3c\x00\x63") // method 99
    None
  } catch {
    BasicError::UnknownMethod(c, m) => Some((c, m))
    _ => None
  }
  @test.assert_eq(err, Some(((60).to_uint16(), (99).to_uint16())))
}

///|
/// Input that ends before the arguments are complete is `Truncated`.
test "truncated input raises" {
  for
    payload in [b"", b"\x00\x3c", b"\x00\x3c\x00\x28", b"\x00\x3c\x00\x28\x00"] {
    let raised = try {
      let _ = basic_dec(payload)
      false
    } catch {
      BasicError::Truncated => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// A short string containing invalid UTF-8 is `InvalidUtf8`.
test "invalid utf-8 raises" {
  let raised = try {
    let _ = basic_dec(b"\x00\x3c\x00\x28\x00\x00\x02\xff\xfe")
    false
  } catch {
    BasicError::InvalidUtf8 => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
