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
/// Encode one value and return its bytes.
fn enc(v : FieldValue) -> Bytes raise TableError {
  let w = @codec.Writer::new()
  v.encode(w)
  w.to_bytes()
}

///|
/// Golden encode: each constructor maps to the exact tag+payload from
/// AMQP 0-9-1 §4.2.5.
test "value tags golden encode" {
  @test.assert_eq(enc(FieldValue::Bool(true)), b"\x74\x01")
  @test.assert_eq(enc(FieldValue::Bool(false)), b"\x74\x00")
  @test.assert_eq(enc(FieldValue::I8(-2)), b"\x62\xfe")
  @test.assert_eq(enc(FieldValue::U8(b'\xff')), b"\x42\xff")
  @test.assert_eq(enc(FieldValue::ShortStr("hi")), b"\x73\x02hi")
  @test.assert_eq(enc(FieldValue::I16(Int16::from_int(-2))), b"\x55\xff\xfe")
  @test.assert_eq(enc(FieldValue::U16((0x0102).to_uint16())), b"\x75\x01\x02")
  @test.assert_eq(enc(FieldValue::I32(-2)), b"\x49\xff\xff\xff\xfe")
  @test.assert_eq(enc(FieldValue::U32(0x01020304)), b"\x69\x01\x02\x03\x04")
  @test.assert_eq(
    enc(FieldValue::I64(Int64::from_int(-2))),
    b"\x4c\xff\xff\xff\xff\xff\xff\xff\xfe",
  )
  @test.assert_eq(
    enc(FieldValue::U64(0x0102030405060708)),
    b"\x6c\x01\x02\x03\x04\x05\x06\x07\x08",
  )
  @test.assert_eq(enc(FieldValue::F32(1.0)), b"\x66\x3f\x80\x00\x00")
  @test.assert_eq(
    enc(FieldValue::F64(1.0)),
    b"\x64\x3f\xf0\x00\x00\x00\x00\x00\x00",
  )
  @test.assert_eq(
    enc(FieldValue::Decimal(Decimal::{ scale: b'\x02', value: -123, })),
    b"\x44\x02\xff\xff\xff\x85",
  )
  @test.assert_eq(enc(FieldValue::LongStr("hi")), b"\x53\x00\x00\x00\x02hi")
  @test.assert_eq(
    enc(
      FieldValue::Array([
        FieldValue::U8(b'\x01'),
        FieldValue::I16(Int16::from_int(-2)),
      ]),
    ),
    b"\x41\x00\x00\x00\x05\x42\x01\x55\xff\xfe",
  )
  @test.assert_eq(
    enc(FieldValue::Timestamp(0x0102030405060708)),
    b"\x54\x01\x02\x03\x04\x05\x06\x07\x08",
  )
  @test.assert_eq(
    enc(FieldValue::Table(FieldTable::new())),
    b"\x46\x00\x00\x00\x00",
  )
  @test.assert_eq(enc(FieldValue::Void), b"\x56")
  @test.assert_eq(
    enc(FieldValue::Bytes(b"\x01\x02")),
    b"\x78\x00\x00\x00\x02\x01\x02",
  )
}

///|
/// Golden decode: the same vectors read back to their values.
test "value tags golden decode" {
  let r = @codec.Reader::new(
    b"\x62\xfe\x42\xff\x73\x02hi\x55\xff\xfe\x75\x01\x02\x49\xff\xff\xff\xfe\x69\x01\x02\x03\x04\x66\x3f\x80\x00\x00\x64\x3f\xf0\x00\x00\x00\x00\x00\x00\x44\x02\xff\xff\xff\x85\x54\x01\x02\x03\x04\x05\x06\x07\x08\x56",
  )
  @test.assert_eq(FieldValue::decode(r), FieldValue::I8(-2))
  @test.assert_eq(FieldValue::decode(r), FieldValue::U8(b'\xff'))
  @test.assert_eq(FieldValue::decode(r), FieldValue::ShortStr("hi"))
  @test.assert_eq(FieldValue::decode(r), FieldValue::I16(Int16::from_int(-2)))
  @test.assert_eq(FieldValue::decode(r), FieldValue::U16((0x0102).to_uint16()))
  @test.assert_eq(FieldValue::decode(r), FieldValue::I32(-2))
  @test.assert_eq(FieldValue::decode(r), FieldValue::U32(0x01020304))
  @test.assert_eq(FieldValue::decode(r), FieldValue::F32(1.0))
  @test.assert_eq(FieldValue::decode(r), FieldValue::F64(1.0))
  @test.assert_eq(
    FieldValue::decode(r),
    FieldValue::Decimal(Decimal::{ scale: b'\x02', value: -123, }),
  )
  @test.assert_eq(
    FieldValue::decode(r),
    FieldValue::Timestamp(0x0102030405060708),
  )
  @test.assert_eq(FieldValue::decode(r), FieldValue::Void)
}

///|
/// Golden table: entries are written sorted; the byte vector is
/// hand-derived from §4.2.5.5.
test "table golden encode" {
  let t = FieldTable::new()
  t.set("u8", FieldValue::U8(b'\xff'))
  t.set("bool", FieldValue::Bool(true))
  let w = @codec.Writer::new()
  t.encode(w)
  @test.assert_eq(
    w.to_bytes(),
    b"\x00\x00\x00\x0c\x04bool\x74\x01\x02u8\x42\xff",
  )
}

///|
/// Golden table decode: the same vector reads back entry by entry.
test "table golden decode" {
  let r = @codec.Reader::new(b"\x00\x00\x00\x0c\x04bool\x74\x01\x02u8\x42\xff")
  let t = FieldTable::decode(r)
  @test.assert_eq(t.length(), 2)
  @test.assert_eq(t.get("bool"), Some(FieldValue::Bool(true)))
  @test.assert_eq(t.get("u8"), Some(FieldValue::U8(b'\xff')))
  @test.assert_eq(t.get("missing"), None)
}

///|
/// Nested tables and arrays roundtrip with their length prefixes.
test "nested roundtrip" {
  let inner = FieldTable::new()
  inner.set("k", FieldValue::I8(-2))
  let outer = FieldTable::new()
  outer.set("inner", FieldValue::Table(inner))
  outer.set(
    "arr",
    FieldValue::Array([
      FieldValue::I8(-1),
      FieldValue::ShortStr("x"),
      FieldValue::Void,
    ]),
  )
  let w = @codec.Writer::new()
  outer.encode(w)
  let back = FieldTable::decode(@codec.Reader::new(w.to_bytes()))
  match back.get("inner") {
    Some(FieldValue::Table(t)) =>
      @test.assert_eq(t.get("k"), Some(FieldValue::I8(-2)))
    _ => @test.assert_eq(0, 1)
  }
  @test.assert_eq(
    back.get("arr"),
    Some(
      FieldValue::Array([
        FieldValue::I8(-1),
        FieldValue::ShortStr("x"),
        FieldValue::Void,
      ]),
    ),
  )
}

///|
/// All 18 field types survive an encode/decode roundtrip.
test "all types roundtrip" {
  let t = FieldTable::new()
  t.set("bool", FieldValue::Bool(true))
  t.set("i8", FieldValue::I8(-2))
  t.set("u8", FieldValue::U8(b'\xfe'))
  t.set("s", FieldValue::ShortStr("hello"))
  t.set("i16", FieldValue::I16(Int16::from_int(-1234)))
  t.set("u16", FieldValue::U16((0x1234).to_uint16()))
  t.set("i32", FieldValue::I32(-123456))
  t.set("u32", FieldValue::U32(0x12345678))
  t.set("i64", FieldValue::I64(Int64::from_int(-123456789)))
  t.set("u64", FieldValue::U64(0x0102030405060708))
  t.set("f32", FieldValue::F32(1.5))
  t.set("f64", FieldValue::F64(2.5))
  t.set("dec", FieldValue::Decimal(Decimal::{ scale: b'\x02', value: -123, }))
  t.set("long", FieldValue::LongStr("long string"))
  t.set("arr", FieldValue::Array([FieldValue::I8(-1), FieldValue::Void]))
  t.set("ts", FieldValue::Timestamp(0x0102030405060708))
  t.set("void", FieldValue::Void)
  t.set("bytes", FieldValue::Bytes(b"\x00\xff\x10"))

  let w = @codec.Writer::new()
  t.encode(w)
  let back = FieldTable::decode(@codec.Reader::new(w.to_bytes()))
  @test.assert_eq(back.length(), 18)
  for entry in t.iter() {
    @test.assert_eq(back.get(entry.0), Some(entry.1))
  }
}

///|
/// A field name starting with a digit is rejected.
test "invalid key raises" {
  let t = FieldTable::new()
  for bad in ["1abc", "a b", ""] {
    let raised = try {
      t.set(bad, FieldValue::Void)
      false
    } catch {
      TableError::InvalidKey(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
  let raised = try {
    t.set("a".repeat(129), FieldValue::Void)
    false
  } catch {
    TableError::InvalidKey(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Dotted field names (e.g. RabbitMQ's capability names) are accepted
/// and survive an encode/decode roundtrip.
test "dotted field names are accepted" {
  let t = FieldTable::new()
  t.set("basic.nack", FieldValue::Bool(true))
  let w = @codec.Writer::new()
  t.encode(w)
  let r = @codec.Reader::new(w.to_bytes())
  let back = FieldTable::decode(r)
  @test.assert_eq(back.get("basic.nack"), Some(FieldValue::Bool(true)))
}

///|
/// An unknown tag octet is reported as `InvalidTag` with the octet.
test "unknown tag raises" {
  let r = @codec.Reader::new(b"\x5a")
  let err = try {
    let _ = FieldValue::decode(r)
    None
  } catch {
    TableError::InvalidTag(t) => Some(t)
    _ => None
  }
  @test.assert_eq(err, Some(b'Z'))
}

///|
/// A length prefix larger than the remaining input is `Truncated`.
test "hostile lengths raise" {
  let r1 = @codec.Reader::new(b"\x00\x00\x00\x10\x04bool")
  let e1 = try {
    let _ = FieldTable::decode(r1)
    None
  } catch {
    TableError::Truncated => Some(0)
    _ => None
  }
  @test.assert_eq(e1, Some(0))

  let r2 = @codec.Reader::new(b"\x41\xff\xff\xff\xff")
  let e2 = try {
    let _ = FieldValue::decode(r2)
    None
  } catch {
    TableError::Truncated => Some(0)
    _ => None
  }
  @test.assert_eq(e2, Some(0))
}

///|
/// Values outside their wire range are rejected at encode.
test "out of range values raise" {
  for v in [FieldValue::I8(200), FieldValue::I8(-129)] {
    let raised = try {
      let _ = enc(v)
      false
    } catch {
      TableError::ValueOutOfRange(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
  let raised = try {
    let _ = enc(FieldValue::ShortStr("x".repeat(256)))
    false
  } catch {
    TableError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// An empty table is exactly a zero length prefix.
test "empty table" {
  let t = FieldTable::new()
  let w = @codec.Writer::new()
  t.encode(w)
  @test.assert_eq(w.to_bytes(), b"\x00\x00\x00\x00")
  let back = FieldTable::decode(@codec.Reader::new(w.to_bytes()))
  @test.assert_eq(back.is_empty(), true)
}
