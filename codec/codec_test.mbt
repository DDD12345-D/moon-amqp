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
/// Golden decode: each primitive is checked against hand-written
/// big-endian byte vectors from AMQP 0-9-1 §4.2.5.
test "golden decode integers" {
  let r = @codec.Reader::new(b"\x01\x00\xff\xff\x80\x00")
  @test.assert_eq(r.read_u16().to_uint(), 0x0100)
  @test.assert_eq(r.read_i16().to_int(), -1)
  @test.assert_eq(r.read_i16().to_int(), -32768)

  let r2 = @codec.Reader::new(
    b"\x01\x02\x03\x04\xff\xff\xff\xff\x80\x00\x00\x00",
  )
  @test.assert_eq(r2.read_u32().to_uint64(), 0x01020304)
  @test.assert_eq(r2.read_i32(), -1)
  @test.assert_eq(r2.read_i32(), -2147483648)

  let r3 = @codec.Reader::new(
    b"\x00\x00\x00\x00\x00\x00\x01\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00",
  )
  @test.assert_eq(r3.read_u64(), 0x100)
  @test.assert_eq(r3.read_i64().to_int(), -1)
  @test.assert_eq(r3.read_i64().to_int(), -65536)
}

///|
/// Golden encode: values become the exact byte vectors from §4.2.5.
test "golden encode" {
  let w = @codec.Writer::new()
  w.write_u16((0x0100).to_uint16())
  w.write_i16(Int16::from_int(-2))
  w.write_u32(0x01020304)
  w.write_i32(-1)
  w.write_u64(0x100)
  @test.assert_eq(
    w.to_bytes(),
    b"\x01\x00\xff\xfe\x01\x02\x03\x04\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x01\x00",
  )
}

///|
/// Golden encode: float roundtrip preserves bits.
test "float roundtrip preserves bits" {
  let r = @codec.Reader::new(b"\x3f\x80\x00\x00")
  let f = r.read_f32()
  let w = @codec.Writer::new()
  w.write_f32(f)
  @test.assert_eq(w.to_bytes(), b"\x3f\x80\x00\x00")

  let r2 = @codec.Reader::new(b"\x3f\xf0\x00\x00\x00\x00\x00\x00")
  let d = r2.read_f64()
  let w2 = @codec.Writer::new()
  w2.write_f64(d)
  @test.assert_eq(w2.to_bytes(), b"\x3f\xf0\x00\x00\x00\x00\x00\x00")
}

///|
/// Strings: shortstr uses a 1-byte prefix, longstr a 4-byte prefix (§4.2.5).
test "golden strings" {
  let r = @codec.Reader::new(b"\x05hello\x00\x00\x00\x03abc")
  @test.assert_eq(r.read_shortstr(), "hello")
  @test.assert_eq(r.read_longstr(), "abc")

  let w = @codec.Writer::new()
  w.write_shortstr("hello")
  w.write_longstr("abc")
  @test.assert_eq(w.to_bytes(), b"\x05hello\x00\x00\x00\x03abc")
}

///|
/// Booleans: 0 is false, any nonzero octet is true.
test "bool decoding" {
  let r = @codec.Reader::new(b"\x00\x01\x02")
  @test.assert_eq(r.read_bool(), false)
  @test.assert_eq(r.read_bool(), true)
  @test.assert_eq(r.read_bool(), true)
}

///|
/// Every read advances the position; truncation raises `OutOfRange`.
test "truncation raises" {
  let r = @codec.Reader::new(b"\x01")
  let raised = try {
    let _ = r.read_u16()
    false
  } catch {
    CodecError::OutOfRange => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A failed read leaves the position unchanged, so a caller can recover.
test "failed read leaves position" {
  let r = @codec.Reader::new(b"\x01")
  let _ = r.read_u16() catch { _ => (0).to_uint16() }
  @test.assert_eq(r.remaining(), 1)
  @test.assert_eq(r.read_byte(), b'\x01')
}

///|
/// Hostile longstr length is reported as `OutOfRange`, not a wrap-around.
test "hostile longstr length" {
  let r = @codec.Reader::new(b"\xff\xff\xff\xffabc")
  let raised = try {
    let _ = r.read_longstr()
    false
  } catch {
    CodecError::OutOfRange => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A shortstr longer than 255 bytes cannot be written.
test "shortstr too long raises" {
  let w = @codec.Writer::new()
  let raised = try {
    w.write_shortstr("x".repeat(256))
    false
  } catch {
    CodecError::OutOfRange => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Empty strings are valid: zero-length prefixes.
test "empty strings" {
  let w = @codec.Writer::new()
  w.write_shortstr("")
  w.write_longstr("")
  let r = @codec.Reader::new(w.to_bytes())
  @test.assert_eq(r.read_shortstr(), "")
  @test.assert_eq(r.read_longstr(), "")
}
