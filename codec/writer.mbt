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
/// Growing writer for wire-format bytes.
///
/// All integers are big-endian, per AMQP 0-9-1 §4.2.5 "Field types".
/// `Buffer` owns its storage, so a `Writer` is cheap to copy.
pub struct Writer {
  buf : Buffer
}

///|
/// Create an empty writer, optionally pre-allocating space for `size_hint` bytes.
pub fn Writer::new(size_hint? : Int) -> Writer {
  match size_hint {
    Some(n) => { buf: Buffer(size_hint=n), }
    None => { buf: Buffer(), }
  }
}

///|
/// Append the bytes written so far.
pub fn Writer::to_bytes(self : Writer) -> Bytes {
  self.buf.to_bytes()
}

///|
/// Append one octet (§4.2.5 octet).
pub fn Writer::write_byte(self : Writer, value : Byte) -> Unit {
  self.buf.write_byte(value)
}

///|
/// Append one octet as a boolean: `false` writes 0, `true` writes 1.
pub fn Writer::write_bool(self : Writer, value : Bool) -> Unit {
  self.write_byte(if value { b'\x01' } else { b'\x00' })
}

///|
/// Append a 16-bit unsigned integer (§4.2.5 short-uint).
pub fn Writer::write_u16(self : Writer, value : UInt16) -> Unit {
  self.buf.write_uint16_be(value)
}

///|
/// Append a 16-bit signed integer (§4.2.5 short-int).
pub fn Writer::write_i16(self : Writer, value : Int16) -> Unit {
  self.buf.write_int16_be(value)
}

///|
/// Append a 32-bit unsigned integer (§4.2.5 long-uint).
pub fn Writer::write_u32(self : Writer, value : UInt) -> Unit {
  self.buf.write_uint_be(value)
}

///|
/// Append a 32-bit signed integer (§4.2.5 long-int).
pub fn Writer::write_i32(self : Writer, value : Int) -> Unit {
  self.buf.write_int_be(value)
}

///|
/// Append a 64-bit unsigned integer (§4.2.5 long-long-uint).
pub fn Writer::write_u64(self : Writer, value : UInt64) -> Unit {
  self.buf.write_uint64_be(value)
}

///|
/// Append a 64-bit signed integer (§4.2.5 long-long-int).
pub fn Writer::write_i64(self : Writer, value : Int64) -> Unit {
  self.buf.write_int64_be(value)
}

///|
/// Append a 32-bit IEEE 754 float (§4.2.5 float).
pub fn Writer::write_f32(self : Writer, value : Float) -> Unit {
  self.buf.write_float_be(value)
}

///|
/// Append a 64-bit IEEE 754 double (§4.2.5 double).
pub fn Writer::write_f64(self : Writer, value : Double) -> Unit {
  self.buf.write_double_be(value)
}

///|
/// Append a short string: 1-byte length prefix + UTF-8 bytes (§4.2.5 shortstr).
/// Raises `CodecError::OutOfRange` when the UTF-8 encoding is longer than 255 bytes.
pub fn Writer::write_shortstr(
  self : Writer,
  value : String,
) -> Unit raise CodecError {
  let bytes = @utf8.encode(value)
  if bytes.length() > 255 {
    raise CodecError::OutOfRange
  }
  self.buf.write_byte(bytes.length().to_byte())
  self.buf.write_bytes(bytes)
}

///|
/// Append a long string: 4-byte length prefix + UTF-8 bytes (§4.2.5 longstr).
/// Raises `CodecError::OutOfRange` when the UTF-8 encoding is longer than 2^32-1 bytes.
pub fn Writer::write_longstr(
  self : Writer,
  value : String,
) -> Unit raise CodecError {
  let bytes = @utf8.encode(value)
  if bytes.length().to_uint64() > 0xFFFFFFFF {
    raise CodecError::OutOfRange
  }
  self.buf.write_uint_be(bytes.length().reinterpret_as_uint())
  self.buf.write_bytes(bytes)
}

///|
/// Append raw bytes without any length prefix.
pub fn Writer::write_bytes(self : Writer, value : Bytes) -> Unit {
  self.buf.write_bytes(value)
}
