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
/// Positional reader over an immutable `Bytes` buffer.
///
/// All integers are big-endian, per AMQP 0-9-1 §4.2.5 "Field types".
/// The position advances only on successful reads, so a failing read
/// leaves the reader usable.
pub struct Reader {
  bytes : Bytes
  mut pos : Int
}

///|
/// Create a reader over `bytes`, starting at position 0.
pub fn Reader::new(bytes : Bytes) -> Reader {
  { bytes, pos: 0, }
}

///|
/// Bytes not yet read.
pub fn Reader::remaining(self : Reader) -> Int {
  self.bytes.length() - self.pos
}

///|
/// Whether all bytes have been read.
pub fn Reader::is_empty(self : Reader) -> Bool {
  !(self.remaining() > 0)
}

///|
/// Read the next `n` bytes, advancing the position.
/// The returned view is a window into the reader's buffer.
/// Raises `CodecError::OutOfRange` when fewer than `n` bytes remain.
pub fn Reader::read_bytes(self : Reader, n : Int) -> BytesView raise CodecError {
  if n < 0 || self.pos + n > self.bytes.length() {
    raise CodecError::OutOfRange
  }
  let out = self.bytes[self.pos:self.pos + n]
  self.pos += n
  out
}

///|
/// Read one octet (§4.2.5 octet).
pub fn Reader::read_byte(self : Reader) -> Byte raise CodecError {
  if self.pos >= self.bytes.length() {
    raise CodecError::OutOfRange
  }
  let b = self.bytes.at(self.pos)
  self.pos += 1
  b
}

///|
/// Read one octet as a boolean: 0 is `false`, any other value is `true`.
pub fn Reader::read_bool(self : Reader) -> Bool raise CodecError {
  !(self.read_byte() == b'\x00')
}

///|
/// Read a 16-bit unsigned integer (§4.2.5 short-uint).
pub fn Reader::read_u16(self : Reader) -> UInt16 raise CodecError {
  let b = self.read_bytes(2)
  ((b.at(0).to_uint() << 8) | b.at(1).to_uint()).to_uint16()
}

///|
/// Read a 16-bit signed integer, sign-extending (§4.2.5 short-int).
pub fn Reader::read_i16(self : Reader) -> Int16 raise CodecError {
  let u = self.read_u16().to_uint()
  let i = u.reinterpret_as_int()
  if u >= 0x8000 {
    Int16::from_int(i - 0x10000)
  } else {
    Int16::from_int(i)
  }
}

///|
/// Read a 32-bit unsigned integer (§4.2.5 long-uint).
pub fn Reader::read_u32(self : Reader) -> UInt raise CodecError {
  let b = self.read_bytes(4)
  (b.at(0).to_uint() << 24) |
  (b.at(1).to_uint() << 16) |
  (b.at(2).to_uint() << 8) |
  b.at(3).to_uint()
}

///|
/// Read a 32-bit signed integer (§4.2.5 long-int).
pub fn Reader::read_i32(self : Reader) -> Int raise CodecError {
  self.read_u32().reinterpret_as_int()
}

///|
/// Read a 64-bit unsigned integer (§4.2.5 long-long-uint).
pub fn Reader::read_u64(self : Reader) -> UInt64 raise CodecError {
  let b = self.read_bytes(8)
  (b.at(0).to_uint64() << 56) |
  (b.at(1).to_uint64() << 48) |
  (b.at(2).to_uint64() << 40) |
  (b.at(3).to_uint64() << 32) |
  (b.at(4).to_uint64() << 24) |
  (b.at(5).to_uint64() << 16) |
  (b.at(6).to_uint64() << 8) |
  b.at(7).to_uint64()
}

///|
/// Read a 64-bit signed integer (§4.2.5 long-long-int).
pub fn Reader::read_i64(self : Reader) -> Int64 raise CodecError {
  self.read_u64().reinterpret_as_int64()
}

///|
/// Read a 32-bit IEEE 754 float (§4.2.5 float).
pub fn Reader::read_f32(self : Reader) -> Float raise CodecError {
  Float::reinterpret_from_uint(self.read_u32())
}

///|
/// Read a 64-bit IEEE 754 double (§4.2.5 double).
pub fn Reader::read_f64(self : Reader) -> Double raise CodecError {
  self.read_u64().reinterpret_as_double()
}

///|
/// Read a short string: 1-byte length prefix + UTF-8 bytes (§4.2.5 shortstr).
/// Raises `CodecError::InvalidUtf8` when the bytes are not valid UTF-8.
pub fn Reader::read_shortstr(self : Reader) -> String raise CodecError {
  let len = self.read_byte().to_int()
  let bytes = self.read_bytes(len)
  @utf8.decode(bytes) catch {
    @utf8.Malformed(_) => raise CodecError::InvalidUtf8
  }
}

///|
/// Read a long string: 4-byte length prefix + UTF-8 bytes (§4.2.5 longstr).
/// Raises `CodecError::InvalidUtf8` when the bytes are not valid UTF-8.
pub fn Reader::read_longstr(self : Reader) -> String raise CodecError {
  let len = self.read_u32()
  // Guard the length against the remaining buffer before converting, so a
  // truncated or hostile length reports `OutOfRange` instead of wrapping.
  if len.to_uint64() > self.remaining().to_uint64() {
    raise CodecError::OutOfRange
  }
  let bytes = self.read_bytes(len.reinterpret_as_int())
  @utf8.decode(bytes) catch {
    @utf8.Malformed(_) => raise CodecError::InvalidUtf8
  }
}
