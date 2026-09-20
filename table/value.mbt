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
/// AMQP decimal: the represented number is `value * 10^-scale`
/// (§4.2.5 decimal).
pub(all) struct Decimal {
  /// Number of digits after the decimal point.
  scale : Byte
  /// Unscaled signed 32-bit value.
  value : Int
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend Decimal with Eq::{not_equal, equal}

///|
pub extend Decimal with @debug.Debug::{to_repr}

///|
/// A field-table value: one of the AMQP 0-9-1 field types (§4.2.5).
///
/// Each constructor corresponds to one wire tag
/// (`t b B s U u I i L l f d D S A T F V x`).
pub(all) enum FieldValue {
  /// `t`: boolean.
  Bool(Bool)
  /// `b`: signed octet.
  I8(Int)
  /// `B`: unsigned octet.
  U8(Byte)
  /// `s`: short string, at most 255 UTF-8 bytes.
  ShortStr(String)
  /// `U`: signed 16-bit integer.
  I16(Int16)
  /// `u`: unsigned 16-bit integer.
  U16(UInt16)
  /// `I`: signed 32-bit integer.
  I32(Int)
  /// `i`: unsigned 32-bit integer.
  U32(UInt)
  /// `L`: signed 64-bit integer.
  I64(Int64)
  /// `l`: unsigned 64-bit integer.
  U64(UInt64)
  /// `f`: 32-bit IEEE 754 float.
  F32(Float)
  /// `d`: 64-bit IEEE 754 double.
  F64(Double)
  /// `D`: decimal.
  Decimal(Decimal)
  /// `S`: long string.
  LongStr(String)
  /// `A`: array; each element carries its own tag octet.
  Array(Array[FieldValue])
  /// `T`: 64-bit POSIX timestamp in seconds.
  Timestamp(UInt64)
  /// `F`: nested field table.
  Table(FieldTable)
  /// `V`: void, no payload.
  Void
  /// `x`: byte array.
  Bytes(Bytes)
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend FieldValue with Eq::{not_equal, equal}

///|
pub extend FieldValue with @debug.Debug::{to_repr}

///|
/// Append the value's tag octet and payload to `w`.
///
/// Raises `TableError::ValueOutOfRange` when a value does not fit its
/// wire type (e.g. `I8(200)` or a short string over 255 bytes).
pub fn FieldValue::encode(
  self : FieldValue,
  w : @codec.Writer,
) -> Unit raise TableError {
  match self {
    FieldValue::Bool(v) => {
      w.write_byte(b't')
      w.write_bool(v)
    }
    FieldValue::I8(v) => {
      check_i8(v)
      w.write_byte(b'b')
      w.write_byte(v.to_byte())
    }
    FieldValue::U8(v) => {
      w.write_byte(b'B')
      w.write_byte(v)
    }
    FieldValue::ShortStr(v) => {
      w.write_byte(b's')
      write_shortstr_checked(w, v)
    }
    FieldValue::I16(v) => {
      w.write_byte(b'U')
      w.write_i16(v)
    }
    FieldValue::U16(v) => {
      w.write_byte(b'u')
      w.write_u16(v)
    }
    FieldValue::I32(v) => {
      check_i32(v)
      w.write_byte(b'I')
      w.write_i32(v)
    }
    FieldValue::U32(v) => {
      if v > 0xFFFFFFFF {
        raise TableError::ValueOutOfRange("unsigned 32-bit value out of range")
      }
      w.write_byte(b'i')
      w.write_u32(v)
    }
    FieldValue::I64(v) => {
      w.write_byte(b'L')
      w.write_i64(v)
    }
    FieldValue::U64(v) => {
      w.write_byte(b'l')
      w.write_u64(v)
    }
    FieldValue::F32(v) => {
      w.write_byte(b'f')
      w.write_f32(v)
    }
    FieldValue::F64(v) => {
      w.write_byte(b'd')
      w.write_f64(v)
    }
    FieldValue::Decimal(v) => {
      check_i32(v.value)
      w.write_byte(b'D')
      w.write_byte(v.scale)
      w.write_i32(v.value)
    }
    FieldValue::LongStr(v) => {
      w.write_byte(b'S')
      write_longstr_checked(w, v)
    }
    FieldValue::Array(values) => {
      w.write_byte(b'A')
      let payload = @codec.Writer::new()
      for v in values {
        v.encode(payload)
      }
      write_payload(w, payload)
    }
    FieldValue::Timestamp(v) => {
      w.write_byte(b'T')
      w.write_u64(v)
    }
    FieldValue::Table(t) => {
      w.write_byte(b'F')
      t.encode(w)
    }
    FieldValue::Void => w.write_byte(b'V')
    FieldValue::Bytes(v) => {
      w.write_byte(b'x')
      w.write_u32(v.length().reinterpret_as_uint())
      w.write_bytes(v)
    }
  }
}

///|
/// Read one field value (tag octet + payload) from `r`.
///
/// Raises `TableError::InvalidTag` for an unknown tag,
/// `TableError::Truncated` for input that ends early, and
/// `TableError::InvalidUtf8` for malformed strings.
pub fn FieldValue::decode(r : @codec.Reader) -> FieldValue raise TableError {
  match next_byte(r) {
    b't' => FieldValue::Bool(next_byte(r) != b'\x00')
    b'b' => {
      let u = next_byte(r).to_int()
      FieldValue::I8(if u >= 128 { u - 256 } else { u })
    }
    b'B' => FieldValue::U8(next_byte(r))
    b's' => FieldValue::ShortStr(next_shortstr(r))
    b'U' => FieldValue::I16(next_i16(r))
    b'u' => FieldValue::U16(next_u16(r))
    b'I' => FieldValue::I32(next_i32(r))
    b'i' => FieldValue::U32(next_u32(r))
    b'L' => FieldValue::I64(next_i64(r))
    b'l' => FieldValue::U64(next_u64(r))
    b'f' => FieldValue::F32(next_f32(r))
    b'd' => FieldValue::F64(next_f64(r))
    b'D' =>
      FieldValue::Decimal(Decimal::{ scale: next_byte(r), value: next_i32(r), })
    b'S' => FieldValue::LongStr(next_longstr(r))
    b'A' => {
      let len = next_u32(r)
      if len.to_uint64() > r.remaining().to_uint64() {
        raise TableError::Truncated
      }
      let end = r.remaining() - len.reinterpret_as_int()
      let values : Array[FieldValue] = []
      while r.remaining() > end {
        values.push(FieldValue::decode(r))
      }
      FieldValue::Array(values)
    }
    b'T' => FieldValue::Timestamp(next_u64(r))
    b'F' => FieldValue::Table(FieldTable::decode(r))
    b'V' => FieldValue::Void
    b'x' => {
      let len = next_u32(r)
      FieldValue::Bytes(next_bytes(r, len.reinterpret_as_int()).to_owned())
    }
    other => raise TableError::InvalidTag(other)
  }
}

///|
/// Raise when a signed octet value is outside -128..=127.
fn check_i8(v : Int) -> Unit raise TableError {
  if v < -128 || v > 127 {
    raise TableError::ValueOutOfRange("signed octet must be in -128..=127")
  }
}

///|
/// Raise when a signed 32-bit value is outside the Int32 range.
fn check_i32(v : Int) -> Unit raise TableError {
  if v < -2147483648 || v > 2147483647 {
    raise TableError::ValueOutOfRange("signed 32-bit value out of range")
  }
}

///|
/// `write_shortstr` mapped onto `TableError`.
fn write_shortstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise TableError {
  w.write_shortstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise TableError::ValueOutOfRange(
        "short string longer than 255 UTF-8 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `write_longstr` mapped onto `TableError`.
fn write_longstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise TableError {
  w.write_longstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise TableError::ValueOutOfRange("long string longer than 2^32-1 bytes")
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// Write a 4-byte byte-length prefix followed by the bytes already in `payload`.
fn write_payload(w : @codec.Writer, payload : @codec.Writer) -> Unit {
  let bytes = payload.to_bytes()
  w.write_u32(bytes.length().reinterpret_as_uint())
  w.write_bytes(bytes)
}

///|
/// `read_byte` mapped onto `TableError`.
fn next_byte(r : @codec.Reader) -> Byte raise TableError {
  r.read_byte() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_u16` mapped onto `TableError`.
fn next_u16(r : @codec.Reader) -> UInt16 raise TableError {
  r.read_u16() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_i16` mapped onto `TableError`.
fn next_i16(r : @codec.Reader) -> Int16 raise TableError {
  r.read_i16() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_u32` mapped onto `TableError`.
fn next_u32(r : @codec.Reader) -> UInt raise TableError {
  r.read_u32() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_i32` mapped onto `TableError`.
fn next_i32(r : @codec.Reader) -> Int raise TableError {
  r.read_i32() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_u64` mapped onto `TableError`.
fn next_u64(r : @codec.Reader) -> UInt64 raise TableError {
  r.read_u64() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_i64` mapped onto `TableError`.
fn next_i64(r : @codec.Reader) -> Int64 raise TableError {
  r.read_i64() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_f32` mapped onto `TableError`.
fn next_f32(r : @codec.Reader) -> Float raise TableError {
  r.read_f32() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_f64` mapped onto `TableError`.
fn next_f64(r : @codec.Reader) -> Double raise TableError {
  r.read_f64() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_shortstr` mapped onto `TableError`.
fn next_shortstr(r : @codec.Reader) -> String raise TableError {
  r.read_shortstr() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_longstr` mapped onto `TableError`.
fn next_longstr(r : @codec.Reader) -> String raise TableError {
  r.read_longstr() catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}

///|
/// `read_bytes` mapped onto `TableError`.
fn next_bytes(r : @codec.Reader, n : Int) -> BytesView raise TableError {
  r.read_bytes(n) catch {
    @codec.CodecError::OutOfRange => raise TableError::Truncated
    @codec.CodecError::InvalidUtf8 => raise TableError::InvalidUtf8
  }
}
