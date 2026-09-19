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
/// AMQP class id for the methods in this package (§3.2.2). Every
/// method payload starts with the class id and a method id.
pub let basic_class_id : UInt16 = (60).to_uint16()

///|
/// Basic.Publish (method 40): publish a message to an exchange. The
/// method frame is followed by the message's content header and body
/// frames (§4.2.6).
pub(all) struct BasicPublish {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The exchange to publish to.
  exchange : String
  /// Messages carrying this routing key are routed by the exchange.
  routing_key : String
  /// Return the message instead of dropping it when it is unroutable.
  mandatory : Bool
  /// Drop the message instead of queueing it when no consumer is
  /// ready at once. RabbitMQ does not implement immediate: it treats
  /// every publish as non-immediate.
  immediate : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.Return (method 50): the server returns an unroutable message
/// that was published with `mandatory` set. It is followed by the
/// message's content header and body frames.
pub(all) struct BasicReturn {
  /// Reply code from the error table (§3.2.2), e.g. 312 `NO-ROUTE`.
  reply_code : UInt16
  /// Human-readable description of the error.
  reply_text : String
  /// The exchange the message was published to.
  exchange : String
  /// The routing key the message was published with.
  routing_key : String
} derive(Eq, @debug.Debug)

///|
/// The methods of the basic class (60): publishing and the
/// unroutable-message return path it can trigger.
pub(all) enum BasicMethod {
  BasicPublish(BasicPublish)
  BasicReturn(BasicReturn)
} derive(Eq, @debug.Debug)

///|
/// Encode the method payload: class id, method id, arguments
/// (§3.2.2). The two bit fields pack into one octet, least
/// significant first (§4.2.5.2).
pub fn BasicMethod::encode(
  self : BasicMethod,
  w : @codec.Writer,
) -> Unit raise BasicError {
  match self {
    BasicPublish(p) => {
      write_method_header(w, basic_class_id, (40).to_uint16())
      require_zero_reserved("basic.publish", p.reserved_1)
      w.write_u16(p.reserved_1)
      write_shortstr_checked(w, p.exchange)
      write_shortstr_checked(w, p.routing_key)
      w.write_byte(pack_bits([p.mandatory, p.immediate]))
    }
    BasicReturn(ret) => {
      write_method_header(w, basic_class_id, (50).to_uint16())
      w.write_u16(ret.reply_code)
      write_shortstr_checked(w, ret.reply_text)
      write_shortstr_checked(w, ret.exchange)
      write_shortstr_checked(w, ret.routing_key)
    }
  }
}

///|
/// Decode a method payload, dispatching on the method id. Unknown
/// classes and methods raise `UnexpectedClass` and `UnknownMethod`
/// with the offending ids.
pub fn BasicMethod::decode(r : @codec.Reader) -> BasicMethod raise BasicError {
  let class_id = next_u16(r)
  if class_id != basic_class_id {
    raise BasicError::UnexpectedClass(class_id)
  }
  let method_id = next_u16(r)
  match method_id.to_uint().reinterpret_as_int() {
    40 => {
      let reserved_1 = next_u16(r)
      let exchange = next_shortstr(r)
      let routing_key = next_shortstr(r)
      let flags = next_byte(r).to_int()
      BasicPublish(BasicPublish::{
        reserved_1,
        exchange,
        routing_key,
        mandatory: (flags & 1) != 0,
        immediate: (flags & 2) != 0,
      })
    }
    50 => {
      let reply_code = next_u16(r)
      let reply_text = next_shortstr(r)
      let exchange = next_shortstr(r)
      let routing_key = next_shortstr(r)
      BasicReturn(BasicReturn::{
        reply_code,
        reply_text,
        exchange,
        routing_key,
      })
    }
    _ => raise BasicError::UnknownMethod(class_id, method_id)
  }
}

///|
/// Pack booleans into one octet, first flag in the low bit
/// (§4.2.5.2).
fn pack_bits(bits : Array[Bool]) -> Byte {
  let mut octet = 0
  for i in 0..<bits.length() {
    if bits[i] {
      octet = octet | (1 << i)
    }
  }
  octet.to_byte()
}

///|
/// The reserved ticket field in basic methods is fixed to 0.
fn require_zero_reserved(
  method_name : String,
  value : UInt16,
) -> Unit raise BasicError {
  if value != (0).to_uint16() {
    raise BasicError::ValueOutOfRange(method_name + " reserved-1 must be 0")
  }
}

///|
/// Write a short string, mapping codec failures to `BasicError`.
fn write_shortstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise BasicError {
  w.write_shortstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise BasicError::ValueOutOfRange(
        "short string longer than 255 UTF-8 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}

///|
/// Read the method header (class id + method id).
fn write_method_header(
  w : @codec.Writer,
  class_id : UInt16,
  method_id : UInt16,
) -> Unit {
  w.write_u16(class_id)
  w.write_u16(method_id)
}

///|
/// Read one unsigned 16-bit value.
fn next_u16(r : @codec.Reader) -> UInt16 raise BasicError {
  r.read_u16() catch {
    @codec.CodecError::OutOfRange => raise BasicError::Truncated
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}

///|
/// Read one byte.
fn next_byte(r : @codec.Reader) -> Byte raise BasicError {
  r.read_byte() catch {
    @codec.CodecError::OutOfRange => raise BasicError::Truncated
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}

///|
/// Read one short string.
fn next_shortstr(r : @codec.Reader) -> String raise BasicError {
  r.read_shortstr() catch {
    @codec.CodecError::OutOfRange => raise BasicError::Truncated
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}
