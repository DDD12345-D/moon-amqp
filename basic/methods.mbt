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
/// Basic.Qos (method 10): set the prefetch window of a channel. The
/// broker sends at most `prefetch_count` messages before waiting for
/// settlements.
pub(all) struct BasicQos {
  /// Maximum window size in bytes; RabbitMQ does not implement it
  /// and ignores it.
  prefetch_size : UInt
  /// Maximum number of unacknowledged deliveries.
  prefetch_count : UInt16
  /// Apply the settings to the whole connection, not just the
  /// channel. RabbitMQ does not implement this.
  global : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.Consume (method 20): start a consumer on a queue. The
/// broker starts sending Basic.Deliver methods to this channel.
pub(all) struct BasicConsume {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue to consume from.
  queue : String
  /// The consumer tag to use; an empty string lets the server
  /// assign one, reported back in Basic.ConsumeOk.
  consumer_tag : String
  /// Do not receive messages published on this connection.
  no_local : Bool
  /// Deliveries need no acknowledgement: the broker settles them
  /// as soon as they are delivered.
  no_ack : Bool
  /// The queue may only be consumed by this consumer.
  exclusive : Bool
  /// Do not send Basic.ConsumeOk in reply.
  nowait : Bool
  /// Arguments for the queue's implementation, e.g. `x-priority`.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Basic.ConsumeOk (method 21): the server confirms the consumer
/// and reports the tag deliveries will use.
pub(all) struct BasicConsumeOk {
  /// The consumer tag (the server's assignment when the request
  /// sent an empty tag).
  consumer_tag : String
} derive(Eq, @debug.Debug)

///|
/// Basic.Cancel (method 30): stop a consumer. The broker stops
/// delivering to it.
pub(all) struct BasicCancel {
  /// The consumer tag to cancel.
  consumer_tag : String
  /// Do not send Basic.CancelOk in reply.
  nowait : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.CancelOk (method 31): the server confirms the
/// cancellation.
pub(all) struct BasicCancelOk {
  /// The cancelled consumer tag.
  consumer_tag : String
} derive(Eq, @debug.Debug)

///|
/// Basic.Deliver (method 60): the server delivers a message to a
/// consumer. The method frame is followed by the message's content
/// header and body frames (§4.2.6).
pub(all) struct BasicDeliver {
  /// The consumer the message is delivered to.
  consumer_tag : String
  /// Delivery identifier, unique per channel while the delivery is
  /// unacknowledged.
  delivery_tag : UInt64
  /// The broker already delivered this message at least once.
  redelivered : Bool
  /// The exchange the message was published to; the empty string
  /// means the default exchange.
  exchange : String
  /// The routing key the message was published with.
  routing_key : String
} derive(Eq, @debug.Debug)

///|
/// Basic.Get (method 70): poll one message from a queue without
/// registering a consumer.
pub(all) struct BasicGet {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue to poll.
  queue : String
  /// The message needs no acknowledgement.
  no_ack : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.GetOk (method 71): a message was available; it follows as
/// content header and body frames.
pub(all) struct BasicGetOk {
  /// Delivery identifier for acknowledgement.
  delivery_tag : UInt64
  /// The broker already delivered this message at least once.
  redelivered : Bool
  /// The exchange the message was published to.
  exchange : String
  /// The routing key the message was published with.
  routing_key : String
  /// The number of messages remaining on the queue.
  message_count : UInt
} derive(Eq, @debug.Debug)

///|
/// Basic.GetEmpty (method 72): the queue holds no message.
pub(all) struct BasicGetEmpty {
  /// Reserved short string; the spec names the field `cluster-id`
  /// and fixes it to the empty string.
  cluster_id : String
} derive(Eq, @debug.Debug)

///|
/// Basic.Ack (method 80): settle deliveries.
pub(all) struct BasicAck {
  /// The delivery to settle.
  delivery_tag : UInt64
  /// Settle every outstanding delivery up to and including
  /// `delivery_tag`.
  multiple : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.Reject (method 90): reject one delivery. Prefer Basic.Nack,
/// which also handles multiple deliveries.
pub(all) struct BasicReject {
  /// The delivery to reject.
  delivery_tag : UInt64
  /// Return the message to its queue for redelivery; without it
  /// the broker drops the message (or dead-letters it).
  requeue : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.RecoverAsync (method 100): deprecated recovery request that
/// received no confirmation; use Basic.Recover instead.
pub(all) struct BasicRecoverAsync {
  /// Return all unacknowledged messages to their queues.
  requeue : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.Recover (method 110): ask the broker to redeliver the
/// channel's unacknowledged messages.
pub(all) struct BasicRecover {
  /// Return the messages to their queues first, so they may be
  /// redelivered to any consumer; without it they are redelivered
  /// as-is to their original consumers.
  requeue : Bool
} derive(Eq, @debug.Debug)

///|
/// Basic.Nack (method 120, a RabbitMQ extension): reject deliveries,
/// optionally several at once.
pub(all) struct BasicNack {
  /// The delivery to settle.
  delivery_tag : UInt64
  /// Settle every outstanding delivery up to and including
  /// `delivery_tag`.
  multiple : Bool
  /// Return the message to its queue for redelivery; without it
  /// the broker drops the message (or dead-letters it).
  requeue : Bool
} derive(Eq, @debug.Debug)

///|
/// The methods of the basic class (60): publishing and the
/// unroutable-message return path, consumption and polling, and
/// acknowledgement-based flow control.
pub(all) enum BasicMethod {
  BasicQos(BasicQos)
  BasicQosOk(Unit)
  BasicConsume(BasicConsume)
  BasicConsumeOk(BasicConsumeOk)
  BasicCancel(BasicCancel)
  BasicCancelOk(BasicCancelOk)
  BasicPublish(BasicPublish)
  BasicReturn(BasicReturn)
  BasicDeliver(BasicDeliver)
  BasicGet(BasicGet)
  BasicGetOk(BasicGetOk)
  BasicGetEmpty(BasicGetEmpty)
  BasicAck(BasicAck)
  BasicReject(BasicReject)
  BasicRecoverAsync(BasicRecoverAsync)
  BasicRecover(BasicRecover)
  BasicRecoverOk(Unit)
  BasicNack(BasicNack)
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
    BasicQos(q) => {
      write_method_header(w, basic_class_id, (10).to_uint16())
      w.write_u32(q.prefetch_size)
      w.write_u16(q.prefetch_count)
      w.write_byte(pack_bits([q.global]))
    }
    BasicQosOk(_) => write_method_header(w, basic_class_id, (11).to_uint16())
    BasicConsume(c) => {
      write_method_header(w, basic_class_id, (20).to_uint16())
      require_zero_reserved("basic.consume", c.reserved_1)
      w.write_u16(c.reserved_1)
      write_shortstr_checked(w, c.queue)
      write_shortstr_checked(w, c.consumer_tag)
      w.write_byte(pack_bits([c.no_local, c.no_ack, c.exclusive, c.nowait]))
      write_table_checked(w, c.arguments)
    }
    BasicConsumeOk(ok) => {
      write_method_header(w, basic_class_id, (21).to_uint16())
      write_shortstr_checked(w, ok.consumer_tag)
    }
    BasicCancel(c) => {
      write_method_header(w, basic_class_id, (30).to_uint16())
      write_shortstr_checked(w, c.consumer_tag)
      w.write_byte(pack_bits([c.nowait]))
    }
    BasicCancelOk(ok) => {
      write_method_header(w, basic_class_id, (31).to_uint16())
      write_shortstr_checked(w, ok.consumer_tag)
    }
    BasicDeliver(d) => {
      write_method_header(w, basic_class_id, (60).to_uint16())
      write_shortstr_checked(w, d.consumer_tag)
      w.write_u64(d.delivery_tag)
      w.write_byte(pack_bits([d.redelivered]))
      write_shortstr_checked(w, d.exchange)
      write_shortstr_checked(w, d.routing_key)
    }
    BasicGet(g) => {
      write_method_header(w, basic_class_id, (70).to_uint16())
      require_zero_reserved("basic.get", g.reserved_1)
      w.write_u16(g.reserved_1)
      write_shortstr_checked(w, g.queue)
      w.write_byte(pack_bits([g.no_ack]))
    }
    BasicGetOk(ok) => {
      write_method_header(w, basic_class_id, (71).to_uint16())
      w.write_u64(ok.delivery_tag)
      w.write_byte(pack_bits([ok.redelivered]))
      write_shortstr_checked(w, ok.exchange)
      write_shortstr_checked(w, ok.routing_key)
      w.write_u32(ok.message_count)
    }
    BasicGetEmpty(g) => {
      write_method_header(w, basic_class_id, (72).to_uint16())
      if g.cluster_id != "" {
        raise BasicError::ValueOutOfRange(
          "basic.get-empty cluster-id must be the empty string",
        )
      }
      write_shortstr_checked(w, g.cluster_id)
    }
    BasicAck(a) => {
      write_method_header(w, basic_class_id, (80).to_uint16())
      w.write_u64(a.delivery_tag)
      w.write_byte(pack_bits([a.multiple]))
    }
    BasicReject(r) => {
      write_method_header(w, basic_class_id, (90).to_uint16())
      w.write_u64(r.delivery_tag)
      w.write_byte(pack_bits([r.requeue]))
    }
    BasicRecoverAsync(r) => {
      write_method_header(w, basic_class_id, (100).to_uint16())
      w.write_byte(pack_bits([r.requeue]))
    }
    BasicRecover(r) => {
      write_method_header(w, basic_class_id, (110).to_uint16())
      w.write_byte(pack_bits([r.requeue]))
    }
    BasicRecoverOk(_) =>
      write_method_header(w, basic_class_id, (111).to_uint16())
    BasicNack(n) => {
      write_method_header(w, basic_class_id, (120).to_uint16())
      w.write_u64(n.delivery_tag)
      w.write_byte(pack_bits([n.multiple, n.requeue]))
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
    10 => {
      let prefetch_size = next_u32(r)
      let prefetch_count = next_u16(r)
      let flags = next_byte(r).to_int()
      BasicQos(BasicQos::{
        prefetch_size,
        prefetch_count,
        global: (flags & 1) != 0,
      })
    }
    11 => BasicQosOk(())
    20 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let consumer_tag = next_shortstr(r)
      let flags = next_byte(r).to_int()
      let arguments = next_table(r)
      BasicConsume(BasicConsume::{
        reserved_1,
        queue,
        consumer_tag,
        no_local: (flags & 1) != 0,
        no_ack: (flags & 2) != 0,
        exclusive: (flags & 4) != 0,
        nowait: (flags & 8) != 0,
        arguments,
      })
    }
    21 => BasicConsumeOk(BasicConsumeOk::{ consumer_tag: next_shortstr(r), })
    30 =>
      BasicCancel(BasicCancel::{
        consumer_tag: next_shortstr(r),
        nowait: next_bool(r),
      })
    31 => BasicCancelOk(BasicCancelOk::{ consumer_tag: next_shortstr(r), })
    60 => {
      let consumer_tag = next_shortstr(r)
      let delivery_tag = next_u64(r)
      let redelivered = next_bool(r)
      let exchange = next_shortstr(r)
      let routing_key = next_shortstr(r)
      BasicDeliver(BasicDeliver::{
        consumer_tag,
        delivery_tag,
        redelivered,
        exchange,
        routing_key,
      })
    }
    70 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let no_ack = next_bool(r)
      BasicGet(BasicGet::{ reserved_1, queue, no_ack, })
    }
    71 => {
      let delivery_tag = next_u64(r)
      let redelivered = next_bool(r)
      let exchange = next_shortstr(r)
      let routing_key = next_shortstr(r)
      let message_count = next_u32(r)
      BasicGetOk(BasicGetOk::{
        delivery_tag,
        redelivered,
        exchange,
        routing_key,
        message_count,
      })
    }
    72 => BasicGetEmpty(BasicGetEmpty::{ cluster_id: next_shortstr(r), })
    80 => {
      let delivery_tag = next_u64(r)
      let multiple = next_bool(r)
      BasicAck(BasicAck::{ delivery_tag, multiple, })
    }
    90 => {
      let delivery_tag = next_u64(r)
      let requeue = next_bool(r)
      BasicReject(BasicReject::{ delivery_tag, requeue, })
    }
    100 => BasicRecoverAsync(BasicRecoverAsync::{ requeue: next_bool(r), })
    110 => BasicRecover(BasicRecover::{ requeue: next_bool(r), })
    111 => BasicRecoverOk(())
    120 => {
      let delivery_tag = next_u64(r)
      let flags = next_byte(r).to_int()
      BasicNack(BasicNack::{
        delivery_tag,
        multiple: (flags & 1) != 0,
        requeue: (flags & 2) != 0,
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
/// Read one unsigned 32-bit value.
fn next_u32(r : @codec.Reader) -> UInt raise BasicError {
  r.read_u32() catch {
    @codec.CodecError::OutOfRange => raise BasicError::Truncated
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}

///|
/// Read a bit field that holds a single flag (bit 0).
fn next_bool(r : @codec.Reader) -> Bool raise BasicError {
  (next_byte(r).to_int() & 1) != 0
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
