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
/// AMQP class ids for the methods in this package (§3.2.2). Every
/// method payload starts with one of these class ids and a method id.
pub let channel_class_id : UInt16 = (20).to_uint16()

///|
pub let exchange_class_id : UInt16 = (40).to_uint16()

///|
pub let queue_class_id : UInt16 = (50).to_uint16()

///|
/// Channel.Open (method 10): the client asks the server to open a
/// channel for method multiplexing.
pub(all) struct ChannelOpen {
  /// Reserved: must be the empty short string.
  reserved_1 : String
} derive(Eq, @debug.Debug)

///|
/// Channel.Open-Ok (method 11): the server confirms the channel is
/// open.
pub(all) struct ChannelOpenOk {
  /// Reserved: must be the empty long string.
  reserved_1 : String
} derive(Eq, @debug.Debug)

///|
/// Channel.Flow (method 20): either peer asks the other to pause or
/// resume delivery on this channel.
pub(all) struct ChannelFlow {
  /// `true` starts delivery, `false` pauses it.
  active : Bool
} derive(Eq, @debug.Debug)

///|
/// Channel.Flow-Ok (method 21): confirms a flow change.
pub(all) struct ChannelFlowOk {
  /// `true` starts delivery, `false` pauses it.
  active : Bool
} derive(Eq, @debug.Debug)

///|
/// Channel.Close (method 40): a peer reports an error on this channel
/// and asks the other to close it.
pub(all) struct ChannelClose {
  /// Reply code from the error table (§3.2.2), e.g. 404 `NOT-FOUND`.
  reply_code : UInt16
  /// Human-readable description of the error.
  reply_text : String
  /// Class id of the method that failed, when applicable.
  class_id : UInt16
  /// Method id of the method that failed, when applicable.
  method_id : UInt16
} derive(Eq, @debug.Debug)

///|
/// Exchange.Declare (method 10): declare an exchange, or verify one
/// exists when `passive` is set. The five bits are packed into one
/// octet (§4.2.5.2), least significant first: passive, durable,
/// auto-delete, internal, no-wait.
pub(all) struct ExchangeDeclare {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The exchange name; must not be empty.
  exchange : String
  /// The exchange type, e.g. `"direct"`, `"fanout"`, `"topic"`,
  /// `"headers"`.
  exchange_type : String
  /// Only check that the exchange exists; fail if it does not.
  passive : Bool
  /// Survive a broker restart.
  durable : Bool
  /// Delete once the last binding is removed.
  auto_delete : Bool
  /// Only reachable by other exchanges (no client publish).
  internal : Bool
  /// Do not expect a Declare-Ok reply.
  no_wait : Bool
  /// Exchange arguments, e.g. `"alternate-exchange"`.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Exchange.Delete (method 20): delete an exchange. The two bits are
/// packed into one octet (§4.2.5.2): if-unused, no-wait.
pub(all) struct ExchangeDelete {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The exchange name.
  exchange : String
  /// Only delete if the exchange has no bindings.
  if_unused : Bool
  /// Do not expect a Delete-Ok reply.
  no_wait : Bool
} derive(Eq, @debug.Debug)

///|
/// Exchange.Bind (method 30): bind two exchanges together.
pub(all) struct ExchangeBind {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The exchange that messages arrive on after binding.
  destination : String
  /// The exchange that messages arrive from.
  source : String
  /// Messages are routed from `source` to `destination` when they
  /// carry this routing key.
  routing_key : String
  /// Do not expect a Bind-Ok reply.
  no_wait : Bool
  /// Binding arguments, e.g. `"x-match"` for headers exchanges.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Exchange.Unbind (method 40): remove an exchange-to-exchange
/// binding.
pub(all) struct ExchangeUnbind {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The exchange that messages arrive on after binding.
  destination : String
  /// The exchange that messages arrive from.
  source : String
  /// The routing key the binding was created with.
  routing_key : String
  /// Do not expect an Unbind-Ok reply.
  no_wait : Bool
  /// Binding arguments.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Queue.Declare (method 10): declare a queue, or verify one exists
/// when `passive` is set. An empty `queue` asks the server for an
/// auto-generated name. The five bits are packed into one octet
/// (§4.2.5.2), least significant first: passive, durable, exclusive,
/// auto-delete, no-wait.
pub(all) struct QueueDeclare {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue name; empty means "generate a unique name".
  queue : String
  /// Only check that the queue exists; fail if it does not.
  passive : Bool
  /// Survive a broker restart.
  durable : Bool
  /// Only usable by the declaring connection.
  exclusive : Bool
  /// Delete once the last consumer is gone.
  auto_delete : Bool
  /// Do not expect a Declare-Ok reply.
  no_wait : Bool
  /// Queue arguments, e.g. `"x-max-priority"`, `"x-message-ttl"`.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Queue.Declare-Ok (method 11): the server confirms the declaration
/// and reports the queue's name and current usage.
pub(all) struct QueueDeclareOk {
  /// The queue name (auto-generated when the declare used "").
  queue : String
  /// Number of ready messages in the queue.
  message_count : UInt
  /// Number of consumers currently attached.
  consumer_count : UInt
} derive(Eq, @debug.Debug)

///|
/// Queue.Bind (method 20): bind a queue to an exchange.
pub(all) struct QueueBind {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue name.
  queue : String
  /// The exchange to bind to.
  exchange : String
  /// Messages carrying this routing key are routed to the queue.
  routing_key : String
  /// Do not expect a Bind-Ok reply.
  no_wait : Bool
  /// Binding arguments, e.g. `"x-match"` for headers exchanges.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// Queue.Purge (method 30): drop all ready messages from a queue.
pub(all) struct QueuePurge {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue name.
  queue : String
  /// Do not expect a Purge-Ok reply.
  no_wait : Bool
} derive(Eq, @debug.Debug)

///|
/// Queue.Purge-Ok (method 31): confirms the purge.
pub(all) struct QueuePurgeOk {
  /// Number of messages that were dropped.
  message_count : UInt
} derive(Eq, @debug.Debug)

///|
/// Queue.Delete (method 40): delete a queue. The three bits are
/// packed into one octet (§4.2.5.2): if-unused, if-empty, no-wait.
pub(all) struct QueueDelete {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue name.
  queue : String
  /// Only delete if the queue has no consumers.
  if_unused : Bool
  /// Only delete if the queue is empty.
  if_empty : Bool
  /// Do not expect a Delete-Ok reply.
  no_wait : Bool
} derive(Eq, @debug.Debug)

///|
/// Queue.Delete-Ok (method 41): confirms the deletion.
pub(all) struct QueueDeleteOk {
  /// Number of messages that were deleted with the queue.
  message_count : UInt
} derive(Eq, @debug.Debug)

///|
/// Queue.Unbind (method 50): remove a queue-to-exchange binding.
/// Unlike the other unbind methods it carries no `no_wait` bit.
pub(all) struct QueueUnbind {
  /// Reserved: must be 0 (a leftover ticket from AMQP 0-8).
  reserved_1 : UInt16
  /// The queue name.
  queue : String
  /// The exchange the binding was created with.
  exchange : String
  /// The routing key the binding was created with.
  routing_key : String
  /// Binding arguments.
  arguments : @table.FieldTable
} derive(Eq, @debug.Debug)

///|
/// One of the channel- (20), exchange- (40) or queue-class (50)
/// methods (§3.2.2): channel lifecycle and topology declarations.
/// The payload wire form is: class id, method id, arguments.
pub(all) enum ChannelMethod {
  ChannelOpen(ChannelOpen)
  ChannelOpenOk(ChannelOpenOk)
  ChannelFlow(ChannelFlow)
  ChannelFlowOk(ChannelFlowOk)
  ChannelClose(ChannelClose)
  ChannelCloseOk
  ExchangeDeclare(ExchangeDeclare)
  ExchangeDeclareOk
  ExchangeDelete(ExchangeDelete)
  ExchangeDeleteOk
  ExchangeBind(ExchangeBind)
  ExchangeBindOk
  ExchangeUnbind(ExchangeUnbind)
  ExchangeUnbindOk
  QueueDeclare(QueueDeclare)
  QueueDeclareOk(QueueDeclareOk)
  QueueBind(QueueBind)
  QueueBindOk
  QueuePurge(QueuePurge)
  QueuePurgeOk(QueuePurgeOk)
  QueueDelete(QueueDelete)
  QueueDeleteOk(QueueDeleteOk)
  QueueUnbind(QueueUnbind)
  QueueUnbindOk
} derive(Eq, @debug.Debug)

///|
/// Append the method's wire form (class id, method id, arguments) to `w`.
///
/// Raises `ChannelError::ValueOutOfRange` when a value does not fit
/// its wire type (e.g. a short string over 255 bytes) or a reserved
/// field is not its fixed spec value, and `ChannelError::InvalidUtf8`
/// for strings that cannot be UTF-8 encoded.
pub fn ChannelMethod::encode(
  self : ChannelMethod,
  w : @codec.Writer,
) -> Unit raise ChannelError {
  match self {
    // Class channel (20).
    ChannelMethod::ChannelOpen(s) => {
      write_method_header(w, channel_class_id, (10).to_uint16())
      require_empty_reserved("channel.open", s.reserved_1)
      write_shortstr_checked(w, s.reserved_1)
    }
    ChannelMethod::ChannelOpenOk(s) => {
      write_method_header(w, channel_class_id, (11).to_uint16())
      require_empty_reserved("channel.open-ok", s.reserved_1)
      write_longstr_checked(w, s.reserved_1)
    }
    ChannelMethod::ChannelFlow(s) => {
      write_method_header(w, channel_class_id, (20).to_uint16())
      w.write_bool(s.active)
    }
    ChannelMethod::ChannelFlowOk(s) => {
      write_method_header(w, channel_class_id, (21).to_uint16())
      w.write_bool(s.active)
    }
    ChannelMethod::ChannelClose(s) => {
      write_method_header(w, channel_class_id, (40).to_uint16())
      w.write_u16(s.reply_code)
      write_shortstr_checked(w, s.reply_text)
      w.write_u16(s.class_id)
      w.write_u16(s.method_id)
    }
    ChannelMethod::ChannelCloseOk =>
      write_method_header(w, channel_class_id, (41).to_uint16())
    // Class exchange (40).
    ChannelMethod::ExchangeDeclare(s) => {
      write_method_header(w, exchange_class_id, (10).to_uint16())
      require_zero_reserved("exchange.declare", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.exchange)
      write_shortstr_checked(w, s.exchange_type)
      w.write_byte(
        pack_bits([s.passive, s.durable, s.auto_delete, s.internal, s.no_wait]),
      )
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::ExchangeDeclareOk =>
      write_method_header(w, exchange_class_id, (11).to_uint16())
    ChannelMethod::ExchangeDelete(s) => {
      write_method_header(w, exchange_class_id, (20).to_uint16())
      require_zero_reserved("exchange.delete", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.exchange)
      w.write_byte(pack_bits([s.if_unused, s.no_wait]))
    }
    ChannelMethod::ExchangeDeleteOk =>
      write_method_header(w, exchange_class_id, (21).to_uint16())
    ChannelMethod::ExchangeBind(s) => {
      write_method_header(w, exchange_class_id, (30).to_uint16())
      require_zero_reserved("exchange.bind", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.destination)
      write_shortstr_checked(w, s.source)
      write_shortstr_checked(w, s.routing_key)
      w.write_byte(pack_bits([s.no_wait]))
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::ExchangeBindOk =>
      write_method_header(w, exchange_class_id, (31).to_uint16())
    ChannelMethod::ExchangeUnbind(s) => {
      write_method_header(w, exchange_class_id, (40).to_uint16())
      require_zero_reserved("exchange.unbind", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.destination)
      write_shortstr_checked(w, s.source)
      write_shortstr_checked(w, s.routing_key)
      w.write_byte(pack_bits([s.no_wait]))
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::ExchangeUnbindOk =>
      write_method_header(w, exchange_class_id, (51).to_uint16())
    // Class queue (50).
    ChannelMethod::QueueDeclare(s) => {
      write_method_header(w, queue_class_id, (10).to_uint16())
      require_zero_reserved("queue.declare", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.queue)
      w.write_byte(
        pack_bits([s.passive, s.durable, s.exclusive, s.auto_delete, s.no_wait]),
      )
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::QueueDeclareOk(s) => {
      write_method_header(w, queue_class_id, (11).to_uint16())
      write_shortstr_checked(w, s.queue)
      w.write_u32(s.message_count)
      w.write_u32(s.consumer_count)
    }
    ChannelMethod::QueueBind(s) => {
      write_method_header(w, queue_class_id, (20).to_uint16())
      require_zero_reserved("queue.bind", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.queue)
      write_shortstr_checked(w, s.exchange)
      write_shortstr_checked(w, s.routing_key)
      w.write_byte(pack_bits([s.no_wait]))
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::QueueBindOk =>
      write_method_header(w, queue_class_id, (21).to_uint16())
    ChannelMethod::QueuePurge(s) => {
      write_method_header(w, queue_class_id, (30).to_uint16())
      require_zero_reserved("queue.purge", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.queue)
      w.write_byte(pack_bits([s.no_wait]))
    }
    ChannelMethod::QueuePurgeOk(s) => {
      write_method_header(w, queue_class_id, (31).to_uint16())
      w.write_u32(s.message_count)
    }
    ChannelMethod::QueueDelete(s) => {
      write_method_header(w, queue_class_id, (40).to_uint16())
      require_zero_reserved("queue.delete", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.queue)
      w.write_byte(pack_bits([s.if_unused, s.if_empty, s.no_wait]))
    }
    ChannelMethod::QueueDeleteOk(s) => {
      write_method_header(w, queue_class_id, (41).to_uint16())
      w.write_u32(s.message_count)
    }
    ChannelMethod::QueueUnbind(s) => {
      write_method_header(w, queue_class_id, (50).to_uint16())
      require_zero_reserved("queue.unbind", s.reserved_1)
      w.write_u16(s.reserved_1)
      write_shortstr_checked(w, s.queue)
      write_shortstr_checked(w, s.exchange)
      write_shortstr_checked(w, s.routing_key)
      write_table_checked(w, s.arguments)
    }
    ChannelMethod::QueueUnbindOk =>
      write_method_header(w, queue_class_id, (51).to_uint16())
  }
}

///|
/// Read one channel-, exchange- or queue-class method from `r`,
/// dispatching on the class id and then the method id.
///
/// Raises `ChannelError::UnexpectedClass` when the class-id is not 20,
/// 40 or 50, `ChannelError::UnknownMethod` for a method-id outside the
/// given class, `ChannelError::Truncated` for input that ends early,
/// `ChannelError::InvalidUtf8` for malformed strings, and
/// `ChannelError::InvalidTable` for a malformed field table.
pub fn ChannelMethod::decode(
  r : @codec.Reader,
) -> ChannelMethod raise ChannelError {
  let class_id = next_u16(r)
  let method_id = next_u16(r)
  match class_id.to_uint().reinterpret_as_int() {
    20 => decode_channel_method(r, method_id)
    40 => decode_exchange_method(r, method_id)
    50 => decode_queue_method(r, method_id)
    _ => raise ChannelError::UnexpectedClass(class_id)
  }
}

///|
/// Channel-class (20) method dispatch.
fn decode_channel_method(
  r : @codec.Reader,
  method_id : UInt16,
) -> ChannelMethod raise ChannelError {
  match method_id.to_uint().reinterpret_as_int() {
    10 =>
      ChannelMethod::ChannelOpen(ChannelOpen::{ reserved_1: next_shortstr(r), })
    11 =>
      ChannelMethod::ChannelOpenOk(ChannelOpenOk::{
        reserved_1: next_longstr(r),
      })
    20 => ChannelMethod::ChannelFlow(ChannelFlow::{ active: next_bool(r), })
    21 => ChannelMethod::ChannelFlowOk(ChannelFlowOk::{ active: next_bool(r), })
    40 =>
      ChannelMethod::ChannelClose(ChannelClose::{
        reply_code: next_u16(r),
        reply_text: next_shortstr(r),
        class_id: next_u16(r),
        method_id: next_u16(r),
      })
    41 => ChannelMethod::ChannelCloseOk
    _ => raise ChannelError::UnknownMethod(channel_class_id, method_id)
  }
}

///|
/// Exchange-class (40) method dispatch.
fn decode_exchange_method(
  r : @codec.Reader,
  method_id : UInt16,
) -> ChannelMethod raise ChannelError {
  match method_id.to_uint().reinterpret_as_int() {
    10 => {
      let reserved_1 = next_u16(r)
      let exchange = next_shortstr(r)
      let exchange_type = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::ExchangeDeclare(ExchangeDeclare::{
        reserved_1,
        exchange,
        exchange_type,
        passive: (flags & 1) != 0,
        durable: (flags & 2) != 0,
        auto_delete: (flags & 4) != 0,
        internal: (flags & 8) != 0,
        no_wait: (flags & 16) != 0,
        arguments: next_table(r),
      })
    }
    11 => ChannelMethod::ExchangeDeclareOk
    20 => {
      let reserved_1 = next_u16(r)
      let exchange = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::ExchangeDelete(ExchangeDelete::{
        reserved_1,
        exchange,
        if_unused: (flags & 1) != 0,
        no_wait: (flags & 2) != 0,
      })
    }
    21 => ChannelMethod::ExchangeDeleteOk
    30 => {
      let reserved_1 = next_u16(r)
      let destination = next_shortstr(r)
      let source = next_shortstr(r)
      let routing_key = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::ExchangeBind(ExchangeBind::{
        reserved_1,
        destination,
        source,
        routing_key,
        no_wait: (flags & 1) != 0,
        arguments: next_table(r),
      })
    }
    31 => ChannelMethod::ExchangeBindOk
    40 => {
      let reserved_1 = next_u16(r)
      let destination = next_shortstr(r)
      let source = next_shortstr(r)
      let routing_key = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::ExchangeUnbind(ExchangeUnbind::{
        reserved_1,
        destination,
        source,
        routing_key,
        no_wait: (flags & 1) != 0,
        arguments: next_table(r),
      })
    }
    51 => ChannelMethod::ExchangeUnbindOk
    _ => raise ChannelError::UnknownMethod(exchange_class_id, method_id)
  }
}

///|
/// Queue-class (50) method dispatch.
fn decode_queue_method(
  r : @codec.Reader,
  method_id : UInt16,
) -> ChannelMethod raise ChannelError {
  match method_id.to_uint().reinterpret_as_int() {
    10 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::QueueDeclare(QueueDeclare::{
        reserved_1,
        queue,
        passive: (flags & 1) != 0,
        durable: (flags & 2) != 0,
        exclusive: (flags & 4) != 0,
        auto_delete: (flags & 8) != 0,
        no_wait: (flags & 16) != 0,
        arguments: next_table(r),
      })
    }
    11 =>
      ChannelMethod::QueueDeclareOk(QueueDeclareOk::{
        queue: next_shortstr(r),
        message_count: next_u32(r),
        consumer_count: next_u32(r),
      })
    20 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let exchange = next_shortstr(r)
      let routing_key = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::QueueBind(QueueBind::{
        reserved_1,
        queue,
        exchange,
        routing_key,
        no_wait: (flags & 1) != 0,
        arguments: next_table(r),
      })
    }
    21 => ChannelMethod::QueueBindOk
    30 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::QueuePurge(QueuePurge::{
        reserved_1,
        queue,
        no_wait: (flags & 1) != 0,
      })
    }
    31 =>
      ChannelMethod::QueuePurgeOk(QueuePurgeOk::{ message_count: next_u32(r), })
    40 => {
      let reserved_1 = next_u16(r)
      let queue = next_shortstr(r)
      let flags = next_byte(r).to_int()
      ChannelMethod::QueueDelete(QueueDelete::{
        reserved_1,
        queue,
        if_unused: (flags & 1) != 0,
        if_empty: (flags & 2) != 0,
        no_wait: (flags & 4) != 0,
      })
    }
    41 =>
      ChannelMethod::QueueDeleteOk(QueueDeleteOk::{
        message_count: next_u32(r),
      })
    50 =>
      ChannelMethod::QueueUnbind(QueueUnbind::{
        reserved_1: next_u16(r),
        queue: next_shortstr(r),
        exchange: next_shortstr(r),
        routing_key: next_shortstr(r),
        arguments: next_table(r),
      })
    51 => ChannelMethod::QueueUnbindOk
    _ => raise ChannelError::UnknownMethod(queue_class_id, method_id)
  }
}

///|
/// `read_byte` mapped onto `ChannelError`.
fn next_byte(r : @codec.Reader) -> Byte raise ChannelError {
  r.read_byte() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `read_bool` mapped onto `ChannelError`.
fn next_bool(r : @codec.Reader) -> Bool raise ChannelError {
  r.read_bool() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `read_u16` mapped onto `ChannelError`.
fn next_u16(r : @codec.Reader) -> UInt16 raise ChannelError {
  r.read_u16() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `read_u32` mapped onto `ChannelError`.
fn next_u32(r : @codec.Reader) -> UInt raise ChannelError {
  r.read_u32() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `read_shortstr` mapped onto `ChannelError`.
fn next_shortstr(r : @codec.Reader) -> String raise ChannelError {
  r.read_shortstr() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `read_longstr` mapped onto `ChannelError`.
fn next_longstr(r : @codec.Reader) -> String raise ChannelError {
  r.read_longstr() catch {
    @codec.CodecError::OutOfRange => raise ChannelError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `FieldTable::decode` mapped onto `ChannelError`.
fn next_table(r : @codec.Reader) -> @table.FieldTable raise ChannelError {
  @table.FieldTable::decode(r) catch {
    @table.TableError::Truncated => raise ChannelError::Truncated
    @table.TableError::InvalidUtf8 => raise ChannelError::InvalidUtf8
    @table.TableError::InvalidTag(_) =>
      raise ChannelError::InvalidTable("unknown field tag in field table")
    @table.TableError::InvalidKey(m) => raise ChannelError::InvalidTable(m)
    @table.TableError::ValueOutOfRange(m) => raise ChannelError::InvalidTable(m)
  }
}

///|
/// Write the 2-byte class id and the 2-byte method id.
fn write_method_header(
  w : @codec.Writer,
  class_id : UInt16,
  method_id : UInt16,
) -> Unit {
  w.write_u16(class_id)
  w.write_u16(method_id)
}

///|
/// Pack bit fields into one octet (§4.2.5.2): the first bit listed in
/// the method's arguments occupies the least significant bit.
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
/// Enforce a reserved-1 field's fixed empty value (§3.2.2).
fn require_empty_reserved(
  method_name : String,
  value : String,
) -> Unit raise ChannelError {
  if value != "" {
    raise ChannelError::ValueOutOfRange(
      method_name + " reserved-1 must be the empty string",
    )
  }
}

///|
/// Enforce a reserved ticket field's fixed zero value (§3.2.2).
fn require_zero_reserved(
  method_name : String,
  value : UInt16,
) -> Unit raise ChannelError {
  if value != (0).to_uint16() {
    raise ChannelError::ValueOutOfRange(method_name + " reserved-1 must be 0")
  }
}

///|
/// `write_shortstr` mapped onto `ChannelError`.
fn write_shortstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise ChannelError {
  w.write_shortstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise ChannelError::ValueOutOfRange(
        "short string longer than 255 UTF-8 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `write_longstr` mapped onto `ChannelError`.
fn write_longstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise ChannelError {
  w.write_longstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise ChannelError::ValueOutOfRange(
        "long string longer than 2^32-1 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise ChannelError::InvalidUtf8
  }
}

///|
/// `FieldTable::encode` mapped onto `ChannelError`.
fn write_table_checked(
  w : @codec.Writer,
  t : @table.FieldTable,
) -> Unit raise ChannelError {
  t.encode(w) catch {
    @table.TableError::ValueOutOfRange(m) =>
      raise ChannelError::ValueOutOfRange(m)
    @table.TableError::InvalidUtf8 => raise ChannelError::InvalidUtf8
    @table.TableError::InvalidKey(m) => raise ChannelError::ValueOutOfRange(m)
    _ => raise ChannelError::InvalidTable("unexpected table encode error")
  }
}
