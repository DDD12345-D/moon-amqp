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
