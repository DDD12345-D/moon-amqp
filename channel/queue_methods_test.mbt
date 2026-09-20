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
/// Encode one queue-class method to its payload bytes.
fn queue_enc(m : @channel.ChannelMethod) -> Bytes raise @channel.ChannelError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// Golden encode for every queue-class method (50): the vectors are
/// hand-derived from §3.2.2 — class id, method id, arguments. The bit
/// fields pack into one octet, least significant first (§4.2.5.2).
test "queue methods golden encode" {
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x32\x00\x0a\x00\x00\x04jobs\x02\x00\x00\x00\x00",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (0).to_uint16(),
        queue: "",
        passive: true,
        durable: true,
        exclusive: true,
        auto_delete: true,
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x32\x00\x0a\x00\x00\x00\x1f\x00\x00\x00\x00",
  )
  let args = @table.FieldTable::new()
  args.set("x-max-priority", @table.FieldValue::I8(10))
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: args,
      }),
    ),
    b"\x00\x32\x00\x0a\x00\x00\x04jobs\x02\x00\x00\x00\x11\x0ex-max-priority\x62\x0a",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDeclareOk(@channel.QueueDeclareOk::{
        queue: "amq.gen-abc",
        message_count: 42,
        consumer_count: 7,
      }),
    ),
    b"\x00\x32\x00\x0b\x0bamq.gen-abc\x00\x00\x00\x2a\x00\x00\x00\x07",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueBind(@channel.QueueBind::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "r",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x32\x00\x14\x00\x00\x01q\x02ex\x01r\x00\x00\x00\x00\x00",
  )
  @test.assert_eq(
    queue_enc(@channel.ChannelMethod::QueueBindOk),
    b"\x00\x32\x00\x15",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueuePurge(@channel.QueuePurge::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        no_wait: true,
      }),
    ),
    b"\x00\x32\x00\x1e\x00\x00\x01q\x01",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueuePurgeOk(@channel.QueuePurgeOk::{
        message_count: 0,
      }),
    ),
    b"\x00\x32\x00\x1f\x00\x00\x00\x00",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDelete(@channel.QueueDelete::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        if_unused: true,
        if_empty: true,
        no_wait: false,
      }),
    ),
    b"\x00\x32\x00\x28\x00\x00\x01q\x03",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDeleteOk(@channel.QueueDeleteOk::{
        message_count: 5,
      }),
    ),
    b"\x00\x32\x00\x29\x00\x00\x00\x05",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueUnbind(@channel.QueueUnbind::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "r",
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x32\x00\x32\x00\x00\x01q\x02ex\x01r\x00\x00\x00\x00",
  )
  @test.assert_eq(
    queue_enc(@channel.ChannelMethod::QueueUnbindOk),
    b"\x00\x32\x00\x33",
  )
}

///|
/// The reserved ticket field in queue methods is fixed to 0.
test "queue reserved-1 must be zero" {
  for
    bad in [
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        passive: false,
        durable: false,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.QueueBind(@channel.QueueBind::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "r",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.QueuePurge(@channel.QueuePurge::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        no_wait: false,
      }),
      @channel.ChannelMethod::@channel.QueueDelete(@channel.QueueDelete::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        if_unused: false,
        if_empty: false,
        no_wait: false,
      }),
      @channel.ChannelMethod::@channel.QueueUnbind(@channel.QueueUnbind::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "r",
        arguments: @table.FieldTable::new(),
      }),
    ] {
    let raised = try {
      let _ = queue_enc(bad)
      false
    } catch {
      @channel.ChannelError::ValueOutOfRange(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// Decode one queue-class method payload.
fn queue_dec(
  payload : Bytes,
) -> @channel.ChannelMethod raise @channel.ChannelError {
  @channel.ChannelMethod::decode(@codec.Reader::new(payload))
}

///|
/// Golden decode: the golden encode vectors read back to their values,
/// including the bit fields unpacked from the flags octet.
test "queue methods golden decode" {
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x0a\x00\x00\x04jobs\x02\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: "jobs",
      passive: false,
      durable: true,
      exclusive: false,
      auto_delete: false,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x0a\x00\x00\x00\x1f\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: "",
      passive: true,
      durable: true,
      exclusive: true,
      auto_delete: true,
      no_wait: true,
      arguments: @table.FieldTable::new(),
    }),
  )
  let args = @table.FieldTable::new()
  args.set("x-max-priority", @table.FieldValue::I8(10))
  @test.assert_eq(
    queue_dec(
      b"\x00\x32\x00\x0a\x00\x00\x04jobs\x02\x00\x00\x00\x11\x0ex-max-priority\x62\x0a",
    ),
    @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
      reserved_1: (0).to_uint16(),
      queue: "jobs",
      passive: false,
      durable: true,
      exclusive: false,
      auto_delete: false,
      no_wait: false,
      arguments: args,
    }),
  )
  @test.assert_eq(
    queue_dec(
      b"\x00\x32\x00\x0b\x0bamq.gen-abc\x00\x00\x00\x2a\x00\x00\x00\x07",
    ),
    @channel.ChannelMethod::@channel.QueueDeclareOk(@channel.QueueDeclareOk::{
      queue: "amq.gen-abc",
      message_count: 42,
      consumer_count: 7,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x14\x00\x00\x01q\x02ex\x01r\x00\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueBind(@channel.QueueBind::{
      reserved_1: (0).to_uint16(),
      queue: "q",
      exchange: "ex",
      routing_key: "r",
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x15"),
    @channel.ChannelMethod::QueueBindOk,
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x1e\x00\x00\x01q\x01"),
    @channel.ChannelMethod::@channel.QueuePurge(@channel.QueuePurge::{
      reserved_1: (0).to_uint16(),
      queue: "q",
      no_wait: true,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x1f\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueuePurgeOk(@channel.QueuePurgeOk::{
      message_count: 0,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x28\x00\x00\x01q\x03"),
    @channel.ChannelMethod::@channel.QueueDelete(@channel.QueueDelete::{
      reserved_1: (0).to_uint16(),
      queue: "q",
      if_unused: true,
      if_empty: true,
      no_wait: false,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x29\x00\x00\x00\x05"),
    @channel.ChannelMethod::@channel.QueueDeleteOk(@channel.QueueDeleteOk::{
      message_count: 5,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x32\x00\x00\x01q\x02ex\x01r\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueUnbind(@channel.QueueUnbind::{
      reserved_1: (0).to_uint16(),
      queue: "q",
      exchange: "ex",
      routing_key: "r",
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x33"),
    @channel.ChannelMethod::QueueUnbindOk,
  )
}

///|
/// Every queue-class method survives an encode/decode roundtrip.
test "queue methods roundtrip" {
  let args = @table.FieldTable::new()
  args.set("x-message-ttl", @table.FieldValue::I32(60000))
  for
    m in [
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: args,
      }),
      @channel.ChannelMethod::@channel.QueueDeclare(@channel.QueueDeclare::{
        reserved_1: (0).to_uint16(),
        queue: "",
        passive: false,
        durable: false,
        exclusive: true,
        auto_delete: true,
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.QueueDeclareOk(@channel.QueueDeclareOk::{
        queue: "amq.gen-x",
        message_count: 0,
        consumer_count: 3,
      }),
      @channel.ChannelMethod::@channel.QueueBind(@channel.QueueBind::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "route.#",
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::QueueBindOk,
      @channel.ChannelMethod::@channel.QueuePurge(@channel.QueuePurge::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        no_wait: false,
      }),
      @channel.ChannelMethod::@channel.QueuePurgeOk(@channel.QueuePurgeOk::{
        message_count: 12,
      }),
      @channel.ChannelMethod::@channel.QueueDelete(@channel.QueueDelete::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        if_unused: true,
        if_empty: false,
        no_wait: true,
      }),
      @channel.ChannelMethod::@channel.QueueDeleteOk(@channel.QueueDeleteOk::{
        message_count: 0,
      }),
      @channel.ChannelMethod::@channel.QueueUnbind(@channel.QueueUnbind::{
        reserved_1: (0).to_uint16(),
        queue: "q",
        exchange: "ex",
        routing_key: "r",
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::QueueUnbindOk,
    ] {
    @test.assert_eq(queue_dec(queue_enc(m)), m)
  }
}
