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
/// Encode one exchange-class method to its payload bytes.
fn exch_enc(m : @channel.ChannelMethod) -> Bytes raise @channel.ChannelError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// Golden encode for every exchange-class method (40): the vectors are
/// hand-derived from §3.2.2 — class id, method id, arguments. The bit
/// fields pack into one octet, least significant first (§4.2.5.2).
test "exchange methods golden encode" {
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        exchange_type: "fanout",
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x28\x00\x0a\x00\x00\x04logs\x06fanout\x02\x00\x00\x00\x00",
  )
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (0).to_uint16(),
        exchange: "e",
        exchange_type: "t",
        passive: false,
        durable: false,
        auto_delete: false,
        internal: true,
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x28\x00\x0a\x00\x00\x01e\x01t\x18\x00\x00\x00\x00",
  )
  let args = @table.FieldTable::new()
  args.set("b", @table.FieldValue::Bool(true))
  args.set("a", @table.FieldValue::Bool(false))
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (0).to_uint16(),
        exchange: "e",
        exchange_type: "t",
        passive: false,
        durable: false,
        auto_delete: false,
        internal: false,
        no_wait: false,
        arguments: args,
      }),
    ),
    b"\x00\x28\x00\x0a\x00\x00\x01e\x01t\x00\x00\x00\x00\x08\x01a\x74\x00\x01b\x74\x01",
  )
  @test.assert_eq(
    exch_enc(@channel.ChannelMethod::ExchangeDeclareOk),
    b"\x00\x28\x00\x0b",
  )
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeDelete(@channel.ExchangeDelete::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        if_unused: false,
        no_wait: true,
      }),
    ),
    b"\x00\x28\x00\x14\x00\x00\x04logs\x02",
  )
  @test.assert_eq(
    exch_enc(@channel.ChannelMethod::ExchangeDeleteOk),
    b"\x00\x28\x00\x15",
  )
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeBind(@channel.ExchangeBind::{
        reserved_1: (0).to_uint16(),
        destination: "d",
        source: "s",
        routing_key: "k",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x28\x00\x1e\x00\x00\x01d\x01s\x01k\x00\x00\x00\x00\x00",
  )
  @test.assert_eq(
    exch_enc(@channel.ChannelMethod::ExchangeBindOk),
    b"\x00\x28\x00\x1f",
  )
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeUnbind(@channel.ExchangeUnbind::{
        reserved_1: (0).to_uint16(),
        destination: "d",
        source: "s",
        routing_key: "k",
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x28\x00\x28\x00\x00\x01d\x01s\x01k\x01\x00\x00\x00\x00",
  )
  @test.assert_eq(
    exch_enc(@channel.ChannelMethod::ExchangeUnbindOk),
    b"\x00\x28\x00\x33",
  )
}

///|
/// The reserved ticket field in exchange methods is fixed to 0.
test "exchange reserved-1 must be zero" {
  for
    bad in [
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (1).to_uint16(),
        exchange: "e",
        exchange_type: "t",
        passive: false,
        durable: false,
        auto_delete: false,
        internal: false,
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.ExchangeDelete(@channel.ExchangeDelete::{
        reserved_1: (1).to_uint16(),
        exchange: "e",
        if_unused: false,
        no_wait: false,
      }),
      @channel.ChannelMethod::@channel.ExchangeBind(@channel.ExchangeBind::{
        reserved_1: (1).to_uint16(),
        destination: "d",
        source: "s",
        routing_key: "k",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.ExchangeUnbind(@channel.ExchangeUnbind::{
        reserved_1: (1).to_uint16(),
        destination: "d",
        source: "s",
        routing_key: "k",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ] {
    let raised = try {
      let _ = exch_enc(bad)
      false
    } catch {
      @channel.ChannelError::ValueOutOfRange(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// Decode one exchange-class method payload.
fn exch_dec(
  payload : Bytes,
) -> @channel.ChannelMethod raise @channel.ChannelError {
  @channel.ChannelMethod::decode(@codec.Reader::new(payload))
}

///|
/// Golden decode: the golden encode vectors read back to their values,
/// including the bit fields unpacked from the flags octet.
test "exchange methods golden decode" {
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x0a\x00\x00\x04logs\x06fanout\x02\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
      reserved_1: (0).to_uint16(),
      exchange: "logs",
      exchange_type: "fanout",
      passive: false,
      durable: true,
      auto_delete: false,
      internal: false,
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x0a\x00\x00\x01e\x01t\x18\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
      reserved_1: (0).to_uint16(),
      exchange: "e",
      exchange_type: "t",
      passive: false,
      durable: false,
      auto_delete: false,
      internal: true,
      no_wait: true,
      arguments: @table.FieldTable::new(),
    }),
  )
  let args = @table.FieldTable::new()
  args.set("a", @table.FieldValue::Bool(false))
  args.set("b", @table.FieldValue::Bool(true))
  @test.assert_eq(
    exch_dec(
      b"\x00\x28\x00\x0a\x00\x00\x01e\x01t\x00\x00\x00\x00\x08\x01a\x74\x00\x01b\x74\x01",
    ),
    @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
      reserved_1: (0).to_uint16(),
      exchange: "e",
      exchange_type: "t",
      passive: false,
      durable: false,
      auto_delete: false,
      internal: false,
      no_wait: false,
      arguments: args,
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x0b"),
    @channel.ChannelMethod::ExchangeDeclareOk,
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x14\x00\x00\x04logs\x02"),
    @channel.ChannelMethod::@channel.ExchangeDelete(@channel.ExchangeDelete::{
      reserved_1: (0).to_uint16(),
      exchange: "logs",
      if_unused: false,
      no_wait: true,
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x15"),
    @channel.ChannelMethod::ExchangeDeleteOk,
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x1e\x00\x00\x01d\x01s\x01k\x00\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.ExchangeBind(@channel.ExchangeBind::{
      reserved_1: (0).to_uint16(),
      destination: "d",
      source: "s",
      routing_key: "k",
      no_wait: false,
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x1f"),
    @channel.ChannelMethod::ExchangeBindOk,
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x28\x00\x00\x01d\x01s\x01k\x01\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.ExchangeUnbind(@channel.ExchangeUnbind::{
      reserved_1: (0).to_uint16(),
      destination: "d",
      source: "s",
      routing_key: "k",
      no_wait: true,
      arguments: @table.FieldTable::new(),
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x33"),
    @channel.ChannelMethod::ExchangeUnbindOk,
  )
}

///|
/// Every exchange-class method survives an encode/decode roundtrip.
test "exchange methods roundtrip" {
  let args = @table.FieldTable::new()
  args.set("x-match", @table.FieldValue::ShortStr("all"))
  for
    m in [
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        exchange_type: "fanout",
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::@channel.ExchangeDeclare(@channel.ExchangeDeclare::{
        reserved_1: (0).to_uint16(),
        exchange: "h",
        exchange_type: "headers",
        passive: false,
        durable: false,
        auto_delete: true,
        internal: false,
        no_wait: true,
        arguments: args,
      }),
      @channel.ChannelMethod::ExchangeDeclareOk,
      @channel.ChannelMethod::@channel.ExchangeDelete(@channel.ExchangeDelete::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        if_unused: true,
        no_wait: true,
      }),
      @channel.ChannelMethod::ExchangeDeleteOk,
      @channel.ChannelMethod::@channel.ExchangeBind(@channel.ExchangeBind::{
        reserved_1: (0).to_uint16(),
        destination: "dest",
        source: "src",
        routing_key: "route.#",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::ExchangeBindOk,
      @channel.ChannelMethod::@channel.ExchangeUnbind(@channel.ExchangeUnbind::{
        reserved_1: (0).to_uint16(),
        destination: "dest",
        source: "src",
        routing_key: "route.#",
        no_wait: true,
        arguments: @table.FieldTable::new(),
      }),
      @channel.ChannelMethod::ExchangeUnbindOk,
    ] {
    @test.assert_eq(exch_dec(exch_enc(m)), m)
  }
}
