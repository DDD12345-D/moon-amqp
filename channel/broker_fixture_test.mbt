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
/// Bytes captured from a live RabbitMQ 4.3.6 broker (Erlang/OTP
/// 27.3.4.17): amqplib 2.0.1 opens a channel, declares an exchange
/// and a queue, binds, purges, and deletes both. The client-bound
/// payloads pin our encoder to the wire; the server-bound replies pin
/// our decoder. Recorded 2026-09-18.
test "broker fixture: client topology bytes" {
  @test.assert_eq(
    chan_enc(
      @channel.ChannelMethod::@channel.ChannelOpen(@channel.ChannelOpen::{
        reserved_1: "",
      }),
    ),
    b"\x00\x14\x00\x0a\x00",
  )
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
      @channel.ChannelMethod::@channel.QueueBind(@channel.QueueBind::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        exchange: "logs",
        routing_key: "info",
        no_wait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x32\x00\x14\x00\x00\x04jobs\x04logs\x04info\x00\x00\x00\x00\x00",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueuePurge(@channel.QueuePurge::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        no_wait: false,
      }),
    ),
    b"\x00\x32\x00\x1e\x00\x00\x04jobs\x00",
  )
  @test.assert_eq(
    queue_enc(
      @channel.ChannelMethod::@channel.QueueDelete(@channel.QueueDelete::{
        reserved_1: (0).to_uint16(),
        queue: "jobs",
        if_unused: false,
        if_empty: false,
        no_wait: false,
      }),
    ),
    b"\x00\x32\x00\x28\x00\x00\x04jobs\x00",
  )
  @test.assert_eq(
    exch_enc(
      @channel.ChannelMethod::@channel.ExchangeDelete(@channel.ExchangeDelete::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        if_unused: false,
        no_wait: false,
      }),
    ),
    b"\x00\x28\x00\x14\x00\x00\x04logs\x00",
  )
}

///|
/// The server-bound replies from the same session decode back to the
/// expected typed methods.
test "broker fixture: server reply bytes" {
  @test.assert_eq(
    chan_dec(b"\x00\x14\x00\x0b\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.ChannelOpenOk(@channel.ChannelOpenOk::{
      reserved_1: "",
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x0b"),
    @channel.ChannelMethod::ExchangeDeclareOk,
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x0b\x04jobs\x00\x00\x00\x00\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueDeclareOk(@channel.QueueDeclareOk::{
      queue: "jobs",
      message_count: 0,
      consumer_count: 0,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x15"),
    @channel.ChannelMethod::QueueBindOk,
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x1f\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueuePurgeOk(@channel.QueuePurgeOk::{
      message_count: 0,
    }),
  )
  @test.assert_eq(
    queue_dec(b"\x00\x32\x00\x29\x00\x00\x00\x00"),
    @channel.ChannelMethod::@channel.QueueDeleteOk(@channel.QueueDeleteOk::{
      message_count: 0,
    }),
  )
  @test.assert_eq(
    exch_dec(b"\x00\x28\x00\x15"),
    @channel.ChannelMethod::ExchangeDeleteOk,
  )
}
