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
/// 27.3.4.17): amqplib 2.0.1 publishes two messages through a proxy.
/// The first carries the five acceptance properties (content-type,
/// headers, delivery-mode, correlation-id, reply-to) plus a body; the
/// second is mandatory to an exchange with no binding, so the broker
/// sends it back as basic.return. The client-bound bytes pin our
/// encoder to amqplib's output for the same message; the server-bound
/// bytes pin our decoder. Recorded 2026-09-19.
test "broker fixture: publish bytes match amqplib" {
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "logs",
        routing_key: "info",
        mandatory: false,
        immediate: false,
      }),
    ),
    b"\x00\x3c\x00\x28\x00\x00\x04logs\x04info\x00",
  )
  let headers = @table.FieldTable::new()
  headers.set("x-count", @table.FieldValue::I8(3))
  @test.assert_eq(
    header_enc(ContentHeader::{
      body_size: (5).to_uint64(),
      properties: BasicProperties::{
        content_type: Some("text/plain"),
        content_encoding: None,
        headers: Some(headers),
        delivery_mode: Some((2).to_byte()),
        priority: None,
        correlation_id: Some("abc"),
        reply_to: Some("me"),
        expiration: None,
        message_id: None,
        timestamp: None,
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
      },
    }),
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\xb6\x00\x0atext/plain\x00\x00\x00\x0a\x07x-count\x62\x03\x02\x03abc\x02me",
  )
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicPublish(BasicPublish::{
        reserved_1: (0).to_uint16(),
        exchange: "unrouted",
        routing_key: "nobody",
        mandatory: true,
        immediate: false,
      }),
    ),
    b"\x00\x3c\x00\x28\x00\x00\x08unrouted\x06nobody\x01",
  )
  // amqplib sends an empty headers table as "present but empty".
  @test.assert_eq(
    header_enc(ContentHeader::{
      body_size: (4).to_uint64(),
      properties: BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: Some(@table.FieldTable::new()),
        delivery_mode: None,
        priority: None,
        correlation_id: None,
        reply_to: None,
        expiration: None,
        message_id: None,
        timestamp: None,
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
      },
    }),
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x20\x00\x00\x00\x00\x00",
  )
}

///|
/// The server-bound frames from the same session decode back to the
/// expected typed values: the unroutable message comes back as
/// basic.return with the header RabbitMQ echoed.
test "broker fixture: return bytes decode" {
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x32\x01\x38\x08NO_ROUTE\x08unrouted\x06nobody"),
    BasicMethod::BasicReturn(BasicReturn::{
      reply_code: (312).to_uint16(),
      reply_text: "NO_ROUTE",
      exchange: "unrouted",
      routing_key: "nobody",
    }),
  )
  @test.assert_eq(
    header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x20\x00\x00\x00\x00\x00",
    ),
    ContentHeader::{
      body_size: (4).to_uint64(),
      properties: BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: Some(@table.FieldTable::new()),
        delivery_mode: None,
        priority: None,
        correlation_id: None,
        reply_to: None,
        expiration: None,
        message_id: None,
        timestamp: None,
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
      },
    },
  )
}
