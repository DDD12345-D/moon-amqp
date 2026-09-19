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
/// End to end: a published message travels as the frame sequence the
/// wire carries — a method frame, a content header frame, then body
/// frames (§4.2.6) — and each payload dispatches back to its typed
/// value.
test "publish travels through frames" {
  let publish = BasicMethod::BasicPublish(BasicPublish::{
    reserved_1: (0).to_uint16(),
    exchange: "logs",
    routing_key: "info",
    mandatory: true,
    immediate: false,
  })
  let method_payload = @codec.Writer::new()
  publish.encode(method_payload)
  let header = ContentHeader::{
    body_size: (5).to_uint64(),
    properties: BasicProperties::{
      content_type: Some("text/plain"),
      content_encoding: None,
      headers: None,
      delivery_mode: Some((2).to_byte()),
      priority: None,
      correlation_id: Some("abc"),
      reply_to: None,
      expiration: None,
      message_id: None,
      timestamp: None,
      message_type: None,
      user_id: None,
      app_id: None,
      cluster_id: None,
    },
  }
  let header_payload = @codec.Writer::new()
  header.encode(header_payload)
  let body = b"hello"

  let out = @codec.Writer::new()
  @frame.Frame::Method((1).to_uint16(), method_payload.to_bytes()).encode(out)
  @frame.Frame::Header((1).to_uint16(), header_payload.to_bytes()).encode(out)
  @frame.Frame::Body((1).to_uint16(), body).encode(out)

  let r = @codec.Reader::new(out.to_bytes())
  match @frame.Frame::decode(r) {
    @frame.Frame::Method(channel, payload) => {
      @test.assert_eq(channel, (1).to_uint16())
      @test.assert_eq(basic_dec(payload), publish)
    }
    _ => @test.assert_eq(0, 1)
  }
  match @frame.Frame::decode(r) {
    @frame.Frame::Header(channel, payload) => {
      @test.assert_eq(channel, (1).to_uint16())
      @test.assert_eq(header_dec(payload), header)
    }
    _ => @test.assert_eq(0, 1)
  }
  match @frame.Frame::decode(r) {
    @frame.Frame::Body(channel, payload) => {
      @test.assert_eq(channel, (1).to_uint16())
      @test.assert_eq(payload, body)
    }
    _ => @test.assert_eq(0, 1)
  }
}
