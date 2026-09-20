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
/// Encode one content header to its payload bytes.
fn header_enc(h : @basic.ContentHeader) -> Bytes raise @basic.BasicError {
  let w = @codec.Writer::new()
  h.encode(w)
  w.to_bytes()
}

///|
/// Golden encode for the content header (§4.2.6.1): class id 60,
/// weight 0, body size, property flags, then the present properties
/// in wire order (high flag bit first). The vectors are hand-derived.
test "content header golden encode" {
  @test.assert_eq(
    header_enc(@basic.ContentHeader::{
      body_size: (5).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: Some("text/plain"),
        content_encoding: None,
        headers: None,
        delivery_mode: Some((2).to_byte()),
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
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x90\x00\x0atext/plain\x02",
  )
  @test.assert_eq(
    header_enc(@basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::new(),
    }),
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
  )
  let headers = @table.FieldTable::new()
  headers.set("x-count", @table.FieldValue::I8(3))
  @test.assert_eq(
    header_enc(@basic.ContentHeader::{
      body_size: (3).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: Some(headers),
        delivery_mode: None,
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
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x26\x00\x00\x00\x00\x0a\x07x-count\x62\x03\x03abc\x02me",
  )
  @test.assert_eq(
    header_enc(@basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: None,
        delivery_mode: None,
        priority: None,
        correlation_id: None,
        reply_to: None,
        expiration: None,
        message_id: None,
        timestamp: Some((1726740000).to_uint64()),
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
      },
    }),
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x66\xeb\xf6\x20",
  )
  @test.assert_eq(
    header_enc(@basic.ContentHeader::{
      body_size: (1).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: None,
        delivery_mode: None,
        priority: Some((9).to_byte()),
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
    b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x08\x00\x09",
  )
}

///|
/// The spec fixes cluster-id to the empty string when present.
test "non-empty cluster-id raises" {
  let raised = try {
    let _ = header_enc(@basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: None,
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
        cluster_id: Some("c1"),
      },
    })
    false
  } catch {
    @basic.BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Decode one content header payload.
fn header_dec(payload : Bytes) -> @basic.ContentHeader raise @basic.BasicError {
  @basic.ContentHeader::decode(@codec.Reader::new(payload))
}

///|
/// Golden decode: the golden encode vectors read back to their values.
test "content header golden decode" {
  @test.assert_eq(
    header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x90\x00\x0atext/plain\x02",
    ),
    @basic.ContentHeader::{
      body_size: (5).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: Some("text/plain"),
        content_encoding: None,
        headers: None,
        delivery_mode: Some((2).to_byte()),
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
  @test.assert_eq(
    header_dec(b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
    @basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::new(),
    },
  )
  let headers = @table.FieldTable::new()
  headers.set("x-count", @table.FieldValue::I8(3))
  @test.assert_eq(
    header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x26\x00\x00\x00\x00\x0a\x07x-count\x62\x03\x03abc\x02me",
    ),
    @basic.ContentHeader::{
      body_size: (3).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: Some(headers),
        delivery_mode: None,
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
    },
  )
  @test.assert_eq(
    header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x66\xeb\xf6\x20",
    ),
    @basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: None,
        delivery_mode: None,
        priority: None,
        correlation_id: None,
        reply_to: None,
        expiration: None,
        message_id: None,
        timestamp: Some((1726740000).to_uint64()),
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
      },
    },
  )
  @test.assert_eq(
    header_dec(b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x08\x00\x09"),
    @basic.ContentHeader::{
      body_size: (1).to_uint64(),
      properties: @basic.BasicProperties::{
        content_type: None,
        content_encoding: None,
        headers: None,
        delivery_mode: None,
        priority: Some((9).to_byte()),
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

///|
/// Headers survive an encode/decode roundtrip, including one with
/// all fourteen properties set and one with scattered bits
/// (content-encoding, expiration, message-id, app-id).
test "content header roundtrip" {
  let headers = @table.FieldTable::new()
  headers.set("x-death-count", @table.FieldValue::I32(2))
  let all = @basic.BasicProperties::{
    content_type: Some("application/json"),
    content_encoding: Some("gzip"),
    headers: Some(headers),
    delivery_mode: Some((2).to_byte()),
    priority: Some((7).to_byte()),
    correlation_id: Some("corr-1"),
    reply_to: Some("amq.rabbitmq.reply-to"),
    expiration: Some("60000"),
    message_id: Some("msg-1"),
    timestamp: Some((1726740000).to_uint64()),
    message_type: Some("order"),
    user_id: Some("guest"),
    app_id: Some("moon-amqp"),
    cluster_id: Some(""),
  }
  let scattered = @basic.BasicProperties::{
    content_type: None,
    content_encoding: Some("br"),
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: None,
    reply_to: None,
    expiration: Some("1000"),
    message_id: Some("m"),
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: Some("app"),
    cluster_id: None,
  }
  for
    h in [
      @basic.ContentHeader::{ body_size: (4096).to_uint64(), properties: all, },
      @basic.ContentHeader::{
        body_size: (0).to_uint64(),
        properties: scattered,
      },
    ] {
    @test.assert_eq(header_dec(header_enc(h)), h)
  }
}

///|
/// A header whose class id is not 60 is rejected with the offending
/// id.
test "unexpected class raises" {
  let err = try {
    let _ = header_dec(
      b"\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    )
    None
  } catch {
    @basic.BasicError::UnexpectedClass(c) => Some(c)
    _ => None
  }
  @test.assert_eq(err, Some((20).to_uint16()))
}

///|
/// Input that ends before the header is complete is `Truncated`.
test "truncated input raises" {
  for
    payload in [
      b"", b"\x00\x3c", b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    ] {
    let raised = try {
      let _ = header_dec(payload)
      false
    } catch {
      @basic.BasicError::Truncated => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// A continuation flag field (§4.2.6.1, bit 0) is consumed; the
/// header still reads as empty.
test "continuation flag field is consumed" {
  @test.assert_eq(
    header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00",
    ),
    @basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::new(),
    },
  )
}

///|
/// Unknown flag bits carry no data and are ignored.
test "unknown flag bits are ignored" {
  @test.assert_eq(
    header_dec(b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02"),
    @basic.ContentHeader::{
      body_size: (0).to_uint64(),
      properties: @basic.BasicProperties::new(),
    },
  )
}

///|
/// A short string property containing invalid UTF-8 is `InvalidUtf8`.
test "invalid utf-8 raises" {
  let raised = try {
    let _ = header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x02\xff\xfe",
    )
    false
  } catch {
    @basic.BasicError::InvalidUtf8 => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A headers table with an unknown value tag is `InvalidTable`.
test "malformed table raises" {
  let raised = try {
    let _ = header_dec(
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x03\x01k\x5a",
    )
    false
  } catch {
    @basic.BasicError::InvalidTable(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
