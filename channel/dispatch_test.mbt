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
/// Decode one method payload.
fn dispatch_dec(payload : Bytes) -> ChannelMethod raise ChannelError {
  ChannelMethod::decode(@codec.Reader::new(payload))
}

///|
/// Decoding dispatches on the class id: the same shape of payload
/// reaches a different method under each class.
test "class id dispatches to the right method" {
  @test.assert_eq(
    dispatch_dec(b"\x00\x14\x00\x0a\x00"),
    ChannelMethod::ChannelOpen(ChannelOpen::{ reserved_1: "", }),
  )
  @test.assert_eq(
    dispatch_dec(b"\x00\x28\x00\x0b"),
    ChannelMethod::ExchangeDeclareOk,
  )
  @test.assert_eq(
    dispatch_dec(b"\x00\x32\x00\x1f\x00\x00\x00\x00"),
    ChannelMethod::QueuePurgeOk(QueuePurgeOk::{ message_count: 0, }),
  )
}

///|
/// A payload whose class id is not 20, 40 or 50 is rejected with the
/// offending id — this lets a connection-level reader route methods to
/// the right package (connection vs channel vs basic).
test "unexpected class raises" {
  let err_conn = try {
    let _ = dispatch_dec(b"\x00\x0a\x00\x0a") // connection class
    None
  } catch {
    ChannelError::UnexpectedClass(c) => Some(c)
    _ => None
  }
  @test.assert_eq(err_conn, Some((10).to_uint16()))
  let err_basic = try {
    let _ = dispatch_dec(b"\x00\x3c\x00\x0a") // basic class
    None
  } catch {
    ChannelError::UnexpectedClass(c) => Some(c)
    _ => None
  }
  @test.assert_eq(err_basic, Some((60).to_uint16()))
}

///|
/// A method id outside its class reports both ids, so the caller can
/// tell exactly which (class, method) pair was unknown.
test "unknown method raises" {
  for
    (payload, cls, meth) in [
      (b"\x00\x14\x00\x63", 20, 99),
      (b"\x00\x28\x00\x63", 40, 99),
      (b"\x00\x32\x00\x63", 50, 99),
    ] {
    let err = try {
      let _ = dispatch_dec(payload)
      None
    } catch {
      ChannelError::UnknownMethod(c, m) => Some((c, m))
      _ => None
    }
    @test.assert_eq(err, Some((cls.to_uint16(), meth.to_uint16())))
  }
}

///|
/// Input that ends before the arguments are complete is `Truncated`.
test "truncated input raises" {
  for payload in [b"", b"\x00", b"\x00\x14", b"\x00\x14\x00\x0a"] {
    let raised = try {
      let _ = dispatch_dec(payload)
      false
    } catch {
      ChannelError::Truncated => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}

///|
/// A long string containing invalid UTF-8 is `InvalidUtf8`.
test "invalid utf-8 raises" {
  let raised = try {
    let _ = dispatch_dec(b"\x00\x14\x00\x0b\x00\x00\x00\x02\xff\xfe")
    false
  } catch {
    ChannelError::InvalidUtf8 => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// An argument table with an unknown value tag is `InvalidTable`.
test "malformed table raises" {
  let raised = try {
    let _ = dispatch_dec(
      b"\x00\x28\x00\x0a\x00\x00\x01e\x01t\x00\x00\x00\x00\x03\x01k\x5a",
    )
    false
  } catch {
    ChannelError::InvalidTable(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// End to end: a method travels inside a full frame — `Frame::decode`
/// hands the payload to `ChannelMethod::decode`, which dispatches on
/// the class id and hands back the typed method.
test "frame payload dispatches to a typed method" {
  let m = ChannelMethod::QueueBind(QueueBind::{
    reserved_1: (0).to_uint16(),
    queue: "q",
    exchange: "ex",
    routing_key: "r",
    no_wait: false,
    arguments: @table.FieldTable::new(),
  })
  let payload_w = @codec.Writer::new()
  m.encode(payload_w)
  let frame_w = @codec.Writer::new()
  @frame.Frame::Method((1).to_uint16(), payload_w.to_bytes()).encode(frame_w)
  match @frame.Frame::decode(@codec.Reader::new(frame_w.to_bytes())) {
    @frame.Frame::Method(channel, payload) => {
      @test.assert_eq(channel, (1).to_uint16())
      @test.assert_eq(dispatch_dec(payload), m)
    }
    _ => @test.assert_eq(0, 1)
  }
}
