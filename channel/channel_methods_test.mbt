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
/// Encode one channel-class method to its payload bytes.
fn chan_enc(m : ChannelMethod) -> Bytes raise ChannelError {
  let w = @codec.Writer::new()
  m.encode(w)
  w.to_bytes()
}

///|
/// Golden encode for every channel-class method (20): the vectors are
/// hand-derived from §3.2.2 — class id, method id, arguments.
test "channel methods golden encode" {
  @test.assert_eq(
    chan_enc(ChannelMethod::ChannelOpen(ChannelOpen::{ reserved_1: "", })),
    b"\x00\x14\x00\x0a\x00",
  )
  @test.assert_eq(
    chan_enc(ChannelMethod::ChannelOpenOk(ChannelOpenOk::{ reserved_1: "", })),
    b"\x00\x14\x00\x0b\x00\x00\x00\x00",
  )
  @test.assert_eq(
    chan_enc(ChannelMethod::ChannelFlow(ChannelFlow::{ active: true, })),
    b"\x00\x14\x00\x14\x01",
  )
  @test.assert_eq(
    chan_enc(ChannelMethod::ChannelFlow(ChannelFlow::{ active: false, })),
    b"\x00\x14\x00\x14\x00",
  )
  @test.assert_eq(
    chan_enc(ChannelMethod::ChannelFlowOk(ChannelFlowOk::{ active: true, })),
    b"\x00\x14\x00\x15\x01",
  )
  @test.assert_eq(
    chan_enc(
      ChannelMethod::ChannelClose(ChannelClose::{
        reply_code: (200).to_uint16(),
        reply_text: "ok",
        class_id: (60).to_uint16(),
        method_id: (40).to_uint16(),
      }),
    ),
    b"\x00\x14\x00\x28\x00\xc8\x02ok\x00\x3c\x00\x28",
  )
  @test.assert_eq(chan_enc(ChannelMethod::ChannelCloseOk), b"\x00\x14\x00\x29")
}

///|
/// The reserved short string in channel.open is fixed to "".
test "channel.open reserved-1 must be empty" {
  let raised = try {
    let _ = chan_enc(
      ChannelMethod::ChannelOpen(ChannelOpen::{ reserved_1: "ticket", }),
    )
    false
  } catch {
    ChannelError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// The reserved long string in channel.open-ok is fixed to "".
test "channel.open-ok reserved-1 must be empty" {
  let raised = try {
    let _ = chan_enc(
      ChannelMethod::ChannelOpenOk(ChannelOpenOk::{ reserved_1: "chan-1", }),
    )
    false
  } catch {
    ChannelError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A reply text longer than 255 UTF-8 bytes does not fit its short
/// string wire type.
test "reply text over 255 bytes raises" {
  let raised = try {
    let _ = chan_enc(
      ChannelMethod::ChannelClose(ChannelClose::{
        reply_code: (406).to_uint16(),
        reply_text: "x".repeat(256),
        class_id: (0).to_uint16(),
        method_id: (0).to_uint16(),
      }),
    )
    false
  } catch {
    ChannelError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
