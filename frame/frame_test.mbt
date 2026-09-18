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
/// Encode one frame and return its bytes.
fn enc(f : Frame) -> Bytes {
  let w = @codec.Writer::new()
  f.encode(w)
  w.to_bytes()
}

///|
/// Golden encode: each frame type maps to the exact wire bytes from
/// AMQP 0-9-1 §4.2.6 (type, channel, size, payload, frame-end `0xCE`).
test "frame golden encode" {
  @test.assert_eq(
    enc(Frame::Method((1).to_uint16(), b"\x00\x0a\x00\x14")),
    b"\x01\x00\x01\x00\x00\x00\x04\x00\x0a\x00\x14\xce",
  )
  @test.assert_eq(
    enc(
      Frame::Header(
        (2).to_uint16(),
        b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
      ),
    ),
    b"\x02\x00\x02\x00\x00\x00\x0c\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xce",
  )
  @test.assert_eq(
    enc(Frame::Body((3).to_uint16(), b"hello")),
    b"\x03\x00\x03\x00\x00\x00\x05hello\xce",
  )
  @test.assert_eq(enc(Frame::Heartbeat), b"\x08\x00\x00\x00\x00\x00\x00\xce")
}

///|
/// Channel and size boundaries: the maximum channel number, an empty
/// payload, and channel 0.
test "frame channel boundaries" {
  @test.assert_eq(
    enc(Frame::Method((65535).to_uint16(), b"")),
    b"\x01\xff\xff\x00\x00\x00\x00\xce",
  )
  @test.assert_eq(
    enc(Frame::Body((0).to_uint16(), b"")),
    b"\x03\x00\x00\x00\x00\x00\x00\xce",
  )
}
