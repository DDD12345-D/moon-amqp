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

///|
/// Golden decode: the same four vectors read back to their frames, one
/// after another from a single reader.
test "frame golden decode" {
  let r = @codec.Reader::new(
    b"\x01\x00\x01\x00\x00\x00\x04\x00\x0a\x00\x14\xce\x02\x00\x02\x00\x00\x00\x0c\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xce\x03\x00\x03\x00\x00\x00\x05hello\xce\x08\x00\x00\x00\x00\x00\x00\xce",
  )
  @test.assert_eq(
    Frame::decode(r),
    Frame::Method((1).to_uint16(), b"\x00\x0a\x00\x14"),
  )
  @test.assert_eq(
    Frame::decode(r),
    Frame::Header(
      (2).to_uint16(),
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    ),
  )
  @test.assert_eq(Frame::decode(r), Frame::Body((3).to_uint16(), b"hello"))
  @test.assert_eq(Frame::decode(r), Frame::Heartbeat)
  @test.assert_eq(r.is_empty(), true)
}

///|
/// Every frame type survives an encode/decode roundtrip.
test "frame roundtrip" {
  let frames = [
    Frame::Method((1).to_uint16(), b"\x00\x0a\x00\x14"),
    Frame::Header(
      (2).to_uint16(),
      b"\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    ),
    Frame::Body((3).to_uint16(), b"hello"),
    Frame::Heartbeat,
  ]
  let w = @codec.Writer::new()
  for f in frames {
    f.encode(w)
  }
  let r = @codec.Reader::new(w.to_bytes())
  for f in frames {
    @test.assert_eq(Frame::decode(r), f)
  }
  @test.assert_eq(r.is_empty(), true)
}

///|
/// Malformed inputs raise the expected variants: cut-off headers and
/// payloads are `Truncated`, an unknown type octet is `BadFrameType`,
/// a wrong frame-end octet is `InvalidFrameEnd`, and heartbeats with a
/// payload or nonzero channel are `InvalidHeartbeat`.
test "malformed frames raise" {
  for
    bad in [
      b"", b"\x01\x00", b"\x01\x00\x01\x00\x00\x00\x02\xaa\xbb", b"\x03\x00\x00\xff\xff\xff\xff",
    ] {
    let raised = try {
      let _ = Frame::decode(@codec.Reader::new(bad))
      false
    } catch {
      FrameError::Truncated => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
  let err = try {
    let _ = Frame::decode(
      @codec.Reader::new(b"\x07\x00\x00\x00\x00\x00\x00\xce"),
    )
    None
  } catch {
    FrameError::BadFrameType(t) => Some(t)
    _ => None
  }
  @test.assert_eq(err, Some(b'\x07'))
  let err = try {
    let _ = Frame::decode(
      @codec.Reader::new(b"\x03\x00\x00\x00\x00\x00\x00\xcf"),
    )
    None
  } catch {
    FrameError::InvalidFrameEnd(b) => Some(b)
    _ => None
  }
  @test.assert_eq(err, Some(b'\xcf'))
  for
    bad in [
      b"\x08\x00\x00\x00\x00\x00\x01\x00\xce", b"\x08\x00\x01\x00\x00\x00\x00\xce",
    ] {
    let raised = try {
      let _ = Frame::decode(@codec.Reader::new(bad))
      false
    } catch {
      FrameError::InvalidHeartbeat => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
}
