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
/// An AMQP 0-9-1 frame (§4.2.6): a 7-byte header (type octet,
/// 2-byte channel number, 4-byte payload size), the payload, and the
/// frame-end octet `0xCE`.
///
/// `Method`, `Header` and `Body` carry `(channel, payload)` — the
/// channel number and the raw payload; `Heartbeat` is fixed by the spec
/// to channel 0 with an empty payload (§4.2.6.7).
pub(all) enum Frame {
  /// Type 1: an RPC request or reply (e.g. `Connection.Start`).
  Method(UInt16, Bytes)
  /// Type 2: a content header (class id, weight, body size, properties).
  Header(UInt16, Bytes)
  /// Type 3: a content body fragment.
  Body(UInt16, Bytes)
  /// Type 8: a connection heartbeat (channel 0, no payload).
  Heartbeat
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend Frame with Eq::{not_equal, equal}

///|
pub extend Frame with @debug.Debug::{to_repr}

///|
/// Append the frame's wire form (type, channel, size, payload,
/// frame-end `0xCE`) to `w`.
///
/// Never raises: the payload is written as raw bytes.
pub fn Frame::encode(self : Frame, w : @codec.Writer) -> Unit {
  match self {
    Frame::Method(channel, payload) => write_frame(w, b'\x01', channel, payload)
    Frame::Header(channel, payload) => write_frame(w, b'\x02', channel, payload)
    Frame::Body(channel, payload) => write_frame(w, b'\x03', channel, payload)
    Frame::Heartbeat => write_frame(w, b'\x08', (0).to_uint16(), b"")
  }
}

///|
/// Read one complete frame from `r`.
///
/// Raises `FrameError::Truncated` when fewer than 8 bytes remain or the
/// payload is cut off, `FrameError::BadFrameType` for an unknown type
/// octet, `FrameError::InvalidFrameEnd` when the frame-end octet is not
/// `0xCE`, and `FrameError::InvalidHeartbeat` when a heartbeat frame
/// carries a payload or a nonzero channel.
pub fn Frame::decode(r : @codec.Reader) -> Frame raise FrameError {
  let type_octet = next_byte(r)
  let channel = next_u16(r)
  let size = next_u32(r)
  if size.to_uint64() > r.remaining().to_uint64() {
    raise FrameError::Truncated
  }
  let payload = next_bytes(r, size.reinterpret_as_int()).to_owned()
  let frame_end = next_byte(r)
  if frame_end != b'\xce' {
    raise FrameError::InvalidFrameEnd(frame_end)
  }
  match type_octet {
    b'\x01' => Frame::Method(channel, payload)
    b'\x02' => Frame::Header(channel, payload)
    b'\x03' => Frame::Body(channel, payload)
    b'\x08' => {
      if channel.to_uint() != 0 || size != 0 {
        raise FrameError::InvalidHeartbeat
      }
      Frame::Heartbeat
    }
    other => raise FrameError::BadFrameType(other)
  }
}

///|
/// Write the 7-byte frame header, the payload, and the frame-end octet.
fn write_frame(
  w : @codec.Writer,
  type_octet : Byte,
  channel : UInt16,
  payload : Bytes,
) -> Unit {
  w.write_byte(type_octet)
  w.write_u16(channel)
  w.write_u32(payload.length().reinterpret_as_uint())
  w.write_bytes(payload)
  w.write_byte(b'\xce')
}

///|
/// `read_byte` mapped onto `FrameError`.
fn next_byte(r : @codec.Reader) -> Byte raise FrameError {
  r.read_byte() catch {
    @codec.CodecError::OutOfRange => raise FrameError::Truncated
    @codec.CodecError::InvalidUtf8 => raise FrameError::Truncated
  }
}

///|
/// `read_u16` mapped onto `FrameError`.
fn next_u16(r : @codec.Reader) -> UInt16 raise FrameError {
  r.read_u16() catch {
    @codec.CodecError::OutOfRange => raise FrameError::Truncated
    @codec.CodecError::InvalidUtf8 => raise FrameError::Truncated
  }
}

///|
/// `read_u32` mapped onto `FrameError`.
fn next_u32(r : @codec.Reader) -> UInt raise FrameError {
  r.read_u32() catch {
    @codec.CodecError::OutOfRange => raise FrameError::Truncated
    @codec.CodecError::InvalidUtf8 => raise FrameError::Truncated
  }
}

///|
/// `read_bytes` mapped onto `FrameError`.
fn next_bytes(r : @codec.Reader, n : Int) -> BytesView raise FrameError {
  r.read_bytes(n) catch {
    @codec.CodecError::OutOfRange => raise FrameError::Truncated
    @codec.CodecError::InvalidUtf8 => raise FrameError::Truncated
  }
}
