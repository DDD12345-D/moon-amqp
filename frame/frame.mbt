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
