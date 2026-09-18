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
/// Errors produced while decoding an AMQP 0-9-1 frame (§4.2.6).
pub suberror FrameError {
  /// Input ends before the frame header, payload, or frame-end is complete.
  Truncated
  /// The frame-end octet is not `0xCE` (§4.2.6.6).
  InvalidFrameEnd(Byte)
  /// The type octet is not 1, 2, 3, or 8 (§4.2.6.1).
  BadFrameType(Byte)
  /// A heartbeat frame carries a payload or a nonzero channel (§4.2.6.7).
  InvalidHeartbeat
}
