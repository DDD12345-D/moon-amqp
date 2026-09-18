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
/// Errors produced while encoding or decoding the channel-, exchange-
/// and queue-class methods (§3.2.2): channel lifecycle and topology
/// declarations.
pub suberror ChannelError {
  /// A string in the method was not valid UTF-8.
  InvalidUtf8
  /// A value does not fit its wire type (e.g. a short string over
  /// 255 bytes) or violates a protocol constraint (a reserved field
  /// that is not empty).
  ValueOutOfRange(String)
  /// A field table inside the method was malformed.
  InvalidTable(String)
}
