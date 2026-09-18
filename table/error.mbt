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
/// Errors raised while encoding or decoding field tables (§4.2.5.5).
pub suberror TableError {
  /// A value tag octet is not one of the AMQP field-type tags.
  InvalidTag(Byte)
  /// The input ended before a table or value was complete.
  Truncated
  /// A value does not fit its AMQP wire type.
  ValueOutOfRange(String)
  /// A key or string contains bytes that are not valid UTF-8.
  InvalidUtf8
  /// A field name violates the §4.2.5.5 naming rules.
  InvalidKey(String)
}
