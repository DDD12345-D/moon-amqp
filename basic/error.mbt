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
/// Errors raised by the basic package: method and content header
/// encoding and decoding failures.
pub suberror BasicError {
  /// The input ended before a value was complete.
  Truncated
  /// A string field contains bytes that are not valid UTF-8.
  InvalidUtf8
  /// A value does not fit its wire type or violates a fixed
  /// constant the protocol demands.
  ValueOutOfRange(String)
  /// A field table is malformed or carries an invalid key.
  InvalidTable(String)
  /// The payload's class id is not the basic class (60).
  UnexpectedClass(UInt16)
  /// The method id is not defined for the basic class.
  UnknownMethod(UInt16, UInt16)
}
