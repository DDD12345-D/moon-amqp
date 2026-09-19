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
/// The basic-class message properties that travel in a content header
/// (§3.2.2). Every property is optional: `None` leaves it out, `Some`
/// writes it and sets its flag bit. On the wire the properties appear
/// in a fixed order, high flag bit first (§4.2.6.1): content-type,
/// content-encoding, headers, delivery-mode, priority, correlation-id,
/// reply-to, expiration, message-id, timestamp, type, user-id, app-id,
/// cluster-id.
pub(all) struct BasicProperties {
  /// MIME content type, e.g. `"application/json"`.
  content_type : String?
  /// MIME content encoding, e.g. `"gzip"`.
  content_encoding : String?
  /// Application-defined headers (a field table).
  headers : @table.FieldTable?
  /// 1 = non-persistent, 2 = persistent; RabbitMQ only accepts 1
  /// and 2.
  delivery_mode : Byte?
  /// Message priority 0-9, used by priority queues.
  priority : Byte?
  /// Application correlation identifier.
  correlation_id : String?
  /// Reply queue address.
  reply_to : String?
  /// Message expiration, in milliseconds.
  expiration : String?
  /// Application message identifier.
  message_id : String?
  /// POSIX timestamp in seconds since the epoch.
  timestamp : UInt64?
  /// Message type name.
  message_type : String?
  /// The creating user id; deprecated by the spec.
  user_id : String?
  /// The creating application id.
  app_id : String?
  /// Intra-cluster routing id; the spec fixes it to the empty
  /// string.
  cluster_id : String?
} derive(Eq, @debug.Debug)

///|
/// A message with no properties set.
pub fn BasicProperties::new() -> BasicProperties {
  BasicProperties::{
    content_type: None,
    content_encoding: None,
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: None,
    reply_to: None,
    expiration: None,
    message_id: None,
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: None,
    cluster_id: None,
  }
}

///|
/// A content header payload (§4.2.6.1): class id, weight, body size,
/// property flags, property list. The header frame follows a method
/// frame that announces content (e.g. `Basic.Publish`) and precedes
/// the content body frames.
pub(all) struct ContentHeader {
  /// Total size of the content body in bytes.
  body_size : UInt64
  /// The message properties carried by this header.
  properties : BasicProperties
} derive(Eq, @debug.Debug)

///|
/// Append the content header payload to `w`.
pub fn ContentHeader::encode(
  self : ContentHeader,
  w : @codec.Writer,
) -> Unit raise BasicError {
  self.properties.check()
  w.write_u16(basic_class_id)
  // Weight: fixed to 0 by the grammar (§4.2.6.1).
  w.write_u16((0).to_uint16())
  w.write_u64(self.body_size)
  w.write_u16(self.properties.encode_flags())
  self.properties.write_present(w)
}

///|
/// Decode a content header payload. Unknown flag bits are ignored
/// and continuation flag fields (§4.2.6.1) are consumed, so future
/// protocol extensions do not break reading.
pub fn ContentHeader::decode(
  r : @codec.Reader,
) -> ContentHeader raise BasicError {
  let class_id = next_u16(r)
  if class_id != basic_class_id {
    raise BasicError::UnexpectedClass(class_id)
  }
  let _weight = next_u16(r) // fixed to 0 by the grammar
  let body_size = next_u64(r)
  let flags = read_flags(r)
  let content_type = if (flags & 0x8000) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let content_encoding = if (flags & 0x4000) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let headers = if (flags & 0x2000) != 0 { Some(next_table(r)) } else { None }
  let delivery_mode = if (flags & 0x1000) != 0 {
    Some(next_byte(r))
  } else {
    None
  }
  let priority = if (flags & 0x0800) != 0 { Some(next_byte(r)) } else { None }
  let correlation_id = if (flags & 0x0400) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let reply_to = if (flags & 0x0200) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let expiration = if (flags & 0x0100) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let message_id = if (flags & 0x0080) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let timestamp = if (flags & 0x0040) != 0 { Some(next_u64(r)) } else { None }
  let message_type = if (flags & 0x0020) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let user_id = if (flags & 0x0010) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  let app_id = if (flags & 0x0008) != 0 { Some(next_shortstr(r)) } else { None }
  let cluster_id = if (flags & 0x0004) != 0 {
    Some(next_shortstr(r))
  } else {
    None
  }
  ContentHeader::{
    body_size,
    properties: BasicProperties::{
      content_type,
      content_encoding,
      headers,
      delivery_mode,
      priority,
      correlation_id,
      reply_to,
      expiration,
      message_id,
      timestamp,
      message_type,
      user_id,
      app_id,
      cluster_id,
    },
  }
}

///|
/// The flag bits of the present properties, one bit per property in
/// wire order, bit 15 = content-type (§4.2.6.1).
fn BasicProperties::encode_flags(self : BasicProperties) -> UInt16 {
  let mut flags = 0
  if self.content_type != None {
    flags = flags | 0x8000
  }
  if self.content_encoding != None {
    flags = flags | 0x4000
  }
  if self.headers != None {
    flags = flags | 0x2000
  }
  if self.delivery_mode != None {
    flags = flags | 0x1000
  }
  if self.priority != None {
    flags = flags | 0x0800
  }
  if self.correlation_id != None {
    flags = flags | 0x0400
  }
  if self.reply_to != None {
    flags = flags | 0x0200
  }
  if self.expiration != None {
    flags = flags | 0x0100
  }
  if self.message_id != None {
    flags = flags | 0x0080
  }
  if self.timestamp != None {
    flags = flags | 0x0040
  }
  if self.message_type != None {
    flags = flags | 0x0020
  }
  if self.user_id != None {
    flags = flags | 0x0010
  }
  if self.app_id != None {
    flags = flags | 0x0008
  }
  if self.cluster_id != None {
    flags = flags | 0x0004
  }
  flags.to_uint16()
}

///|
/// Append the present properties in wire order, high flag bit first.
fn BasicProperties::write_present(
  self : BasicProperties,
  w : @codec.Writer,
) -> Unit raise BasicError {
  write_if_some_str(w, self.content_type)
  write_if_some_str(w, self.content_encoding)
  write_if_some_table(w, self.headers)
  write_if_some_byte(w, self.delivery_mode)
  write_if_some_byte(w, self.priority)
  write_if_some_str(w, self.correlation_id)
  write_if_some_str(w, self.reply_to)
  write_if_some_str(w, self.expiration)
  write_if_some_str(w, self.message_id)
  write_if_some_u64(w, self.timestamp)
  write_if_some_str(w, self.message_type)
  write_if_some_str(w, self.user_id)
  write_if_some_str(w, self.app_id)
  write_if_some_str(w, self.cluster_id)
}

///|
/// Enforce the property-level constants the spec demands.
fn BasicProperties::check(self : BasicProperties) -> Unit raise BasicError {
  match self.cluster_id {
    Some("") => ()
    Some(_) =>
      raise BasicError::ValueOutOfRange("cluster-id must be the empty string")
    None => ()
  }
}

///|
/// Read the property flags, consuming continuation flag fields
/// (§4.2.6.1: bit 0 set means a further flags field follows). The
/// basic class needs 14 bits, so no continuation should ever appear;
/// still, reading them keeps the decoder total.
fn read_flags(r : @codec.Reader) -> Int raise BasicError {
  let mut flags = next_u16(r).to_int()
  while (flags & 1) != 0 {
    flags = next_u16(r).to_int()
  }
  flags
}

///|
/// Write a present short string property.
fn write_if_some_str(w : @codec.Writer, v : String?) -> Unit raise BasicError {
  match v {
    Some(s) => write_shortstr_checked(w, s)
    None => ()
  }
}

///|
/// Write a present octet property.
fn write_if_some_byte(w : @codec.Writer, v : Byte?) -> Unit {
  match v {
    Some(b) => w.write_byte(b)
    None => ()
  }
}

///|
/// Write a present 64-bit property.
fn write_if_some_u64(w : @codec.Writer, v : UInt64?) -> Unit {
  match v {
    Some(n) => w.write_u64(n)
    None => ()
  }
}

///|
/// Write a present field table property.
fn write_if_some_table(
  w : @codec.Writer,
  v : @table.FieldTable?,
) -> Unit raise BasicError {
  match v {
    Some(t) => write_table_checked(w, t)
    None => ()
  }
}

///|
/// Write a field table, mapping table failures to `BasicError`.
fn write_table_checked(
  w : @codec.Writer,
  t : @table.FieldTable,
) -> Unit raise BasicError {
  t.encode(w) catch {
    @table.TableError::ValueOutOfRange(m) =>
      raise BasicError::ValueOutOfRange(m)
    @table.TableError::InvalidUtf8 => raise BasicError::InvalidUtf8
    @table.TableError::InvalidKey(m) => raise BasicError::InvalidTable(m)
    @table.TableError::Truncated => raise BasicError::Truncated
    @table.TableError::InvalidTag(_) =>
      raise BasicError::InvalidTable("unknown field tag in field table")
  }
}

///|
/// Read one unsigned 64-bit value.
fn next_u64(r : @codec.Reader) -> UInt64 raise BasicError {
  r.read_u64() catch {
    @codec.CodecError::OutOfRange => raise BasicError::Truncated
    @codec.CodecError::InvalidUtf8 => raise BasicError::InvalidUtf8
  }
}

///|
/// Read one field table.
fn next_table(r : @codec.Reader) -> @table.FieldTable raise BasicError {
  @table.FieldTable::decode(r) catch {
    @table.TableError::Truncated => raise BasicError::Truncated
    @table.TableError::InvalidUtf8 => raise BasicError::InvalidUtf8
    @table.TableError::InvalidTag(_) =>
      raise BasicError::InvalidTable("unknown field tag in field table")
    @table.TableError::InvalidKey(m) => raise BasicError::InvalidTable(m)
    @table.TableError::ValueOutOfRange(m) => raise BasicError::InvalidTable(m)
  }
}
