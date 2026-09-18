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
/// AMQP class id for `connection` (§4.3). Every method payload starts
/// with this class id and a method id.
let connection_class_id : UInt16 = (10).to_uint16()

///|
/// Connection.Start (method 10): the server's greeting, offering
/// protocol versions, its properties, SASL mechanisms, and locales.
pub(all) struct Start {
  version_major : Byte
  version_minor : Byte
  server_properties : @table.FieldTable
  /// Space-separated SASL mechanism names, e.g. `"AMQPLAIN PLAIN"`.
  mechanisms : String
  /// Space-separated locale names, e.g. `"en_US"`.
  locales : String
} derive(Eq, @debug.Debug)

///|
/// Connection.Start-Ok (method 11): the client's reply, choosing a
/// SASL mechanism, a response, and a locale.
pub(all) struct StartOk {
  client_properties : @table.FieldTable
  /// The chosen SASL mechanism, e.g. `"PLAIN"`.
  mechanism : String
  /// The SASL response, e.g. `"\0user\0password"` for PLAIN.
  response : String
  /// The chosen locale, e.g. `"en_US"`.
  locale : String
} derive(Eq, @debug.Debug)

///|
/// Connection.Tune (method 30): the server's resource negotiation
/// limits; 0 means "no limit" / "no heartbeat".
pub(all) struct Tune {
  channel_max : UInt16
  frame_max : UInt
  heartbeat : UInt16
} derive(Eq, @debug.Debug)

///|
/// Connection.Tune-Ok (method 31): the client's accepted limits.
pub(all) struct TuneOk {
  channel_max : UInt16
  frame_max : UInt
  heartbeat : UInt16
} derive(Eq, @debug.Debug)

///|
/// Connection.Open (method 40): the client asks for access to a
/// virtual host. The reserved fields are fixed by the spec to an
/// empty string and `false`; `encode` enforces that.
pub(all) struct Open {
  virtual_host : String
  /// Reserved: must be the empty short string (§4.3.11.5).
  reserved_1 : String
  /// Reserved: must be `false` (§4.3.11.6).
  reserved_2 : Bool
} derive(Eq, @debug.Debug)

///|
/// Connection.Open-Ok (method 41): the server grants virtual-host
/// access. The reserved field is fixed to an empty string.
pub(all) struct OpenOk {
  /// Reserved: must be the empty short string (§4.3.11.8).
  reserved_1 : String
} derive(Eq, @debug.Debug)

///|
/// One of the Connection-class methods used during the handshake
/// (§4.3). The payload wire form is: class id, method id, arguments.
pub(all) enum ConnectionMethod {
  Start(Start)
  StartOk(StartOk)
  Tune(Tune)
  TuneOk(TuneOk)
  Open(Open)
  OpenOk(OpenOk)
} derive(Eq, @debug.Debug)

///|
/// Append the method's wire form (class id, method id, arguments) to `w`.
///
/// Raises `ConnectionError::ValueOutOfRange` when a value does not fit
/// its wire type (e.g. a short string over 255 bytes) or a reserved
/// field is not its fixed spec value, and `ConnectionError::InvalidUtf8`
/// for strings that cannot be UTF-8 encoded.
pub fn ConnectionMethod::encode(
  self : ConnectionMethod,
  w : @codec.Writer,
) -> Unit raise ConnectionError {
  match self {
    ConnectionMethod::Start(s) => {
      write_method_header(w, (10).to_uint16())
      w.write_byte(s.version_major)
      w.write_byte(s.version_minor)
      write_table_checked(w, s.server_properties)
      write_longstr_checked(w, s.mechanisms)
      write_longstr_checked(w, s.locales)
    }
    ConnectionMethod::StartOk(s) => {
      write_method_header(w, (11).to_uint16())
      write_table_checked(w, s.client_properties)
      write_shortstr_checked(w, s.mechanism)
      write_longstr_checked(w, s.response)
      write_shortstr_checked(w, s.locale)
    }
    ConnectionMethod::Tune(t) => {
      write_method_header(w, (30).to_uint16())
      w.write_u16(t.channel_max)
      w.write_u32(t.frame_max)
      w.write_u16(t.heartbeat)
    }
    ConnectionMethod::TuneOk(t) => {
      write_method_header(w, (31).to_uint16())
      w.write_u16(t.channel_max)
      w.write_u32(t.frame_max)
      w.write_u16(t.heartbeat)
    }
    ConnectionMethod::Open(o) => {
      write_method_header(w, (40).to_uint16())
      write_shortstr_checked(w, o.virtual_host)
      if o.reserved_1 != "" {
        raise ConnectionError::ValueOutOfRange(
          "Open.reserved_1 must be the empty string",
        )
      }
      write_shortstr_checked(w, o.reserved_1)
      if o.reserved_2 {
        raise ConnectionError::ValueOutOfRange("Open.reserved_2 must be false")
      }
      w.write_bool(o.reserved_2)
    }
    ConnectionMethod::OpenOk(o) => {
      write_method_header(w, (41).to_uint16())
      if o.reserved_1 != "" {
        raise ConnectionError::ValueOutOfRange(
          "OpenOk.reserved_1 must be the empty string",
        )
      }
      write_shortstr_checked(w, o.reserved_1)
    }
  }
}

///|
/// Write the 2-byte class id (always 10 here) and the method id.
fn write_method_header(w : @codec.Writer, method_id : UInt16) -> Unit {
  w.write_u16(connection_class_id)
  w.write_u16(method_id)
}

///|
/// `write_shortstr` mapped onto `ConnectionError`.
fn write_shortstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise ConnectionError {
  w.write_shortstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise ConnectionError::ValueOutOfRange(
        "short string longer than 255 UTF-8 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `write_longstr` mapped onto `ConnectionError`.
fn write_longstr_checked(
  w : @codec.Writer,
  v : String,
) -> Unit raise ConnectionError {
  w.write_longstr(v) catch {
    @codec.CodecError::OutOfRange =>
      raise ConnectionError::ValueOutOfRange(
        "long string longer than 2^32-1 bytes",
      )
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `FieldTable::encode` mapped onto `ConnectionError`.
fn write_table_checked(
  w : @codec.Writer,
  t : @table.FieldTable,
) -> Unit raise ConnectionError {
  t.encode(w) catch {
    @table.TableError::ValueOutOfRange(m) =>
      raise ConnectionError::ValueOutOfRange(m)
    @table.TableError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
    @table.TableError::InvalidKey(m) =>
      raise ConnectionError::ValueOutOfRange(m)
    _ => raise ConnectionError::InvalidTable("unexpected table encode error")
  }
}
