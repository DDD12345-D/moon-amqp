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
/// Connection.Close (method 50): one peer tells the other the
/// connection is ending (§4.3.12). `class_id`/`method_id` name the
/// method whose failure provoked the close, and are 0 for a
/// connection-level close (the normal shutdown case).
pub(all) struct Close {
  /// The reply code (§4.3.12.4), e.g. 200 for a normal shutdown.
  reply_code : UInt16
  /// Human-readable explanation, e.g. `"Goodbye"`.
  reply_text : String
  /// Failing method class, or 0 for a connection-level close.
  class_id : UInt16
  /// Failing method id, or 0 for a connection-level close.
  method_id : UInt16
} derive(Eq, @debug.Debug)

///|
/// One of the Connection-class methods used during the handshake
/// (§4.3). The payload wire form is: class id, method id, arguments.
/// `CloseOk` carries no fields (§4.3.13), so its payload is `Unit`.
pub(all) enum ConnectionMethod {
  Start(Start)
  StartOk(StartOk)
  Tune(Tune)
  TuneOk(TuneOk)
  Open(Open)
  OpenOk(OpenOk)
  Close(Close)
  CloseOk(Unit)
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
    ConnectionMethod::Close(c) => {
      write_method_header(w, (50).to_uint16())
      w.write_u16(c.reply_code)
      write_shortstr_checked(w, c.reply_text)
      w.write_u16(c.class_id)
      w.write_u16(c.method_id)
    }
    ConnectionMethod::CloseOk(_) => write_method_header(w, (51).to_uint16())
  }
}

///|
/// Read one Connection-class method from `r`.
///
/// Raises `ConnectionError::UnexpectedClass` when the class-id is not
/// 10, `ConnectionError::UnknownMethod` for a method-id outside the
/// Connection class, `ConnectionError::Truncated` for input that ends
/// early, `ConnectionError::InvalidUtf8` for malformed strings, and
/// `ConnectionError::InvalidTable` for a malformed field table.
pub fn ConnectionMethod::decode(
  r : @codec.Reader,
) -> ConnectionMethod raise ConnectionError {
  let class_id = next_u16(r)
  if class_id != connection_class_id {
    raise ConnectionError::UnexpectedClass(class_id)
  }
  let method_id = next_u16(r)
  match method_id.to_uint().reinterpret_as_int() {
    10 =>
      ConnectionMethod::Start(Start::{
        version_major: next_byte(r),
        version_minor: next_byte(r),
        server_properties: next_table(r),
        mechanisms: next_longstr(r),
        locales: next_longstr(r),
      })
    11 =>
      ConnectionMethod::StartOk(StartOk::{
        client_properties: next_table(r),
        mechanism: next_shortstr(r),
        response: next_longstr(r),
        locale: next_shortstr(r),
      })
    30 =>
      ConnectionMethod::Tune(Tune::{
        channel_max: next_u16(r),
        frame_max: next_u32(r),
        heartbeat: next_u16(r),
      })
    31 =>
      ConnectionMethod::TuneOk(TuneOk::{
        channel_max: next_u16(r),
        frame_max: next_u32(r),
        heartbeat: next_u16(r),
      })
    40 =>
      ConnectionMethod::Open(Open::{
        virtual_host: next_shortstr(r),
        reserved_1: next_shortstr(r),
        reserved_2: next_bool(r),
      })
    41 => ConnectionMethod::OpenOk(OpenOk::{ reserved_1: next_shortstr(r), })
    50 =>
      ConnectionMethod::Close(Close::{
        reply_code: next_u16(r),
        reply_text: next_shortstr(r),
        class_id: next_u16(r),
        method_id: next_u16(r),
      })
    51 => ConnectionMethod::CloseOk(())
    _ => raise ConnectionError::UnknownMethod(method_id)
  }
}

///|
/// Write the 2-byte class id (always 10 here) and the method id.
fn write_method_header(w : @codec.Writer, method_id : UInt16) -> Unit {
  w.write_u16(connection_class_id)
  w.write_u16(method_id)
}

///|
/// `read_byte` mapped onto `ConnectionError`.
fn next_byte(r : @codec.Reader) -> Byte raise ConnectionError {
  r.read_byte() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `read_bool` mapped onto `ConnectionError`.
fn next_bool(r : @codec.Reader) -> Bool raise ConnectionError {
  r.read_bool() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `read_u16` mapped onto `ConnectionError`.
fn next_u16(r : @codec.Reader) -> UInt16 raise ConnectionError {
  r.read_u16() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `read_u32` mapped onto `ConnectionError`.
fn next_u32(r : @codec.Reader) -> UInt raise ConnectionError {
  r.read_u32() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `read_shortstr` mapped onto `ConnectionError`.
fn next_shortstr(r : @codec.Reader) -> String raise ConnectionError {
  r.read_shortstr() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `read_longstr` mapped onto `ConnectionError`.
fn next_longstr(r : @codec.Reader) -> String raise ConnectionError {
  r.read_longstr() catch {
    @codec.CodecError::OutOfRange => raise ConnectionError::Truncated
    @codec.CodecError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
  }
}

///|
/// `FieldTable::decode` mapped onto `ConnectionError`.
fn next_table(r : @codec.Reader) -> @table.FieldTable raise ConnectionError {
  @table.FieldTable::decode(r) catch {
    @table.TableError::Truncated => raise ConnectionError::Truncated
    @table.TableError::InvalidUtf8 => raise ConnectionError::InvalidUtf8
    @table.TableError::InvalidTag(_) =>
      raise ConnectionError::InvalidTable("unknown field tag in field table")
    @table.TableError::InvalidKey(m) => raise ConnectionError::InvalidTable(m)
    @table.TableError::ValueOutOfRange(m) =>
      raise ConnectionError::InvalidTable(m)
  }
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
