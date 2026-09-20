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
/// The protocol header is exactly the eight §4.2.2 bytes.
test "protocol header golden" {
  let w = @codec.Writer::new()
  @connection.Handshake::write_protocol_header(w)
  @test.assert_eq(w.to_bytes(), b"AMQP\x00\x00\x09\x01")
}

///|
/// The full happy path: Start, Start-Ok, Tune, Tune-Ok, Open, Open-Ok
/// advance the phases in order and the builders return the right
/// methods.
test "handshake happy path" {
  let h = @connection.Handshake::new()
  @test.assert_eq(h.state(), @connection.HandshakeState::AwaitStart)
  h.receive(
    @connection.ConnectionMethod::@connection.Start(@connection.Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::AwaitTune)

  let client = @table.FieldTable::new()
  client.set("product", @table.FieldValue::ShortStr("moon-amqp"))
  let start_ok = h.start_ok(client, "PLAIN", "guest", "en_US")
  match start_ok {
    @connection.ConnectionMethod::StartOk(s) => {
      @test.assert_eq(s.mechanism, "PLAIN")
      @test.assert_eq(s.response, "guest")
      @test.assert_eq(s.locale, "en_US")
      @test.assert_eq(
        s.client_properties.get("product"),
        Some(@table.FieldValue::ShortStr("moon-amqp")),
      )
    }
    _ => @test.assert_eq(0, 1)
  }

  h.receive(
    @connection.ConnectionMethod::@connection.Tune(@connection.Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::AwaitOpenOk)

  let tune_ok = h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16())
  @test.assert_eq(
    tune_ok,
    @connection.ConnectionMethod::@connection.TuneOk(@connection.TuneOk::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  let open = h.open("/")
  @test.assert_eq(
    open,
    @connection.ConnectionMethod::@connection.Open(@connection.Open::{
      virtual_host: "/",
      reserved_1: "",
      reserved_2: false,
    }),
  )

  h.receive(
    @connection.ConnectionMethod::@connection.OpenOk(@connection.OpenOk::{
      reserved_1: "",
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::@connection.Open)
}

///|
/// A server method out of order is an `UnexpectedMethod` error and
/// leaves the phase unchanged.
test "unexpected server methods raise" {
  let h = @connection.Handshake::new()
  let raised = try {
    h.receive(
      @connection.ConnectionMethod::@connection.Tune(@connection.Tune::{
        channel_max: (2047).to_uint16(),
        frame_max: 131072,
        heartbeat: (60).to_uint16(),
      }),
    )
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  @test.assert_eq(h.state(), @connection.HandshakeState::AwaitStart)

  h.receive(
    @connection.ConnectionMethod::@connection.Start(@connection.Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  let raised = try {
    h.receive(
      @connection.ConnectionMethod::@connection.Start(@connection.Start::{
        version_major: b'\x00',
        version_minor: b'\x09',
        server_properties: @table.FieldTable::new(),
        mechanisms: "PLAIN",
        locales: "en_US",
      }),
    )
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  @test.assert_eq(h.state(), @connection.HandshakeState::AwaitTune)
}

///|
/// The reply builders refuse to run before the server method they
/// answer has arrived.
test "builders require their phase" {
  let h = @connection.Handshake::new()
  let raised = try {
    let _ = h.start_ok(@table.FieldTable::new(), "PLAIN", "", "en_US")
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)

  h.receive(
    @connection.ConnectionMethod::@connection.Start(@connection.Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  let raised = try {
    let _ = h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16())
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.open("/")
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Drive a handshake through the happy path to `Open`.
fn open_handshake() -> @connection.Handshake raise @connection.ConnectionError {
  let h = @connection.Handshake::new()
  h.receive(
    @connection.ConnectionMethod::@connection.Start(@connection.Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  h.receive(
    @connection.ConnectionMethod::@connection.Tune(@connection.Tune::{
      channel_max: (2047).to_uint16(),
      frame_max: 131072,
      heartbeat: (60).to_uint16(),
    }),
  )
  h.receive(
    @connection.ConnectionMethod::@connection.OpenOk(@connection.OpenOk::{
      reserved_1: "",
    }),
  )
  h
}

///|
/// A Connection.Close from the server during the handshake (e.g. a
/// refused login) ends the connection from any phase, records the
/// reason, and is answered with Close-Ok.
test "server close during handshake ends the connection" {
  let h = @connection.Handshake::new()
  h.receive(
    @connection.ConnectionMethod::@connection.Start(@connection.Start::{
      version_major: b'\x00',
      version_minor: b'\x09',
      server_properties: @table.FieldTable::new(),
      mechanisms: "PLAIN",
      locales: "en_US",
    }),
  )
  h.receive(
    @connection.ConnectionMethod::@connection.Close(@connection.Close::{
      reply_code: (403).to_uint16(),
      reply_text: "ACCESS_REFUSED - Login was refused",
      class_id: (0).to_uint16(),
      method_id: (0).to_uint16(),
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::Closed)
  match h.close_reason() {
    Some(c) => {
      @test.assert_eq(c.reply_code, (403).to_uint16())
      @test.assert_eq(c.reply_text, "ACCESS_REFUSED - Login was refused")
    }
    None => @test.assert_eq(0, 1)
  }
  @test.assert_eq(h.close_ok(), @connection.ConnectionMethod::CloseOk(()))
}

///|
/// A Connection.Close from the server on an open connection (e.g. an
/// administrative shutdown) ends the connection and is answered with
/// Close-Ok.
test "server close on an open connection" {
  let h = open_handshake()
  h.receive(
    @connection.ConnectionMethod::@connection.Close(@connection.Close::{
      reply_code: (320).to_uint16(),
      reply_text: "CONNECTION_FORCED - shutdown",
      class_id: (0).to_uint16(),
      method_id: (0).to_uint16(),
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::Closed)
  @test.assert_eq(h.close_ok(), @connection.ConnectionMethod::CloseOk(()))
}

///|
/// The normal shutdown: the client sends Close (class and method ids
/// 0), the server answers Close-Ok, and the connection is closed.
test "client close then server close-ok" {
  let h = open_handshake()
  let close = h.close((200).to_uint16(), "Goodbye")
  @test.assert_eq(
    close,
    @connection.ConnectionMethod::@connection.Close(@connection.Close::{
      reply_code: (200).to_uint16(),
      reply_text: "Goodbye",
      class_id: (0).to_uint16(),
      method_id: (0).to_uint16(),
    }),
  )
  @test.assert_eq(h.state(), @connection.HandshakeState::@connection.Open)
  h.receive(@connection.ConnectionMethod::CloseOk(()))
  @test.assert_eq(h.state(), @connection.HandshakeState::Closed)
  @test.assert_eq(h.close_reason(), None)
}

///|
/// Close-Ok without a preceding Close from the client is a protocol
/// violation and leaves the connection open.
test "close-ok without a close raises" {
  let h = open_handshake()
  let raised = try {
    h.receive(@connection.ConnectionMethod::CloseOk(()))
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  @test.assert_eq(h.state(), @connection.HandshakeState::@connection.Open)
}

///|
/// The terminal Closed phase leaks nothing: every method, every
/// builder, is rejected, in both directions of the close.
test "closed connection rejects everything" {
  let h = open_handshake()
  let _ = h.close((200).to_uint16(), "Goodbye")
  h.receive(@connection.ConnectionMethod::CloseOk(()))

  for
    m in [
      @connection.ConnectionMethod::@connection.Start(@connection.Start::{
        version_major: b'\x00',
        version_minor: b'\x09',
        server_properties: @table.FieldTable::new(),
        mechanisms: "PLAIN",
        locales: "en_US",
      }),
      @connection.ConnectionMethod::@connection.Tune(@connection.Tune::{
        channel_max: (2047).to_uint16(),
        frame_max: 131072,
        heartbeat: (60).to_uint16(),
      }),
      @connection.ConnectionMethod::@connection.OpenOk(@connection.OpenOk::{
        reserved_1: "",
      }),
      @connection.ConnectionMethod::@connection.Close(@connection.Close::{
        reply_code: (320).to_uint16(),
        reply_text: "CONNECTION_FORCED",
        class_id: (0).to_uint16(),
        method_id: (0).to_uint16(),
      }),
      @connection.ConnectionMethod::CloseOk(()),
    ] {
    let raised = try {
      h.receive(m)
      false
    } catch {
      @connection.ConnectionError::UnexpectedMethod(_) => true
      _ => false
    }
    @test.assert_eq(raised, true)
  }
  let raised = try {
    let _ = h.start_ok(@table.FieldTable::new(), "PLAIN", "", "en_US")
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.tune_ok((2047).to_uint16(), 131072, (60).to_uint16())
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.open("/")
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.close((200).to_uint16(), "Goodbye")
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let raised = try {
    let _ = h.close_ok()
    false
  } catch {
    @connection.ConnectionError::UnexpectedMethod(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
