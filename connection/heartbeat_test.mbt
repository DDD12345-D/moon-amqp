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
/// A 60-second interval asks for a heartbeat once no frame has been
/// sent for half the interval, and only then.
test "heartbeat is due after half an interval without sends" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  @test.assert_eq(h.poll(0), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(29), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(30), @connection.HeartbeatAction::Send)
  h.note_sent(31)
  @test.assert_eq(h.poll(60), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(61), @connection.HeartbeatAction::Send)
}

///|
/// Any frame sent resets the send clock, including the heartbeat
/// itself.
test "sending any frame resets the send clock" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  @test.assert_eq(h.poll(30), @connection.HeartbeatAction::Send)
  h.note_sent(30)
  @test.assert_eq(h.poll(30), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(59), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(60), @connection.HeartbeatAction::Send)
}

///|
/// No frame received within two full intervals means the connection
/// is dead (§4.2.7). Receiving is the only thing that revives it,
/// restarting the timeout clock.
test "two intervals without a frame times the connection out" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  // Keep the send clock quiet so the receive clock alone decides.
  h.note_sent(90)
  @test.assert_eq(h.poll(119), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(120), @connection.HeartbeatAction::TimedOut)
  // A late frame revives the connection.
  h.note_received(121)
  h.note_sent(212)
  @test.assert_eq(h.poll(240), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(241), @connection.HeartbeatAction::TimedOut)
}

///|
/// Frames received do not count as frames sent: the send clock keeps
/// running even while the peer chatters.
test "receiving does not reset the send clock" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  h.note_received(10)
  h.note_received(20)
  @test.assert_eq(h.poll(29), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(30), @connection.HeartbeatAction::Send)
}

///|
/// Timeout wins over sending: a connection whose peer went silent is
/// dead, and no heartbeat can save it.
test "timeout takes priority over sending" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  @test.assert_eq(h.poll(1000), @connection.HeartbeatAction::TimedOut)
}

///|
/// An interval of 0 disables heartbeats entirely: nothing is ever
/// due and the connection never times out.
test "zero interval disables heartbeats" {
  let h = @connection.Heartbeat::new((0).to_uint16(), 0)
  @test.assert_eq(h.poll(1), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(1000000), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.interval(), (0).to_uint16())
}

///|
/// A 1-second interval sends on every poll (half an interval rounds
/// down to 0) and times out after two seconds.
test "tiny intervals send eagerly" {
  let h = @connection.Heartbeat::new((1).to_uint16(), 0)
  @test.assert_eq(h.poll(0), @connection.HeartbeatAction::Send)
  h.note_sent(0)
  @test.assert_eq(h.poll(1), @connection.HeartbeatAction::Send)
  @test.assert_eq(h.poll(2), @connection.HeartbeatAction::TimedOut)
}

///|
/// `stop` ends all tracking: nothing is ever due again, and a
/// deliberately closed connection is never reported dead. A closed
/// connection's heartbeat responsibilities do not linger.
test "stop silences the heartbeat forever" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  h.stop()
  @test.assert_eq(h.stopped(), true)
  @test.assert_eq(h.poll(1000), @connection.HeartbeatAction::Nothing)
  h.note_received(1000)
  h.note_sent(1000)
  @test.assert_eq(h.poll(100000), @connection.HeartbeatAction::Nothing)
}

///|
/// A late frame revives a connection that timed out.
test "a late frame revives a timed-out connection" {
  let h = @connection.Heartbeat::new((60).to_uint16(), 0)
  @test.assert_eq(h.poll(120), @connection.HeartbeatAction::TimedOut)
  h.note_received(120)
  h.note_sent(120)
  @test.assert_eq(h.poll(120), @connection.HeartbeatAction::Nothing)
  @test.assert_eq(h.poll(150), @connection.HeartbeatAction::Send)
}
