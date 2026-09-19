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
/// What the connection should do about heartbeats right now
/// (§4.2.7), per `Heartbeat::poll`.
pub(all) enum HeartbeatAction {
  /// All quiet: no heartbeat is due and the peer is responding.
  Nothing
  /// Send a Heartbeat frame: no frame (of any kind) has been sent
  /// for half the heartbeat interval.
  Send
  /// No frame (heartbeat or otherwise) arrived within two heartbeat
  /// intervals: the connection must be considered dead (§4.2.7).
  TimedOut
} derive(Eq, @debug.Debug)

///|
/// Heartbeat bookkeeping for one connection (§4.2.7). The caller
/// drives it: after every frame sent or received, call `note_sent`
/// or `note_received` with a monotonic timestamp in seconds; before
/// writing anything, ask `poll` what to do next.
///
/// The rules, per §4.2.7 and RabbitMQ's interpretation: send a
/// heartbeat when no frame has been sent for half the interval, and
/// declare the connection dead when no frame has arrived for two
/// full intervals. A negotiated interval of 0 disables heartbeats.
/// `stop` ends all tracking, which closes a connection's heartbeat
/// responsibilities cleanly.
pub struct Heartbeat {
  /// Negotiated interval in seconds; 0 disables heartbeats.
  interval : Int
  /// Timestamp of the last frame sent, in the caller's monotonic
  /// seconds.
  mut last_sent : Int
  /// Timestamp of the last frame received, in the caller's monotonic
  /// seconds.
  mut last_received : Int
  /// `stop` was called: nothing is due, ever.
  mut stopped : Bool
}

///|
/// Start tracking for a connection whose negotiated interval is
/// `interval` seconds (`TuneOk.heartbeat`); 0 disables heartbeats.
/// `now` seeds the send and receive clocks.
pub fn Heartbeat::new(interval : UInt16, now : Int) -> Heartbeat {
  {
    interval: interval.to_int(),
    last_sent: now,
    last_received: now,
    stopped: false,
  }
}

///|
/// The negotiated interval in seconds.
pub fn Heartbeat::interval(self : Heartbeat) -> UInt16 {
  self.interval.to_uint16()
}

///|
/// Whether `stop` was called.
pub fn Heartbeat::stopped(self : Heartbeat) -> Bool {
  self.stopped
}

///|
/// What to do now. `TimedOut` takes priority: once no frame has
/// arrived for two intervals the connection is dead, and sending a
/// heartbeat cannot save it. `poll` itself only reads the clock; it
/// is the caller's job to call `note_sent` after actually writing
/// the frame.
pub fn Heartbeat::poll(self : Heartbeat, now : Int) -> HeartbeatAction {
  if self.stopped || self.interval == 0 {
    return HeartbeatAction::Nothing
  }
  if now - self.last_received >= 2 * self.interval {
    return HeartbeatAction::TimedOut
  }
  if now - self.last_sent >= self.interval / 2 {
    return HeartbeatAction::Send
  }
  HeartbeatAction::Nothing
}

///|
/// Record that a frame (any kind, heartbeat included) was sent at
/// `now`; resets the send clock.
pub fn Heartbeat::note_sent(self : Heartbeat, now : Int) -> Unit {
  self.last_sent = now
}

///|
/// Record that a frame (any kind, heartbeat included) was received
/// at `now`; resets the receive clock. A late frame revives a
/// connection that `poll` has reported `TimedOut`.
pub fn Heartbeat::note_received(self : Heartbeat, now : Int) -> Unit {
  self.last_received = now
}

///|
/// End all tracking: `poll` then reports `Nothing` forever, however
/// much time passes. A connection that was closed deliberately is
/// not "timed out", and it must never ask for further traffic.
pub fn Heartbeat::stop(self : Heartbeat) -> Unit {
  self.stopped = true
}
