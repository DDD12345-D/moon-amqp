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
/// Non-JS transport stubs: the e2e driver only runs on the JS
/// backend, where bridge_js.mbt provides the socket transport.
/// These stubs keep every other build target compiling; running the
/// driver on a non-JS target panics.

///|
/// Terminate the process: the e2e driver has no transport outside
/// the JS backend.
fn[T] panic_impl() -> T = "%panic"

///|
fn js_connect(host : String, port : Int) -> Int {
  let _ = host
  let _ = port
  panic_impl()
}

///|
fn js_wait_connect(ms : Int) -> Int {
  let _ = ms
  panic_impl()
}

///|
fn js_send(data : Bytes) -> Unit {
  let _ = data
  panic_impl()
}

///|
fn js_wait(ms : Int) -> Int {
  let _ = ms
  panic_impl()
}

///|
fn js_recv(buf : FixedArray[Byte]) -> Int {
  let _ = buf
  panic_impl()
}

///|
fn js_close() -> Unit {
  panic_impl()
}

///|
fn js_exit(code : Int) -> Int {
  let _ = code
  panic_impl()
}
