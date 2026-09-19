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
/// JS-target transport for the driver package.
///
/// A worker thread owns the TCP socket so node's event loop keeps
/// running while the driver's main thread waits synchronously; bytes
/// received from the broker land in a SharedArrayBuffer and the
/// driver reads them through `Atomics.wait`.
///
/// Control layout (an Int32Array over the first 16 bytes of the
/// buffer): [0] bytes written, [1] unused, [2] status (0 connecting,
/// 1 connected, -1 error), [3] write position. Received data starts
/// at byte 16.

///|
/// Connect to `host:port`; the socket lives in a worker thread.
extern "js" fn js_connect(host : String, port : Int) -> Int =
  #| function(host, port) {
  #|  const workerCode = `
  #|    const { parentPort, workerData } = process.getBuiltinModule("node:worker_threads");
  #|    const net = process.getBuiltinModule("node:net");
  #|    const { host, port, sab } = workerData;
  #|    const ctrl = new Int32Array(sab, 0, 4);
  #|    const dataView = new Uint8Array(sab, 16);
  #|    const sock = net.createConnection({ host, port });
  #|    let writePos = 0;
  #|    let closing = false;
  #|    sock.on("connect", () => Atomics.store(ctrl, 2, 1));
  #|    sock.on("error", (e) => {
  #|      Atomics.store(ctrl, 2, -1);
  #|      console.error("[bridge] socket error:", e.message);
  #|    });
  #|    sock.on("data", (chunk) => {
  #|      if (writePos + chunk.length > dataView.length) {
  #|        console.error("[bridge] buffer overflow, dropping", chunk.length, "bytes");
  #|        return;
  #|      }
  #|      dataView.set(chunk, writePos);
  #|      writePos += chunk.length;
  #|      Atomics.store(ctrl, 3, writePos);
  #|      Atomics.add(ctrl, 0, chunk.length);
  #|      Atomics.notify(ctrl, 0);
  #|    });
  #|    sock.on("close", () => { if (closing) process.exit(0); });
  #|    parentPort.on("message", (msg) => {
  #|      if (!msg) return;
  #|      if (msg.type === "send") sock.write(Buffer.from(msg.data));
  #|      if (msg.type === "close") {
  #|        closing = true;
  #|        sock.end();
  #|        setTimeout(() => process.exit(0), 1000);
  #|      }
  #|    });
  #|  `;
  #|  const { Worker } = process.getBuiltinModule("node:worker_threads");
  #|  const sab = new SharedArrayBuffer(1 << 20);
  #|  const worker = new Worker(workerCode, {
  #|    eval: true,
  #|    workerData: { host, port, sab },
  #|  });
  #|  globalThis.__e2eBridge = {
  #|    sab,
  #|    ctrl: new Int32Array(sab, 0, 4),
  #|    worker,
  #|    readPos: 0,
  #|  };
  #|  return 0;
  #| }

///|
/// Block until the worker reports connected (1) or an error (-1),
/// up to `ms` milliseconds; returns the status word (0 on timeout).
extern "js" fn js_wait_connect(ms : Int) -> Int =
  #| function(ms) {
  #|  const br = globalThis.__e2eBridge;
  #|  if (!br) return -1;
  #|  Atomics.wait(br.ctrl, 2, 0, ms);
  #|  return Atomics.load(br.ctrl, 2);
  #| }

///|
/// Send bytes over the worker's socket.
extern "js" fn js_send(data : Bytes) -> Unit =
  #| function(data) {
  #|  const br = globalThis.__e2eBridge;
  #|  if (br) br.worker.postMessage({ type: "send", data: Buffer.from(data) });
  #| }

///|
/// Block until at least one received byte is available, up to `ms`
/// milliseconds; returns the number of bytes available to read.
extern "js" fn js_wait(ms : Int) -> Int =
  #| function(ms) {
  #|  const br = globalThis.__e2eBridge;
  #|  if (!br) return 0;
  #|  const prev = Atomics.load(br.ctrl, 0);
  #|  Atomics.wait(br.ctrl, 0, prev, ms);
  #|  return Atomics.load(br.ctrl, 0) - br.readPos;
  #| }

///|
/// Copy up to `buf.length()` available bytes into `buf`; returns the
/// number of bytes copied.
extern "js" fn js_recv(buf : FixedArray[Byte]) -> Int =
  #| function(buf) {
  #|  const br = globalThis.__e2eBridge;
  #|  if (!br) return -1;
  #|  const available = Atomics.load(br.ctrl, 0) - br.readPos;
  #|  if (available <= 0) return 0;
  #|  const len = Math.min(available, buf.length);
  #|  const view = new Uint8Array(br.sab, 16 + br.readPos, len);
  #|  for (let i = 0; i < len; i++) buf[i] = view[i];
  #|  br.readPos += len;
  #|  return len;
  #| }

///|
/// Ask the worker to end the socket; the worker exits the process
/// once the socket is closed.
extern "js" fn js_close() -> Unit =
  #| function() {
  #|  const br = globalThis.__e2eBridge;
  #|  if (!br) return;
  #|  br.worker.postMessage({ type: "close" });
  #|  globalThis.__e2eBridge = undefined;
  #| }

///|
/// Exit the process with `code`.
extern "js" fn js_exit(code : Int) -> Int =
  #| function(code) {
  #|  process.exit(code);
  #|  return 0;
  #| }
