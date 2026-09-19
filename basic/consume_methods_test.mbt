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
/// The consumer tag the broker assigned in the recorded consume
/// session (basic/consume_fixture_test.mbt).
let consume_ctag : String = "amq.ctag-hIvI1ZQNT97KFUbcdj8CSQ"

///|
/// Golden encode for the consumption methods: the bytes the spec
/// (§3.2.2) and the §4.2.5.2 bit packing dictate, later verified
/// against the recorded amqplib session.
test "basic consume methods golden encode" {
  // basic.qos: prefetch-size 0, prefetch-count 2, global off.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicQos(BasicQos::{
        prefetch_size: (0).reinterpret_as_uint(),
        prefetch_count: (2).to_uint16(),
        global: false,
      }),
    ),
    b"\x00\x3c\x00\x0a\x00\x00\x00\x00\x00\x02\x00",
  )
  // basic.qos with global.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicQos(BasicQos::{
        prefetch_size: (0).reinterpret_as_uint(),
        prefetch_count: (0).to_uint16(),
        global: true,
      }),
    ),
    b"\x00\x3c\x00\x0a\x00\x00\x00\x00\x00\x00\x01",
  )
  // basic.qos-ok: no arguments.
  @test.assert_eq(basic_enc(BasicMethod::BasicQosOk(())), b"\x00\x3c\x00\x0b")
  // basic.consume: empty tag, no flags, empty arguments table.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicConsume(BasicConsume::{
        reserved_1: (0).to_uint16(),
        queue: "consume-q",
        consumer_tag: "",
        no_local: false,
        no_ack: false,
        exclusive: false,
        nowait: false,
        arguments: @table.FieldTable::new(),
      }),
    ),
    b"\x00\x3c\x00\x14\x00\x00\x09consume-q\x00\x00\x00\x00\x00\x00",
  )
  // basic.consume with no-ack (bit 1) and exclusive (bit 2), and
  // one argument: bits 0x06, table of one I32 entry.
  let args = @table.FieldTable::new()
  args.set("x-priority", @table.FieldValue::I32(5))
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicConsume(BasicConsume::{
        reserved_1: (0).to_uint16(),
        queue: "consume-q",
        consumer_tag: "ctag1",
        no_local: false,
        no_ack: true,
        exclusive: true,
        nowait: false,
        arguments: args,
      }),
    ),
    b"\x00\x3c\x00\x14\x00\x00\x09consume-q\x05ctag1\x06\x00\x00\x00\x10\x0ax-priority\x49\x00\x00\x00\x05",
  )
  // basic.consume-ok.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicConsumeOk(BasicConsumeOk::{
        consumer_tag: consume_ctag,
      }),
    ),
    b"\x00\x3c\x00\x15\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ",
  )
  // basic.cancel.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicCancel(BasicCancel::{
        consumer_tag: consume_ctag,
        nowait: false,
      }),
    ),
    b"\x00\x3c\x00\x1e\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00",
  )
  // basic.cancel with nowait.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicCancel(BasicCancel::{ consumer_tag: "c", nowait: true, }),
    ),
    b"\x00\x3c\x00\x1e\x01c\x01",
  )
  // basic.cancel-ok.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicCancelOk(BasicCancelOk::{ consumer_tag: consume_ctag, }),
    ),
    b"\x00\x3c\x00\x1f\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ",
  )
  // basic.deliver: tag 1, not redelivered, default exchange.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicDeliver(BasicDeliver::{
        consumer_tag: consume_ctag,
        delivery_tag: (1).to_uint64(),
        redelivered: false,
        exchange: "",
        routing_key: "consume-q",
      }),
    ),
    b"\x00\x3c\x00\x3c\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x09consume-q",
  )
  // basic.deliver: tag 4, redelivered.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicDeliver(BasicDeliver::{
        consumer_tag: consume_ctag,
        delivery_tag: (4).to_uint64(),
        redelivered: true,
        exchange: "",
        routing_key: "consume-q",
      }),
    ),
    b"\x00\x3c\x00\x3c\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00\x00\x00\x00\x00\x00\x00\x04\x01\x00\x09consume-q",
  )
  // basic.get.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicGet(BasicGet::{
        reserved_1: (0).to_uint16(),
        queue: "consume-q",
        no_ack: false,
      }),
    ),
    b"\x00\x3c\x00\x46\x00\x00\x09consume-q\x00",
  )
  // basic.get with no-ack.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicGet(BasicGet::{
        reserved_1: (0).to_uint16(),
        queue: "consume-q",
        no_ack: true,
      }),
    ),
    b"\x00\x3c\x00\x46\x00\x00\x09consume-q\x01",
  )
  // basic.get-ok: delivery-tag 9, message-count 3.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicGetOk(BasicGetOk::{
        delivery_tag: (9).to_uint64(),
        redelivered: false,
        exchange: "",
        routing_key: "consume-q",
        message_count: (3).reinterpret_as_uint(),
      }),
    ),
    b"\x00\x3c\x00\x47\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x09consume-q\x00\x00\x00\x03",
  )
  // basic.get-empty: the reserved short string is empty.
  @test.assert_eq(
    basic_enc(BasicMethod::BasicGetEmpty(BasicGetEmpty::{ cluster_id: "", })),
    b"\x00\x3c\x00\x48\x00",
  )
  // basic.ack.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicAck(BasicAck::{
        delivery_tag: (1).to_uint64(),
        multiple: false,
      }),
    ),
    b"\x00\x3c\x00\x50\x00\x00\x00\x00\x00\x00\x00\x01\x00",
  )
  // basic.ack with multiple.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicAck(BasicAck::{
        delivery_tag: (2).to_uint64(),
        multiple: true,
      }),
    ),
    b"\x00\x3c\x00\x50\x00\x00\x00\x00\x00\x00\x00\x02\x01",
  )
  // basic.reject without requeue.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicReject(BasicReject::{
        delivery_tag: (3).to_uint64(),
        requeue: false,
      }),
    ),
    b"\x00\x3c\x00\x5a\x00\x00\x00\x00\x00\x00\x00\x03\x00",
  )
  // basic.reject with requeue.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicReject(BasicReject::{
        delivery_tag: (3).to_uint64(),
        requeue: true,
      }),
    ),
    b"\x00\x3c\x00\x5a\x00\x00\x00\x00\x00\x00\x00\x03\x01",
  )
  // basic.recover-async with requeue.
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicRecoverAsync(BasicRecoverAsync::{ requeue: true, }),
    ),
    b"\x00\x3c\x00\x64\x01",
  )
  // basic.recover without requeue.
  @test.assert_eq(
    basic_enc(BasicMethod::BasicRecover(BasicRecover::{ requeue: false, })),
    b"\x00\x3c\x00\x6e\x00",
  )
  // basic.recover with requeue.
  @test.assert_eq(
    basic_enc(BasicMethod::BasicRecover(BasicRecover::{ requeue: true, })),
    b"\x00\x3c\x00\x6e\x01",
  )
  // basic.recover-ok: no arguments.
  @test.assert_eq(
    basic_enc(BasicMethod::BasicRecoverOk(())),
    b"\x00\x3c\x00\x6f",
  )
  // basic.nack: multiple off, requeue on (bits 0x02).
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicNack(BasicNack::{
        delivery_tag: (2).to_uint64(),
        multiple: false,
        requeue: true,
      }),
    ),
    b"\x00\x3c\x00\x78\x00\x00\x00\x00\x00\x00\x00\x02\x02",
  )
  // basic.nack: multiple on, requeue off (bits 0x01).
  @test.assert_eq(
    basic_enc(
      BasicMethod::BasicNack(BasicNack::{
        delivery_tag: (3).to_uint64(),
        multiple: true,
        requeue: false,
      }),
    ),
    b"\x00\x3c\x00\x78\x00\x00\x00\x00\x00\x00\x00\x03\x01",
  )
}

///|
/// Golden decode: the server-bound vectors read back to their
/// values.
test "basic consume methods golden decode" {
  @test.assert_eq(basic_dec(b"\x00\x3c\x00\x0b"), BasicMethod::BasicQosOk(()))
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x15\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ"),
    BasicMethod::BasicConsumeOk(BasicConsumeOk::{ consumer_tag: consume_ctag, }),
  )
  @test.assert_eq(
    basic_dec(
      b"\x00\x3c\x00\x3c\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x09consume-q",
    ),
    BasicMethod::BasicDeliver(BasicDeliver::{
      consumer_tag: consume_ctag,
      delivery_tag: (1).to_uint64(),
      redelivered: false,
      exchange: "",
      routing_key: "consume-q",
    }),
  )
  @test.assert_eq(
    basic_dec(
      b"\x00\x3c\x00\x3c\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00\x00\x00\x00\x00\x00\x00\x04\x01\x00\x09consume-q",
    ),
    BasicMethod::BasicDeliver(BasicDeliver::{
      consumer_tag: consume_ctag,
      delivery_tag: (4).to_uint64(),
      redelivered: true,
      exchange: "",
      routing_key: "consume-q",
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x48\x00"),
    BasicMethod::BasicGetEmpty(BasicGetEmpty::{ cluster_id: "", }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x1f\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ"),
    BasicMethod::BasicCancelOk(BasicCancelOk::{ consumer_tag: consume_ctag, }),
  )
}

///|
/// Roundtrip: encoding then decoding returns the value, covering
/// the flag combinations the golden vectors spare.
test "basic consume methods roundtrip" {
  let args = @table.FieldTable::new()
  args.set("x-priority", @table.FieldValue::I32(5))
  let values : Array[BasicMethod] = [
    BasicMethod::BasicQos(BasicQos::{
      prefetch_size: (0).reinterpret_as_uint(),
      prefetch_count: (7).to_uint16(),
      global: true,
    }),
    BasicMethod::BasicConsume(BasicConsume::{
      reserved_1: (0).to_uint16(),
      queue: "consume-q",
      consumer_tag: "ctag1",
      no_local: true,
      no_ack: false,
      exclusive: true,
      nowait: true,
      arguments: args,
    }),
    BasicMethod::BasicConsumeOk(BasicConsumeOk::{ consumer_tag: "ctag1", }),
    BasicMethod::BasicCancel(BasicCancel::{
      consumer_tag: "ctag1",
      nowait: true,
    }),
    BasicMethod::BasicCancelOk(BasicCancelOk::{ consumer_tag: "ctag1", }),
    BasicMethod::BasicDeliver(BasicDeliver::{
      consumer_tag: "ctag1",
      delivery_tag: (42).to_uint64(),
      redelivered: true,
      exchange: "logs",
      routing_key: "info",
    }),
    BasicMethod::BasicGet(BasicGet::{
      reserved_1: (0).to_uint16(),
      queue: "consume-q",
      no_ack: true,
    }),
    BasicMethod::BasicGetOk(BasicGetOk::{
      delivery_tag: (42).to_uint64(),
      redelivered: false,
      exchange: "logs",
      routing_key: "info",
      message_count: (3).reinterpret_as_uint(),
    }),
    BasicMethod::BasicGetEmpty(BasicGetEmpty::{ cluster_id: "", }),
    BasicMethod::BasicAck(BasicAck::{
      delivery_tag: (42).to_uint64(),
      multiple: true,
    }),
    BasicMethod::BasicReject(BasicReject::{
      delivery_tag: (42).to_uint64(),
      requeue: true,
    }),
    BasicMethod::BasicRecoverAsync(BasicRecoverAsync::{ requeue: false, }),
    BasicMethod::BasicRecover(BasicRecover::{ requeue: true, }),
    BasicMethod::BasicRecoverOk(()),
    BasicMethod::BasicNack(BasicNack::{
      delivery_tag: (42).to_uint64(),
      multiple: true,
      requeue: true,
    }),
  ]
  for m in values {
    @test.assert_eq(basic_dec(basic_enc(m)), m)
  }
}

///|
/// The reserved ticket in basic.consume is fixed to 0.
test "basic.consume reserved-1 must be zero" {
  let raised = try {
    let _ = basic_enc(
      BasicMethod::BasicConsume(BasicConsume::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        consumer_tag: "",
        no_local: false,
        no_ack: false,
        exclusive: false,
        nowait: false,
        arguments: @table.FieldTable::new(),
      }),
    )
    false
  } catch {
    BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// The reserved ticket in basic.get is fixed to 0.
test "basic.get reserved-1 must be zero" {
  let raised = try {
    let _ = basic_enc(
      BasicMethod::BasicGet(BasicGet::{
        reserved_1: (1).to_uint16(),
        queue: "q",
        no_ack: false,
      }),
    )
    false
  } catch {
    BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// The reserved short string in basic.get-empty is fixed to the
/// empty string.
test "basic.get-empty cluster-id must be empty" {
  let raised = try {
    let _ = basic_enc(
      BasicMethod::BasicGetEmpty(BasicGetEmpty::{ cluster_id: "x", }),
    )
    false
  } catch {
    BasicError::ValueOutOfRange(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A qos payload ending inside prefetch-size is truncated.
test "truncated qos raises" {
  let raised = try {
    let _ = basic_dec(b"\x00\x3c\x00\x0a\x00\x00\x00")
    false
  } catch {
    BasicError::Truncated => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A deliver payload ending inside its arguments is truncated.
test "truncated deliver raises" {
  let raised = try {
    let _ = basic_dec(b"\x00\x3c\x00\x3c\x01a")
    false
  } catch {
    BasicError::Truncated => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A cancel payload with a non-UTF-8 consumer tag raises.
test "invalid utf-8 in cancel raises" {
  let raised = try {
    let _ = basic_dec(b"\x00\x3c\x00\x1e\x01\xff")
    false
  } catch {
    BasicError::InvalidUtf8 => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
