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
/// A Basic.Deliver for the recorded consumer tag.
fn fixture_deliver(
  delivery_tag : UInt64,
  redelivered : Bool,
) -> @basic.BasicDeliver {
  @basic.BasicDeliver::{
    consumer_tag: consume_ctag,
    delivery_tag,
    redelivered,
    exchange: "",
    routing_key: "consume-q",
  }
}

///|
/// Bytes captured from a live RabbitMQ 4.3.6 broker (Erlang/OTP
/// 27.3.4.17) on 2026-09-19: amqplib 2.0.1 opens a channel, sets
/// prefetch 2, consumes "consume-q" (empty request tag; the broker
/// assigns `amq.ctag-hIvI1ZQNT97KFUbcdj8CSQ`), and settles three
/// deliveries — acking the first, nacking the second back (the
/// broker redelivers it with a new tag and the redelivered flag),
/// rejecting the third without requeue — then polls with basic.get
/// (the queue is empty) and cancels the consumer. The client-bound
/// bytes pin our encoder to amqplib's output for the same methods;
/// the server-bound bytes pin our decoder.
test "broker fixture: consume session client bytes match amqplib" {
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicQos(@basic.BasicQos::{
        prefetch_size: (0).reinterpret_as_uint(),
        prefetch_count: (2).to_uint16(),
        global: false,
      }),
    ),
    b"\x00\x3c\x00\x0a\x00\x00\x00\x00\x00\x02\x00",
  )
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicConsume(@basic.BasicConsume::{
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
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicAck(@basic.BasicAck::{
        delivery_tag: (1).to_uint64(),
        multiple: false,
      }),
    ),
    b"\x00\x3c\x00\x50\x00\x00\x00\x00\x00\x00\x00\x01\x00",
  )
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicNack(@basic.BasicNack::{
        delivery_tag: (2).to_uint64(),
        multiple: false,
        requeue: true,
      }),
    ),
    b"\x00\x3c\x00\x78\x00\x00\x00\x00\x00\x00\x00\x02\x02",
  )
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicReject(@basic.BasicReject::{
        delivery_tag: (3).to_uint64(),
        requeue: false,
      }),
    ),
    b"\x00\x3c\x00\x5a\x00\x00\x00\x00\x00\x00\x00\x03\x00",
  )
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicGet(@basic.BasicGet::{
        reserved_1: (0).to_uint16(),
        queue: "consume-q",
        no_ack: false,
      }),
    ),
    b"\x00\x3c\x00\x46\x00\x00\x09consume-q\x00",
  )
  @test.assert_eq(
    basic_enc(
      @basic.BasicMethod::@basic.BasicCancel(@basic.BasicCancel::{
        consumer_tag: consume_ctag,
        nowait: false,
      }),
    ),
    b"\x00\x3c\x00\x1e\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00",
  )
}

///|
/// The server-bound frames from the same session decode back to
/// their values.
test "broker fixture: consume session server bytes decode" {
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x0b"),
    @basic.BasicMethod::BasicQosOk(()),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x15\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ"),
    @basic.BasicMethod::@basic.BasicConsumeOk(@basic.BasicConsumeOk::{
      consumer_tag: consume_ctag,
    }),
  )
  @test.assert_eq(
    basic_dec(
      b"\x00\x3c\x00\x3c\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x09consume-q",
    ),
    @basic.BasicMethod::@basic.BasicDeliver(@basic.BasicDeliver::{
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
    @basic.BasicMethod::@basic.BasicDeliver(@basic.BasicDeliver::{
      consumer_tag: consume_ctag,
      delivery_tag: (4).to_uint64(),
      redelivered: true,
      exchange: "",
      routing_key: "consume-q",
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x48\x00"),
    @basic.BasicMethod::@basic.BasicGetEmpty(@basic.BasicGetEmpty::{
      cluster_id: "",
    }),
  )
  @test.assert_eq(
    basic_dec(b"\x00\x3c\x00\x1f\x1famq.ctag-hIvI1ZQNT97KFUbcdj8CSQ"),
    @basic.BasicMethod::@basic.BasicCancelOk(@basic.BasicCancelOk::{
      consumer_tag: consume_ctag,
    }),
  )
}

///|
/// The recorded session replays through the dispatch: prefetch 2
/// lets deliveries 1 and 2 in; ack 1 frees room for 3; nack 2 with
/// requeue brings it back as delivery 4 with the redelivered flag;
/// reject 3 and ack 4 settle everything.
test "broker fixture: the recorded settlement sequence replays" {
  let d = @basic.Dispatch::new()
  d.register(@basic.Consumer::{
    tag: consume_ctag,
    queue: "consume-q",
    no_ack: false,
  })
  d.set_prefetch((2).to_uint16())
  let _ = d.deliver(fixture_deliver((1).to_uint64(), false))
  let _ = d.deliver(fixture_deliver((2).to_uint64(), false))
  @test.assert_eq(d.outstanding(), 2)
  d.ack((1).to_uint64(), false)
  let _ = d.deliver(fixture_deliver((3).to_uint64(), false))
  d.nack((2).to_uint64(), false, true)
  let redelivery = d.deliver(fixture_deliver((4).to_uint64(), true))
  @test.assert_eq(redelivery.redelivered, true)
  @test.assert_eq(d.outstanding(), 2)
  d.reject((3).to_uint64(), false)
  d.ack((4).to_uint64(), false)
  @test.assert_eq(d.outstanding(), 0)
  @test.assert_eq(d.deliveries[0].state, Acked)
  @test.assert_eq(d.deliveries[1].state, Requeued)
  @test.assert_eq(d.deliveries[2].state, Discarded)
  @test.assert_eq(d.deliveries[3].state, Acked)
}
