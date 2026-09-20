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
/// A Basic.Deliver for the consumer `tag` with the given delivery
/// tag.
fn mk_deliver(
  consumer_tag : String,
  delivery_tag : UInt64,
) -> @basic.BasicDeliver {
  @basic.BasicDeliver::{
    consumer_tag,
    delivery_tag,
    redelivered: false,
    exchange: "",
    routing_key: "consume-q",
  }
}

///|
/// A redelivered Basic.Deliver.
fn mk_redeliver(
  consumer_tag : String,
  delivery_tag : UInt64,
) -> @basic.BasicDeliver {
  @basic.BasicDeliver::{
    consumer_tag,
    delivery_tag,
    redelivered: true,
    exchange: "",
    routing_key: "consume-q",
  }
}

///|
/// A consumer draining `queue`.
fn mk_consumer(tag : String, no_ack : Bool) -> @basic.Consumer {
  @basic.Consumer::{ tag, queue: "consume-q", no_ack, }
}

///|
/// Multiple consumers: delivers route by consumer tag, and the
/// deliveries interleave without disturbing each other.
test "dispatch routes delivers to the right consumer" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  d.register(mk_consumer("ctag-b", false))
  let da = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let db = d.deliver(mk_deliver("ctag-b", (2).to_uint64()))
  @test.assert_eq(da.consumer_tag, "ctag-a")
  @test.assert_eq(db.consumer_tag, "ctag-b")
  @test.assert_eq(da.state, Unacked)
  @test.assert_eq(d.outstanding(), 2)
}

///|
/// A deliver for a tag that was never registered raises
/// `UnknownConsumer`.
test "deliver to an unknown consumer raises" {
  let d = @basic.Dispatch::new()
  let raised = try {
    let _ = d.deliver(mk_deliver("ctag-x", (1).to_uint64()))
    false
  } catch {
    @basic.BasicError::UnknownConsumer("ctag-x") => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Two consumers may not share a tag.
test "registering a duplicate consumer tag raises" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let raised = try {
    d.register(mk_consumer("ctag-a", false))
    false
  } catch {
    @basic.BasicError::ConsumerExists(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Unregistering removes the consumer: deliveries to it fail, while
/// the others keep working.
test "unregister removes a consumer" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  d.register(mk_consumer("ctag-b", false))
  d.unregister("ctag-a")
  let raised = try {
    let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
    false
  } catch {
    @basic.BasicError::UnknownConsumer(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
  let db = d.deliver(mk_deliver("ctag-b", (2).to_uint64()))
  @test.assert_eq(db.consumer_tag, "ctag-b")
}

///|
/// Unregistering an unknown tag raises.
test "unregistering an unknown consumer raises" {
  let d = @basic.Dispatch::new()
  let raised = try {
    d.unregister("ctag-x")
    false
  } catch {
    @basic.BasicError::UnknownConsumer(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Ack settles exactly the named delivery.
test "ack settles one delivery" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  d.ack((1).to_uint64(), false)
  @test.assert_eq(d.deliveries[0].state, Acked)
  @test.assert_eq(d.deliveries[1].state, Unacked)
  @test.assert_eq(d.outstanding(), 1)
}

///|
/// Ack with multiple settles every outstanding delivery up to and
/// including the tag, leaving later ones outstanding.
test "ack multiple settles all deliveries up to the tag" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (3).to_uint64()))
  d.ack((2).to_uint64(), true)
  @test.assert_eq(d.deliveries[0].state, Acked)
  @test.assert_eq(d.deliveries[1].state, Acked)
  @test.assert_eq(d.deliveries[2].state, Unacked)
  @test.assert_eq(d.outstanding(), 1)
}

///|
/// Ack only touches the matching consumer's deliveries.
test "ack leaves other consumers' deliveries outstanding" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  d.register(mk_consumer("ctag-b", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-b", (2).to_uint64()))
  d.ack((1).to_uint64(), false)
  @test.assert_eq(d.deliveries[0].state, Acked)
  @test.assert_eq(d.deliveries[1].state, Unacked)
}

///|
/// Settling a tag that was never delivered raises `UnknownDelivery`.
test "ack of an unknown tag raises" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let raised = try {
    d.ack((9).to_uint64(), false)
    false
  } catch {
    @basic.BasicError::UnknownDelivery(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A delivery may be settled only once.
test "ack of an already settled tag raises" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  d.ack((1).to_uint64(), false)
  let raised = try {
    d.ack((1).to_uint64(), false)
    false
  } catch {
    @basic.BasicError::UnknownDelivery(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// Nack with requeue sends the delivery back for redelivery.
test "nack with requeue returns the delivery to the queue" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  d.nack((1).to_uint64(), false, true)
  @test.assert_eq(d.deliveries[0].state, Requeued)
  @test.assert_eq(d.deliveries[1].state, Unacked)
  @test.assert_eq(d.outstanding(), 1)
}

///|
/// Nack without requeue discards the delivery.
test "nack without requeue discards the delivery" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  d.nack((1).to_uint64(), false, false)
  @test.assert_eq(d.deliveries[0].state, Discarded)
  @test.assert_eq(d.outstanding(), 0)
}

///|
/// Nack with multiple settles every outstanding delivery up to the
/// tag.
test "nack multiple settles all deliveries up to the tag" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (3).to_uint64()))
  d.nack((2).to_uint64(), true, true)
  @test.assert_eq(d.deliveries[0].state, Requeued)
  @test.assert_eq(d.deliveries[1].state, Requeued)
  @test.assert_eq(d.deliveries[2].state, Unacked)
}

///|
/// Reject with requeue sends the delivery back; without it the
/// delivery is discarded.
test "reject requeues or discards one delivery" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  d.reject((1).to_uint64(), true)
  @test.assert_eq(d.deliveries[0].state, Requeued)
  let d2 = @basic.Dispatch::new()
  d2.register(mk_consumer("ctag-a", false))
  let _ = d2.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  d2.reject((1).to_uint64(), false)
  @test.assert_eq(d2.deliveries[0].state, Discarded)
}

///|
/// Recover with requeue returns every outstanding delivery;
/// already settled deliveries are untouched.
test "recover with requeue returns all outstanding deliveries" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  d.ack((1).to_uint64(), false)
  d.recover(true)
  @test.assert_eq(d.deliveries[0].state, Acked)
  @test.assert_eq(d.deliveries[1].state, Requeued)
  @test.assert_eq(d.outstanding(), 0)
}

///|
/// Recover without requeue redelivers the messages as-is, so they
/// stay outstanding here.
test "recover without requeue keeps deliveries outstanding" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  d.recover(false)
  @test.assert_eq(d.deliveries[0].state, Unacked)
  @test.assert_eq(d.outstanding(), 1)
}

///|
/// A no-ack consumer's deliveries settle on arrival and must not be
/// acknowledged.
test "deliveries to a no-ack consumer settle immediately" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", true))
  let delivery = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  @test.assert_eq(delivery.state, Acked)
  @test.assert_eq(d.outstanding(), 0)
  let raised = try {
    d.ack((1).to_uint64(), false)
    false
  } catch {
    @basic.BasicError::UnknownDelivery(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}

///|
/// A redelivery records the redelivered flag.
test "redelivery records the redelivered flag" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let delivery = d.deliver(mk_redeliver("ctag-a", (4).to_uint64()))
  @test.assert_eq(delivery.redelivered, true)
}

///|
/// The prefetch window gates deliveries: the third delivery of a
/// prefetch-2 channel is refused until an ack frees room.
test "prefetch gates deliveries" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  d.set_prefetch((2).to_uint16())
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  @test.assert_eq(d.can_deliver(), false)
  let raised = try {
    let _ = d.deliver(mk_deliver("ctag-a", (3).to_uint64()))
    false
  } catch {
    @basic.BasicError::PrefetchExceeded => true
    _ => false
  }
  @test.assert_eq(raised, true)
  d.ack((1).to_uint64(), false)
  @test.assert_eq(d.can_deliver(), true)
  let delivery = d.deliver(mk_deliver("ctag-a", (3).to_uint64()))
  @test.assert_eq(delivery.delivery_tag, (3).to_uint64())
  @test.assert_eq(d.outstanding(), 2)
}

///|
/// A prefetch window of 0 means no limit.
test "prefetch of zero means no limit" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (2).to_uint64()))
  let _ = d.deliver(mk_deliver("ctag-a", (3).to_uint64()))
  @test.assert_eq(d.outstanding(), 3)
  @test.assert_eq(d.can_deliver(), true)
}

///|
/// Delivering the same outstanding tag again raises
/// `DuplicateDelivery`.
test "delivering the same outstanding tag again raises" {
  let d = @basic.Dispatch::new()
  d.register(mk_consumer("ctag-a", false))
  let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
  let raised = try {
    let _ = d.deliver(mk_deliver("ctag-a", (1).to_uint64()))
    false
  } catch {
    @basic.BasicError::DuplicateDelivery(_) => true
    _ => false
  }
  @test.assert_eq(raised, true)
}
