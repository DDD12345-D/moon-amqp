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
/// A consumer registered on a channel: the tag from Basic.Consume
/// and the queue it drains.
pub(all) struct Consumer {
  /// The consumer tag (the server's assignment when the request
  /// sent an empty tag).
  tag : String
  /// The queue the consumer drains.
  queue : String
  /// Basic.Consume was sent with no-ack: deliveries settle on
  /// arrival and must not be acknowledged.
  no_ack : Bool
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend Consumer with Eq::{not_equal, equal}

///|
pub extend Consumer with @debug.Debug::{to_repr}

///|
/// The lifecycle of one delivery on a channel. `Unacked` entries
/// form the prefetch window; every delivery ends in one of the
/// three settled states.
pub(all) enum DeliveryState {
  /// Delivered and awaiting ack/nack/reject.
  Unacked
  /// Settled by Basic.Ack.
  Acked
  /// Sent back with requeue: the broker requeues it and may
  /// redeliver it, with a new delivery tag and the redelivered
  /// flag set.
  Requeued
  /// Settled without requeue: the broker drops it.
  Discarded
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend DeliveryState with Eq::{not_equal, equal}

///|
pub extend DeliveryState with @debug.Debug::{to_repr}

///|
/// One delivery routed to a consumer: the routing fields from
/// Basic.Deliver plus its settlement state.
pub(all) struct Delivery {
  /// The consumer the message was delivered to.
  consumer_tag : String
  /// Delivery identifier, unique per channel while outstanding.
  delivery_tag : UInt64
  /// The broker already delivered this message at least once.
  redelivered : Bool
  /// The exchange the message was published to.
  exchange : String
  /// The routing key the message was published with.
  routing_key : String
  /// Settlement state (see `DeliveryState`).
  state : DeliveryState
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend Delivery with Eq::{not_equal, equal}

///|
pub extend Delivery with @debug.Debug::{to_repr}

///|
/// Routes Basic.Deliver methods to registered consumers and tracks
/// the settlement lifecycle of every delivery, mirroring the
/// broker's per-channel state machine. One `Dispatch` serves one
/// channel.
pub struct Dispatch {
  consumers : Array[Consumer]
  deliveries : Array[Delivery]
  mut prefetch : UInt16
}

///|
/// An empty dispatch: no consumers, no deliveries, no prefetch
/// window.
pub fn Dispatch::new() -> Dispatch {
  { consumers: [], deliveries: [], prefetch: (0).to_uint16(), }
}

///|
/// Register a consumer. A duplicate tag raises `ConsumerExists`.
pub fn Dispatch::register(
  self : Dispatch,
  consumer : Consumer,
) -> Unit raise BasicError {
  for c in self.consumers {
    if c.tag == consumer.tag {
      raise BasicError::ConsumerExists(consumer.tag)
    }
  }
  self.consumers.push(consumer)
}

///|
/// Remove a consumer by tag. An unknown tag raises
/// `UnknownConsumer`.
pub fn Dispatch::unregister(
  self : Dispatch,
  tag : String,
) -> Unit raise BasicError {
  for i in 0..<self.consumers.length() {
    if self.consumers[i].tag == tag {
      let _ = self.consumers.remove(i)
      return
    }
  }
  raise BasicError::UnknownConsumer(tag)
}

///|
/// Route a Basic.Deliver to the consumer named in it and record the
/// delivery. Deliveries to no-ack consumers settle immediately (the
/// broker expects no acknowledgement). An unknown consumer tag
/// raises `UnknownConsumer`; a delivery tag that is still
/// outstanding raises `DuplicateDelivery`; delivering beyond the
/// prefetch window raises `PrefetchExceeded`.
pub fn Dispatch::deliver(
  self : Dispatch,
  m : BasicDeliver,
) -> Delivery raise BasicError {
  let consumer = self.find_consumer(m.consumer_tag)
  if self.prefetch != (0).to_uint16() &&
    self.outstanding() >= self.prefetch.to_int() {
    raise BasicError::PrefetchExceeded
  }
  for d in self.deliveries {
    if d.delivery_tag == m.delivery_tag && d.state == Unacked {
      raise BasicError::DuplicateDelivery(m.delivery_tag)
    }
  }
  let delivery = Delivery::{
    consumer_tag: m.consumer_tag,
    delivery_tag: m.delivery_tag,
    redelivered: m.redelivered,
    exchange: m.exchange,
    routing_key: m.routing_key,
    state: if consumer.no_ack {
      Acked
    } else {
      Unacked
    },
  }
  self.deliveries.push(delivery)
  delivery
}

///|
/// Acknowledge one delivery (`multiple` false) or every outstanding
/// delivery up to and including the tag (`multiple` true). Settling
/// a tag that is unknown or already settled raises
/// `UnknownDelivery`.
pub fn Dispatch::ack(
  self : Dispatch,
  delivery_tag : UInt64,
  multiple : Bool,
) -> Unit raise BasicError {
  if !self.settle(delivery_tag, multiple, Acked) {
    raise BasicError::UnknownDelivery(delivery_tag)
  }
}

///|
/// Like `ack`, but with `requeue` the broker returns the delivery
/// to its queue for redelivery, and without it the broker drops it.
pub fn Dispatch::nack(
  self : Dispatch,
  delivery_tag : UInt64,
  multiple : Bool,
  requeue : Bool,
) -> Unit raise BasicError {
  let state = if requeue { Requeued } else { Discarded }
  if !self.settle(delivery_tag, multiple, state) {
    raise BasicError::UnknownDelivery(delivery_tag)
  }
}

///|
/// Reject one delivery (Basic.Reject has no multiple form).
pub fn Dispatch::reject(
  self : Dispatch,
  delivery_tag : UInt64,
  requeue : Bool,
) -> Unit raise BasicError {
  let state = if requeue { Requeued } else { Discarded }
  if !self.settle(delivery_tag, false, state) {
    raise BasicError::UnknownDelivery(delivery_tag)
  }
}

///|
/// Basic.Recover: with `requeue` the broker returns every
/// outstanding delivery to its queue (they may be redelivered to
/// any consumer); without it they are redelivered as-is, so they
/// stay outstanding here.
pub fn Dispatch::recover(self : Dispatch, requeue : Bool) -> Unit {
  if requeue {
    for i in 0..<self.deliveries.length() {
      let d = self.deliveries[i]
      if d.state == Unacked {
        self.deliveries[i] = set_state(d, Requeued)
      }
    }
  }
}

///|
/// The number of deliveries awaiting settlement.
pub fn Dispatch::outstanding(self : Dispatch) -> Int {
  let mut n = 0
  for d in self.deliveries {
    if d.state == Unacked {
      n += 1
    }
  }
  n
}

///|
/// Whether the prefetch window has room for one more delivery. A
/// window of 0 (the default) means no limit.
pub fn Dispatch::can_deliver(self : Dispatch) -> Bool {
  self.prefetch == (0).to_uint16() ||
  self.outstanding() < self.prefetch.to_int()
}

///|
/// Set the prefetch window (Basic.Qos prefetch-count). It applies
/// to the deliveries that follow; shrinking it below the current
/// outstanding count simply blocks new deliveries until settlements
/// free room.
pub fn Dispatch::set_prefetch(self : Dispatch, count : UInt16) -> Unit {
  self.prefetch = count
}

///|
/// The consumer registered under `tag`, raising `UnknownConsumer`
/// when there is none.
fn Dispatch::find_consumer(
  self : Dispatch,
  tag : String,
) -> Consumer raise BasicError {
  for c in self.consumers {
    if c.tag == tag {
      return c
    }
  }
  raise BasicError::UnknownConsumer(tag)
}

///|
/// Settle the matching outstanding deliveries; false when nothing
/// matched (the tag is unknown or already settled).
fn Dispatch::settle(
  self : Dispatch,
  delivery_tag : UInt64,
  multiple : Bool,
  state : DeliveryState,
) -> Bool {
  let mut matched = false
  for i in 0..<self.deliveries.length() {
    let d = self.deliveries[i]
    if d.state == Unacked &&
      (
        d.delivery_tag == delivery_tag ||
        (multiple && d.delivery_tag < delivery_tag)
      ) {
      self.deliveries[i] = set_state(d, state)
      matched = true
    }
  }
  matched
}

///|
/// Replace a delivery's settlement state, keeping its routing
/// fields.
fn set_state(d : Delivery, state : DeliveryState) -> Delivery {
  Delivery::{
    consumer_tag: d.consumer_tag,
    delivery_tag: d.delivery_tag,
    redelivered: d.redelivered,
    exchange: d.exchange,
    routing_key: d.routing_key,
    state,
  }
}
