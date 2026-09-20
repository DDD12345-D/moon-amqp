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
/// A field table: a map from field names to field values
/// (§4.2.5.5 field tables).
///
/// Keys are validated against the spec naming rules on `set`; encoding
/// always writes entries in sorted key order. Decoding is tolerant: keys
/// are read as-is, in whatever order they arrive.
pub struct FieldTable {
  entries : Array[(String, FieldValue)]
} derive(Eq, @debug.Debug)

///|
/// Explicit promotions: the implicit trait-method promotion is deprecated.
pub extend FieldTable with Eq::{not_equal, equal}

///|
pub extend FieldTable with @debug.Debug::{to_repr}

///|
/// An empty table.
pub fn FieldTable::new() -> FieldTable {
  { entries: [], }
}

///|
/// A table holding a copy of `entries`. Keys are not validated here;
/// `encode` validates them.
pub fn FieldTable::from_entries(
  entries : Array[(String, FieldValue)],
) -> FieldTable {
  { entries: entries.copy(), }
}

///|
/// Number of entries.
pub fn FieldTable::length(self : FieldTable) -> Int {
  self.entries.length()
}

///|
/// Whether the table has no entries.
pub fn FieldTable::is_empty(self : FieldTable) -> Bool {
  self.entries.is_empty()
}

///|
/// Insert or replace `value` under `key`.
/// Raises `TableError::InvalidKey` when `key` violates the §4.2.5.5
/// naming rules.
pub fn FieldTable::set(
  self : FieldTable,
  key : String,
  value : FieldValue,
) -> Unit raise TableError {
  validate_key(key)
  match self.find_index(key) {
    Some(i) => self.entries[i] = (key, value)
    None => self.entries.push((key, value))
  }
}

///|
/// The value stored under `key`, if any (first match).
pub fn FieldTable::get(self : FieldTable, key : String) -> FieldValue? {
  match self.find_index(key) {
    Some(i) => Some(self.entries[i].1)
    None => None
  }
}

///|
/// Whether `key` is present.
pub fn FieldTable::contains(self : FieldTable, key : String) -> Bool {
  self.find_index(key) is Some(_)
}

///|
/// Remove `key`, if present, and return the removed value.
pub fn FieldTable::remove(self : FieldTable, key : String) -> FieldValue? {
  match self.find_index(key) {
    Some(i) => {
      let v = self.entries[i].1
      let _ = self.entries.remove(i)
      Some(v)
    }
    None => None
  }
}

///|
/// Iterate over `(key, value)` pairs in insertion order.
pub fn FieldTable::iter(self : FieldTable) -> Iter[(String, FieldValue)] {
  self.entries.iter()
}

///|
/// Write this table: a 4-byte byte-length prefix followed by the entries
/// in sorted key order (§4.2.5.5).
///
/// Raises `TableError` for invalid keys or values that do not fit their
/// wire type.
pub fn FieldTable::encode(
  self : FieldTable,
  w : @codec.Writer,
) -> Unit raise TableError {
  for entry in self.entries {
    validate_key(entry.0)
  }
  let sorted = self.entries.copy()
  sorted.sort_by(fn(a, b) { key_cmp(a.0, b.0) })
  let payload = @codec.Writer::new()
  for entry in sorted {
    let (k, v) = entry
    let kb = @utf8.encode(k)
    payload.write_byte(kb.length().to_byte())
    payload.write_bytes(kb)
    v.encode(payload)
  }
  write_payload(w, payload)
}

///|
/// Read a table: a 4-byte byte-length prefix followed by entries.
/// Keys and values are read as-is; unsorted or duplicate keys are kept.
/// Raises `TableError::Truncated` when the input ends early.
pub fn FieldTable::decode(r : @codec.Reader) -> FieldTable raise TableError {
  let len = next_u32(r)
  if len.to_uint64() > r.remaining().to_uint64() {
    raise TableError::Truncated
  }
  let end = r.remaining() - len.reinterpret_as_int()
  let entries : Array[(String, FieldValue)] = []
  while r.remaining() > end {
    let key = next_key(r)
    let value = FieldValue::decode(r)
    entries.push((key, value))
  }
  { entries, }
}

///|
/// Index of the first entry with key `key`, if any.
fn FieldTable::find_index(self : FieldTable, key : String) -> Int? {
  for i = 0; i < self.entries.length(); i = i + 1 {
    if self.entries[i].0 == key {
      return Some(i)
    }
  }
  None
}

///|
/// Check a field name against the §4.2.5.5 rules: at most 128 characters,
/// starting with a letter, `$` or `#`, continuing with letters, digits,
/// `$`, `#`, `.`, `-` or `_`. The dot and hyphen are not in the formal
/// grammar, but real brokers use them in field names: dots in capability
/// names (e.g. RabbitMQ's `basic.nack`) and hyphens in queue/exchange
/// argument names (e.g. RabbitMQ's `x-max-priority`), so they are
/// accepted for interoperability.
fn validate_key(key : String) -> Unit raise TableError {
  let len = key.length()
  if len == 0 || len > 128 {
    raise TableError::InvalidKey("field name length must be 1..=128 characters")
  }
  let mut first = true
  for ch in key {
    if !is_key_char(ch, first) {
      raise TableError::InvalidKey(
        "field name must start with a letter, '$' or '#' and continue with letters, digits, '$', '#', '.', '-' or '_'",
      )
    }
    first = false
  }
}

///|
/// Whether `ch` is allowed in a field name; `first` selects the stricter
/// first-character set.
fn is_key_char(ch : Char, first : Bool) -> Bool {
  let c = ch.to_int()
  let letter = (c >= 65 && c <= 90) || (c >= 97 && c <= 122)
  let digit = c >= 48 && c <= 57
  let special = c == 36 || c == 35
  let underscore = c == 95
  let dot = c == 46
  let hyphen = c == 45
  if first {
    letter || special
  } else {
    letter || digit || special || underscore || dot || hyphen
  }
}

///|
/// Compare two field names byte-wise. Valid field names are pure ASCII,
/// so code-unit order equals the byte order the spec sorts by.
fn key_cmp(a : String, b : String) -> Int {
  let la = a.length()
  let lb = b.length()
  let n = if la < lb { la } else { lb }
  for i = 0; i < n; i = i + 1 {
    let ca = a.at(i).to_int()
    let cb = b.at(i).to_int()
    if ca != cb {
      return if ca < cb { -1 } else { 1 }
    }
  }
  if la < lb {
    -1
  } else if la > lb {
    1
  } else {
    0
  }
}

///|
/// Read one key: a 1-byte length prefix + UTF-8 bytes.
fn next_key(r : @codec.Reader) -> String raise TableError {
  let len = next_byte(r).to_int()
  @utf8.decode(next_bytes(r, len)) catch {
    _ => raise TableError::InvalidUtf8
  }
}
