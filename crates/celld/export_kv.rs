// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Decoding Durable Object key-value values for change export
//! (docs/design/change-export.md, "Special tables").
//!
//! `_cf_KV` stores each value as V8 structured-clone bytes (wire version 15,
//! see `serialize_storage_value` in `js/storage_ops.rs`), or as a stored-stub
//! row: the tag byte `0x01` and then the same kind of clone. The export
//! carries the value as JSON text, so the bytes have to be read by V8's own
//! `ValueDeserializer`, the decoder the storage API already uses, and not by
//! a reimplementation of the wire format.
//!
//! Capture runs at a safe point that has no isolate scope in hand, so the
//! decoding happens on one dedicated thread that owns a small isolate of its
//! own. The values are plain data (the storage delegate has no host objects),
//! so any isolate reads them the same way. A caller sends one batch per
//! commit and waits for the answer.
//!
//! The JSON is the decoded value with the types JSON lacks tagged. An object
//! whose only key starts with `$` is a tag:
//!
//! | value                          | JSON                                       |
//! |--------------------------------|--------------------------------------------|
//! | `null`, booleans, strings      | themselves                                 |
//! | finite numbers                 | numbers                                    |
//! | `NaN`, `±Infinity`             | `{"$number": "NaN" \| "Infinity" \| "-Infinity"}` |
//! | `undefined`, array holes       | `{"$undefined": true}`                     |
//! | `BigInt`                       | `{"$bigint": "<decimal>"}`                 |
//! | `Date`                         | `{"$date": "<ISO 8601>"}`, `null` inside when invalid |
//! | `RegExp`                       | `{"$regexp": {"source": …, "flags": …}}`   |
//! | `Map`                          | `{"$map": [[key, value], …]}`              |
//! | `Set`                          | `{"$set": [value, …]}`                     |
//! | `ArrayBuffer`, typed arrays, `DataView` | `{"$bytes": {"base64": …, "type": "<constructor>"}}` |
//! | `Error`                        | `{"$error": {"name": …, "message": …}}`    |
//! | boxed primitives               | the primitive                              |
//! | arrays                         | arrays                                     |
//! | objects                        | objects; one with any `$` key becomes `{"$object": {…}}` |
//! | a stored-stub row              | `{"$stub": <decoded marker tree>}`         |
//!
//! A value that refers to itself cannot be written as JSON and does not
//! decode. Nor does anything V8 refuses to read, a value stored in more than
//! [`MAX_DECODE_BYTES`], or one whose JSON would grow far past its stored
//! size (a sparse array, an object shared many times over). The caller
//! exports those as tagged blobs.

use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};

use v8::ValueDeserializerHelper as _;
#[cfg(test)]
use v8::ValueSerializerHelper as _;

/// First byte of a stored-stub row (`STORED_STUB_TAG` in
/// `js/storage_ops.rs`).
const STORED_STUB_TAG: u8 = 0x01;

/// The largest stored value the decoder reads. V8 cannot recover from running
/// out of heap, and a few bytes of wire format can stand for an object, so a
/// larger value is exported as its stored bytes instead.
const MAX_DECODE_BYTES: usize = 2 << 20;

/// The decoder isolate's heap limit, well above what a value of
/// [`MAX_DECODE_BYTES`] and its JSON can take.
const HEAP_LIMIT_BYTES: usize = 512 << 20;

/// Walks a deserialized value into the JSON described in the module docs.
/// Called as `encode(value, stub)`; throws on a cycle.
const ENCODE_JS: &str = r#"
(function () {
  "use strict";
  const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const base64 = (bytes) => {
    let out = "";
    let i = 0;
    for (; i + 2 < bytes.length; i += 3) {
      const n = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];
      out += ALPHABET[n >> 18] + ALPHABET[(n >> 12) & 63] + ALPHABET[(n >> 6) & 63] + ALPHABET[n & 63];
    }
    if (i + 1 === bytes.length) {
      const n = bytes[i] << 16;
      out += ALPHABET[n >> 18] + ALPHABET[(n >> 12) & 63] + "==";
    } else if (i + 2 === bytes.length) {
      const n = (bytes[i] << 16) | (bytes[i + 1] << 8);
      out += ALPHABET[n >> 18] + ALPHABET[(n >> 12) & 63] + ALPHABET[(n >> 6) & 63] + "=";
    }
    return out;
  };
  const kind = (value) => Object.prototype.toString.call(value).slice(8, -1);
  // A few stored bytes can stand for far more: a sparse array's holes, or an
  // object shared many times over. Every value walked and every character
  // written is charged, and a value that runs out does not decode.
  const BUDGET = 64 * 1024 * 1024;
  const VALUE_COST = 64;
  let budget = 0;
  const charge = (units) => {
    budget -= units;
    if (budget < 0) throw new RangeError("the value expands past the export budget");
  };
  // A plain `{}` would treat an own `__proto__` key as the prototype setter.
  const define = (out, key, value) => {
    Object.defineProperty(out, key, { value, enumerable: true, writable: true, configurable: true });
  };
  const encode = (value, ancestors) => {
    charge(VALUE_COST);
    switch (typeof value) {
      case "undefined":
        return { $undefined: true };
      case "boolean":
        return value;
      case "string":
        charge(value.length);
        return value;
      case "number":
        return Number.isFinite(value) ? value : { $number: String(value) };
      case "bigint":
        return { $bigint: value.toString() };
    }
    if (value === null) return null;
    if (ancestors.has(value)) throw new TypeError("the value refers to itself");
    ancestors.add(value);
    try {
      if (value instanceof ArrayBuffer) {
        charge(value.byteLength * 2);
        return { $bytes: { base64: base64(new Uint8Array(value)), type: "ArrayBuffer" } };
      }
      if (ArrayBuffer.isView(value)) {
        charge(value.byteLength * 2);
        const bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
        return { $bytes: { base64: base64(bytes), type: kind(value) } };
      }
      if (Array.isArray(value)) {
        const out = [];
        for (let i = 0; i < value.length; i++) out.push(encode(value[i], ancestors));
        return out;
      }
      switch (kind(value)) {
        case "Date": {
          const time = value.getTime();
          return { $date: Number.isNaN(time) ? null : value.toISOString() };
        }
        case "RegExp":
          return { $regexp: { source: value.source, flags: value.flags } };
        case "Map":
          return { $map: [...value].map(([k, v]) => [encode(k, ancestors), encode(v, ancestors)]) };
        case "Set":
          return { $set: [...value].map((v) => encode(v, ancestors)) };
        case "Error":
          return { $error: { name: String(value.name), message: String(value.message) } };
        case "Number":
        case "String":
        case "Boolean":
        case "BigInt":
          return encode(value.valueOf(), ancestors);
      }
      const out = {};
      let tagged = false;
      for (const key of Object.keys(value)) {
        charge(key.length);
        if (key.startsWith("$")) tagged = true;
        define(out, key, encode(value[key], ancestors));
      }
      return tagged ? { $object: out } : out;
    } finally {
      ancestors.delete(value);
    }
  };
  return (value, stub) => {
    budget = BUDGET;
    const encoded = encode(value, new Set());
    return JSON.stringify(stub ? { $stub: encoded } : encoded);
  };
})()
"#;

enum Job {
    Decode {
        values: Vec<Vec<u8>>,
        reply: mpsc::SyncSender<Vec<Option<String>>>,
    },
    /// Serialize the value a JavaScript expression evaluates to, the way the
    /// storage API does, for tests.
    #[cfg(test)]
    Encode {
        expression: String,
        reply: mpsc::SyncSender<Option<Vec<u8>>>,
    },
}

/// Decode V8-serialized `_cf_KV` values to export JSON text, in order. `None`
/// for a value that does not decode, and for every value when the decoder
/// is unavailable.
pub(crate) fn decode(values: Vec<Vec<u8>>) -> Vec<Option<String>> {
    let count = values.len();
    if count == 0 {
        return Vec::new();
    }
    let (reply, answer) = mpsc::sync_channel(1);
    if !send(Job::Decode { values, reply }) {
        return vec![None; count];
    }
    match answer.recv() {
        Ok(decoded) if decoded.len() == count => decoded,
        _ => {
            tracing::error!("export: the key-value decoder stopped");
            vec![None; count]
        }
    }
}

fn send(job: Job) -> bool {
    static JOBS: OnceLock<Option<Mutex<mpsc::Sender<Job>>>> = OnceLock::new();
    let jobs = JOBS.get_or_init(|| {
        crate::js::Engine::init();
        let (jobs, queue) = mpsc::channel();
        // Spawned from the first caller, a cell thread in the node, after V8
        // is initialized, so it inherits access to V8's protected memory.
        match std::thread::Builder::new()
            .name("export-kv-decode".to_string())
            .spawn(move || run(queue))
        {
            Ok(_) => Some(Mutex::new(jobs)),
            Err(error) => {
                tracing::error!(%error, "export: start the key-value decoder");
                None
            }
        }
    });
    let Some(jobs) = jobs else {
        return false;
    };
    let sent = jobs
        .lock()
        .map(|jobs| jobs.send(job).is_ok())
        .unwrap_or(false);
    if !sent {
        tracing::error!("export: the key-value decoder stopped");
    }
    sent
}

struct Delegate;

impl v8::ValueDeserializerImpl for Delegate {}

#[cfg(test)]
impl v8::ValueSerializerImpl for Delegate {
    fn throw_data_clone_error(&self, scope: &mut v8::PinScope, message: v8::Local<v8::String>) {
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
    }
}

fn run(queue: mpsc::Receiver<Job>) {
    let params = v8::CreateParams::default().heap_limits(0, HEAP_LIMIT_BYTES);
    let mut isolate = v8::Isolate::new(params);
    let (context, encode) = {
        v8::scope!(let hs, &mut isolate);
        let context = v8::Context::new(hs, Default::default());
        let scope = &mut v8::ContextScope::new(hs, context);
        let source = v8::String::new(scope, ENCODE_JS).expect("encoder source");
        let encode = v8::Script::compile(scope, source, None)
            .and_then(|script| script.run(scope))
            .and_then(|value| value.try_cast::<v8::Function>().ok())
            .expect("the key-value encoder compiles");
        (
            v8::Global::new(scope, context),
            v8::Global::new(scope, encode),
        )
    };
    for job in queue {
        match job {
            Job::Decode { values, reply } => {
                // One handle scope per value, so a batch's earlier values are
                // garbage while the later ones decode.
                let decoded = values
                    .iter()
                    .map(|bytes| {
                        if bytes.len() > MAX_DECODE_BYTES {
                            return None;
                        }
                        v8::scope!(let hs, &mut isolate);
                        let local = v8::Local::new(hs, &context);
                        let scope = &mut v8::ContextScope::new(hs, local);
                        let encode = v8::Local::new(scope, &encode);
                        decode_one(scope, encode, bytes)
                    })
                    .collect();
                let _ = reply.send(decoded);
            }
            #[cfg(test)]
            Job::Encode { expression, reply } => {
                v8::scope!(let hs, &mut isolate);
                let local = v8::Local::new(hs, &context);
                let scope = &mut v8::ContextScope::new(hs, local);
                let _ = reply.send(encode_expression(scope, &expression));
            }
        }
    }
}

fn decode_one(
    scope: &mut v8::PinScope,
    encode: v8::Local<v8::Function>,
    bytes: &[u8],
) -> Option<String> {
    let (stub, body) = match bytes.split_first() {
        Some((&STORED_STUB_TAG, body)) => (true, body),
        _ => (false, bytes),
    };
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = &mut tc.init();
    let context = tc.get_current_context();
    let value = {
        let deserializer = v8::ValueDeserializer::new(tc, Box::new(Delegate), body);
        deserializer.set_supports_legacy_wire_format(true);
        if !deserializer.read_header(context).unwrap_or(false) {
            return None;
        }
        deserializer.read_value(context)?
    };
    let receiver = v8::undefined(tc).into();
    let stub = v8::Boolean::new(tc, stub).into();
    let json = encode.call(tc, receiver, &[value, stub])?;
    let json = json.try_cast::<v8::String>().ok()?;
    Some(json.to_rust_string_lossy(tc))
}

#[cfg(test)]
fn encode_expression(scope: &mut v8::PinScope, expression: &str) -> Option<Vec<u8>> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = &mut tc.init();
    let context = tc.get_current_context();
    let source = v8::String::new(tc, expression)?;
    let value = v8::Script::compile(tc, source, None)?.run(tc)?;
    let serializer = v8::ValueSerializer::new(tc, Box::new(Delegate));
    serializer.write_header();
    if !serializer.write_value(context, value).unwrap_or(false) {
        return None;
    }
    let mut bytes = serializer.release();
    // The storage API's pin to wire version 15.
    if bytes.starts_with(&[0xff, 0x10]) {
        bytes[1] = 0x0f;
    }
    Some(bytes)
}

/// Serialize what `expression` evaluates to as the storage API stores it.
#[cfg(test)]
pub(crate) fn encode_for_test(expression: &str) -> Vec<u8> {
    let (reply, answer) = mpsc::sync_channel(1);
    assert!(send(Job::Encode {
        expression: expression.to_string(),
        reply,
    }));
    answer
        .recv()
        .unwrap()
        .unwrap_or_else(|| panic!("{expression} does not serialize"))
}

#[cfg(test)]
mod tests;
