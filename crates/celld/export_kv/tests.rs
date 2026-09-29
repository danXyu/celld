// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

fn json(expression: &str) -> serde_json::Value {
    let bytes = encode_for_test(expression);
    let decoded = decode(vec![bytes]).pop().unwrap();
    let decoded = decoded.unwrap_or_else(|| panic!("{expression} does not decode"));
    serde_json::from_str(&decoded).unwrap()
}

#[test]
fn storage_bytes_are_pinned_to_wire_version_15() {
    assert!(encode_for_test("1").starts_with(&[0xff, 0x0f]));
}

#[test]
fn hand_written_version_15_bytes_decode() {
    // "hi": a one-byte string of length 2.
    let string = vec![0xff, 0x0f, b'"', 0x02, b'h', b'i'];
    // {a: 1}: begin object, key "a", int32 1 (zigzag 2), end with 1 property.
    let object = vec![0xff, 0x0f, b'o', b'"', 0x01, b'a', b'I', 0x02, b'{', 0x01];
    assert_eq!(
        decode(vec![string, object]),
        [Some(r#""hi""#.to_string()), Some(r#"{"a":1}"#.to_string())]
    );
}

#[test]
fn json_types_come_through_as_themselves() {
    assert_eq!(
        json(
            r#"({s: "text", n: 1.5, i: -3, t: true, f: false, z: null, a: [1, "two", [3]], o: {deep: {er: 1}}})"#
        ),
        serde_json::json!({
            "s": "text", "n": 1.5, "i": -3, "t": true, "f": false, "z": null,
            "a": [1, "two", [3]], "o": {"deep": {"er": 1}},
        })
    );
    assert_eq!(json("'plain'"), serde_json::json!("plain"));
    assert_eq!(json("42"), serde_json::json!(42));
}

#[test]
fn types_json_lacks_are_tagged() {
    assert_eq!(
        json(
            "({u: undefined, nan: NaN, inf: -Infinity, big: 12345678901234567890n, \
              date: new Date(Date.UTC(2026, 8, 29, 1, 2, 3)), bad: new Date(NaN), \
              re: /a+b/gi, map: new Map([[1, 'one'], ['k', {v: 2}]]), set: new Set(['x', 3]), \
              buf: new Uint8Array([1, 2, 255]).buffer, view: new Uint16Array([1, 2]), \
              err: new RangeError('too far'), boxed: new String('s'), holes: [1, , 3]})"
        ),
        serde_json::json!({
            "u": {"$undefined": true},
            "nan": {"$number": "NaN"},
            "inf": {"$number": "-Infinity"},
            "big": {"$bigint": "12345678901234567890"},
            "date": {"$date": "2026-09-29T01:02:03.000Z"},
            "bad": {"$date": null},
            "re": {"$regexp": {"source": "a+b", "flags": "gi"}},
            "map": {"$map": [[1, "one"], ["k", {"v": 2}]]},
            "set": {"$set": ["x", 3]},
            "buf": {"$bytes": {"base64": "AQL/", "type": "ArrayBuffer"}},
            "view": {"$bytes": {"base64": "AQACAA==", "type": "Uint16Array"}},
            "err": {"$error": {"name": "RangeError", "message": "too far"}},
            "boxed": "s",
            "holes": [1, {"$undefined": true}, 3],
        })
    );
}

#[test]
fn objects_with_dollar_keys_are_wrapped_so_tags_stay_unambiguous() {
    assert_eq!(
        json(r#"({$date: "not a date", n: {$bigint: 1}})"#),
        serde_json::json!({"$object": {"$date": "not a date", "n": {"$object": {"$bigint": 1}}}})
    );
}

#[test]
fn shared_references_repeat_and_cycles_do_not_decode() {
    assert_eq!(
        json("(() => { const s = {x: 1}; return [s, s]; })()"),
        serde_json::json!([{"x": 1}, {"x": 1}])
    );
    let cycle = encode_for_test("(() => { const o = {}; o.self = o; return o; })()");
    assert_eq!(decode(vec![cycle]), [None]);
}

#[test]
fn stored_stub_rows_are_tagged() {
    let mut row = vec![STORED_STUB_TAG];
    row.extend(encode_for_test(r#"({"\u0001stub": 7, t: "iso"})"#));
    let decoded: serde_json::Value =
        serde_json::from_str(&decode(vec![row]).pop().unwrap().unwrap()).unwrap();
    assert_eq!(
        decoded,
        serde_json::json!({"$stub": {"\u{1}stub": 7, "t": "iso"}})
    );
}

#[test]
fn bytes_v8_cannot_read_do_not_decode() {
    assert_eq!(
        decode(vec![
            Vec::new(),
            b"{\"json\": true}".to_vec(),
            vec![0xff, 0x0f, b'o'],
            vec![STORED_STUB_TAG],
        ]),
        [None, None, None, None]
    );
}

#[test]
fn a_batch_keeps_its_order() {
    let values: Vec<Vec<u8>> = (0..50).map(|i| encode_for_test(&i.to_string())).collect();
    let decoded = decode(values);
    assert_eq!(
        decoded,
        (0..50).map(|i| Some(i.to_string())).collect::<Vec<_>>()
    );
    assert!(decode(Vec::new()).is_empty());
}

#[test]
fn values_over_the_decode_limit_are_not_read() {
    let large = encode_for_test(&format!("'{}'", "x".repeat(MAX_DECODE_BYTES)));
    assert!(large.len() > MAX_DECODE_BYTES);
    let small = encode_for_test("'x'");
    assert_eq!(
        decode(vec![large, small]),
        [None, Some(r#""x""#.to_string())]
    );
}

#[test]
fn an_own_proto_key_is_kept() {
    assert_eq!(
        json(r#"JSON.parse('{"__proto__": {"kept": 1}, "normal": 2}')"#),
        serde_json::json!({"__proto__": {"kept": 1}, "normal": 2})
    );
    assert_eq!(
        json(r#"JSON.parse('{"__proto__": 1, "$x": 2}')"#),
        serde_json::json!({"$object": {"__proto__": 1, "$x": 2}})
    );
}

#[test]
fn values_that_expand_past_the_budget_do_not_decode() {
    let sparse = encode_for_test("new Array(10_000_000)");
    assert!(sparse.len() < 32);
    // 2^30 leaves behind 30 shared levels.
    let shared = encode_for_test(
        "(() => { let a = [1]; for (let i = 0; i < 30; i++) a = [a, a]; return a; })()",
    );
    assert!(shared.len() < 1024);
    let small = encode_for_test("new Array(3)");
    assert_eq!(
        decode(vec![sparse, shared, small]),
        [
            None,
            None,
            Some(r#"[{"$undefined":true},{"$undefined":true},{"$undefined":true}]"#.to_string())
        ]
    );
}
