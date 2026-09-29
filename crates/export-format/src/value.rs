//! SQLite values in export JSON.
//!
//! Integers are JSON integers, reals are JSON numbers, text is a string, a
//! blob is `{"$blob": "<base64>"}`, and `NULL` is `null`. JSON has no
//! infinities, and SQLite stores them, so an infinite real is
//! `{"$real": "inf"}` or `{"$real": "-inf"}`. SQLite stores no NaN: binding
//! one stores `NULL`.

use std::cmp::Ordering;
use std::fmt;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const BLOB_TAG: &str = "$blob";
const REAL_TAG: &str = "$real";

/// One SQLite value.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    fn rank(&self) -> u8 {
        // SQLite's own cross-type order: NULL, numbers, text, blob.
        match self {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
}

// Equality is bitwise for reals so that `Value` can key a map: the consumer
// compares row keys and images, and a key must equal itself.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::mem::discriminant(self).hash(h);
        match self {
            Value::Null => {}
            Value::Integer(i) => i.hash(h),
            // `total_cmp` is equal exactly when the bits are.
            Value::Real(r) => r.to_bits().hash(h),
            Value::Text(t) => t.hash(h),
            Value::Blob(b) => b.hash(h),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            // An integer and a real are different values even when they are
            // numerically equal; the integer sorts first at a tie.
            (Value::Integer(a), Value::Real(b)) => (*a as f64).total_cmp(b).then(Ordering::Less),
            (Value::Real(a), Value::Integer(b)) => {
                a.total_cmp(&(*b as f64)).then(Ordering::Greater)
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => s.serialize_unit(),
            Value::Integer(i) => s.serialize_i64(*i),
            Value::Real(r) if r.is_finite() => s.serialize_f64(*r),
            Value::Real(r) if r.is_infinite() => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry(REAL_TAG, if *r > 0.0 { "inf" } else { "-inf" })?;
                m.end()
            }
            // NaN cannot come out of SQLite; encode it as SQLite stores it.
            Value::Real(_) => s.serialize_unit(),
            Value::Text(t) => s.serialize_str(t),
            Value::Blob(b) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry(BLOB_TAG, &STANDARD.encode(b))?;
                m.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an export value: integer, number, string, null, or {\"$blob\": …}")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Integer(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        i64::try_from(v)
            .map(Value::Integer)
            .map_err(|_| E::custom("integer out of SQLite range"))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Value::Real(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Text(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::Text(v))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let Some(tag) = map.next_key::<String>()? else {
            return Err(de::Error::custom("empty object is not an export value"));
        };
        let body: String = map.next_value()?;
        if map.next_key::<String>()?.is_some() {
            return Err(de::Error::custom("tagged export value has extra fields"));
        }
        match tag.as_str() {
            BLOB_TAG => STANDARD
                .decode(body)
                .map(Value::Blob)
                .map_err(|e| de::Error::custom(format!("bad $blob base64: {e}"))),
            REAL_TAG => match body.as_str() {
                "inf" => Ok(Value::Real(f64::INFINITY)),
                "-inf" => Ok(Value::Real(f64::NEG_INFINITY)),
                _ => Err(de::Error::custom("bad $real")),
            },
            _ => Err(de::Error::custom(format!("unknown export value tag {tag}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round(v: &Value) -> Value {
        serde_json::from_str(&serde_json::to_string(v).unwrap()).unwrap()
    }

    #[test]
    fn encodings_follow_the_design() {
        let cases = [
            (Value::Null, "null"),
            (Value::Integer(-7), "-7"),
            (Value::Integer(i64::MAX), "9223372036854775807"),
            (Value::Real(2.0), "2.0"),
            (Value::Real(0.5), "0.5"),
            (Value::Text("a\"b".into()), r#""a\"b""#),
            (Value::Blob(vec![0, 1, 255]), r#"{"$blob":"AAH/"}"#),
            (Value::Real(f64::INFINITY), r#"{"$real":"inf"}"#),
        ];
        for (v, json) in cases {
            assert_eq!(serde_json::to_string(&v).unwrap(), json);
            assert_eq!(round(&v), v);
        }
    }

    #[test]
    fn an_integral_real_stays_a_real() {
        assert!(matches!(round(&Value::Real(3.0)), Value::Real(_)));
        assert!(matches!(round(&Value::Real(-0.0)), Value::Real(r) if r.is_sign_negative()));
        assert!(matches!(round(&Value::Real(1e300)), Value::Real(_)));
    }

    #[test]
    fn integer_and_real_are_distinct_keys() {
        assert_ne!(Value::Integer(1), Value::Real(1.0));
        assert!(Value::Integer(1) < Value::Real(1.0));
        assert!(Value::Real(0.5) < Value::Integer(1));
    }

    #[test]
    fn rejects_unknown_tags() {
        assert!(serde_json::from_str::<Value>(r#"{"$x":"y"}"#).is_err());
        assert!(serde_json::from_str::<Value>(r#"{"$blob":"AA","k":"v"}"#).is_err());
        assert!(serde_json::from_str::<Value>("true").is_err());
    }
}
