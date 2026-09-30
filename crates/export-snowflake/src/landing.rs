//! The row layout `insert_landing` reads: one per record the loader lands.
//!
//! One field per envelope field, named as the record's JSON fields are,
//! `body`: the record's other fields as a JSON object string, and `source`:
//! where the loader read the record. `kind` is its own column, so routing
//! never parses the body.

use celld_export_format::{DecodeError, Record};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

/// `EXPORT_LANDING`'s record columns, in order.
pub const LANDING_COLUMNS: [&str; 17] = [
    "kind",
    "script",
    "class",
    "cell",
    "cell_name",
    "facet",
    "incarnation",
    "epoch",
    "txid",
    "commit",
    "committed_at",
    "node",
    "origin",
    "fragment",
    "fragments",
    "body",
    "source",
];

/// One record as one `EXPORT_LANDING` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LandingRow {
    pub kind: String,
    pub script: String,
    pub class: String,
    pub cell: String,
    pub cell_name: Option<String>,
    pub facet: Option<String>,
    pub incarnation: u64,
    pub epoch: u64,
    pub txid: u64,
    pub commit: u64,
    /// Milliseconds since the Unix epoch.
    pub committed_at: i64,
    pub node: String,
    pub origin: String,
    pub fragment: u32,
    pub fragments: u32,
    /// The kind-specific fields as a JSON object.
    pub body: String,
    /// Where the record was read, such as `blob-stream/7/1234` (virtual
    /// partition 7, offset 1234). Only for tracing a row back.
    pub source: String,
}

impl LandingRow {
    pub fn from_record(record: &Record, source: impl Into<String>) -> Self {
        let Json::Object(mut fields) =
            serde_json::to_value(record).expect("export records always encode")
        else {
            unreachable!("a record encodes as an object");
        };
        let mut take = |k: &str| fields.remove(k).unwrap_or(Json::Null);
        let string = |v: Json| match v {
            Json::String(s) => s,
            other => unreachable!("envelope field is a string: {other}"),
        };
        let opt_string = |v: Json| match v {
            Json::Null => None,
            Json::String(s) => Some(s),
            other => unreachable!("envelope field is a string: {other}"),
        };
        let row = LandingRow {
            kind: string(take("kind")),
            script: string(take("script")),
            class: string(take("class")),
            cell: string(take("cell")),
            cell_name: opt_string(take("cell_name")),
            facet: opt_string(take("facet")),
            incarnation: record.envelope.stream.incarnation,
            epoch: record.envelope.position.epoch,
            txid: record.envelope.position.txid,
            commit: record.envelope.position.commit,
            committed_at: record.envelope.committed_at,
            node: string(take("node")),
            origin: string(take("origin")),
            fragment: record.envelope.fragment,
            fragments: record.envelope.fragments,
            body: String::new(),
            source: source.into(),
        };
        for k in [
            "incarnation",
            "epoch",
            "txid",
            "commit",
            "committed_at",
            "fragment",
            "fragments",
        ] {
            fields.remove(k);
        }
        LandingRow {
            body: Json::Object(fields).to_string(),
            ..row
        }
    }

    /// The record this row holds.
    pub fn to_record(&self) -> Result<Record, DecodeError> {
        let mut fields: Map<String, Json> = serde_json::from_str(&self.body)?;
        let envelope = serde_json::json!({
            "kind": self.kind,
            "script": self.script,
            "class": self.class,
            "cell": self.cell,
            "cell_name": self.cell_name,
            "facet": self.facet,
            "incarnation": self.incarnation,
            "epoch": self.epoch,
            "txid": self.txid,
            "commit": self.commit,
            "committed_at": self.committed_at,
            "node": self.node,
            "origin": self.origin,
            "fragment": self.fragment,
            "fragments": self.fragments,
        });
        let Json::Object(envelope) = envelope else {
            unreachable!()
        };
        fields.extend(envelope);
        Record::from_json(&serde_json::to_vec(&Json::Object(fields))?)
    }
}
