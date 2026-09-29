//! The row layout of a stage file: what `COPY INTO EXPORT_LANDING` reads.
//!
//! The bucket sink writes Parquet with one column per envelope field, named
//! as the record's JSON fields are, and `body`: the record's other fields as
//! a JSON object string. `kind` is its own column, so a reader can route a
//! record without parsing the body.

use celld_export_format::{DecodeError, Record};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

/// The stage file's columns, in order.
pub const STAGE_COLUMNS: [&str; 16] = [
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
];

/// One record as one stage-file row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageRow {
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
}

impl StageRow {
    pub fn from_record(record: &Record) -> Self {
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
        let row = StageRow {
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
        StageRow {
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
