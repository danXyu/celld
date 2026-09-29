//! The change-export record format and its reference consumer.
//!
//! See `docs/design/change-export.md`, sections Identity and Records. A
//! record is one JSON object: the envelope every record carries ([`Envelope`])
//! flattened together with a body selected by `kind` ([`Body`]). Row values
//! use the encoding in [`value`]. [`fragment`] splits a record under the
//! record size limit and reassembles it; [`dedup`] holds the keys a consumer
//! drops duplicates by; [`consumer`] applies records the way every consumer
//! must, and is the oracle the exporter's tests compare against.
//!
//! Where the design leaves a detail open, this crate fixes it:
//!
//! - an infinite real is `{"$real": "inf"}` or `{"$real": "-inf"}`, since
//!   JSON has no infinities;
//! - a table without a declared primary key has `key_columns` of
//!   [`ROWID_KEY_COLUMN`], and its key is the rowid;
//! - `snapshot_end` carries `scope` (`stream` or `tables`), the table
//!   generations it covered, and the number of `snapshot` records it closes;
//! - a `watermark` carries `from` and `through` positions with its counts,
//!   and a watermark with no `from` counts from the start of its epoch;
//! - a `deleted` record naming a facet carries `target_facet`,
//!   `target_incarnation`, and `subtree`, since it rides the root's stream;
//!   a node's facet delete carries `through_incarnation`, a bound on the
//!   ordered incarnations it removed, in place of `target_incarnation`.
//!
//! Pure: no I/O, clocks, or randomness.

pub mod consumer;
pub mod dedup;
pub mod fragment;
pub mod record;
pub mod value;

pub use consumer::{Consumer, Gap, StreamState, TableState};
pub use dedup::{row_keys, DedupKey, RecordKey, RowDedupKey};
pub use fragment::{split, ReassembleError, Reassembler, Split};
pub use record::*;
pub use value::Value;
