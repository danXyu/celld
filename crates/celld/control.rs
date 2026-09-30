// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Where a fleet keeps its coordination records.
//!
//! A fleet coordinates through a handful of small records that every node
//! compare-and-swaps: the cell ownership records, the node leases (which
//! carry the folded node log), the drain token, the waker role, the deploy
//! pointers and the queue attachments. By default they live in the fleet
//! bucket beside the data they govern. A fleet can instead keep them in one
//! Amazon DynamoDB table, selected once for the whole fleet by the marker
//! `fleet/control.json` in the bucket (see
//! `docs/design/dynamodb-control-plane.md`).
//!
//! The records keep their bucket keys and their JSON bodies in both homes.
//! [`crate::bucket::Bucket`] routes a read, write, delete or listing of one
//! of those keys to the table when the fleet selected it, so no caller
//! builds a table request itself and no caller can forget to: a call site
//! that addressed `nodes/<node>.json` on the bucket addresses the same
//! record on the table. A bucket fleet never takes that branch, so it
//! issues exactly the requests it issued before this module existed.
//!
//! The table has to give the three properties the bucket gives: a
//! conditional create, a conditional overwrite on an opaque token, and
//! read-after-write. Every read is strongly consistent, every conditional
//! write is attempted once, and every failure is classified as either
//! "not committed" or "may have committed", exactly as `put_cas` classifies
//! a bucket failure.

use crate::bucket::Bucket;
use anyhow::{anyhow, bail, ensure, Context};
use bytes::Bytes;
use object_store::aws::{AwsAuthorizer, AwsCredentialProvider};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path;
use object_store::ObjectMeta;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[cfg(test)]
mod tests;

/// The marker that records which store holds this fleet's coordination
/// records. It lives in the bucket, because the bucket is the one thing
/// every node and every operator command already agrees on.
pub const MARKER_KEY: &str = "fleet/control.json";

const MARKER_FORMAT: u8 = 1;

/// The table's own identity item: which fleet it serves.
const META_PK: &str = "meta";
const META_SK: &str = "fleet";

/// The partitions that hold records a listing can return.
const NODES_PK: &str = "nodes";
/// The partition that holds the node leases.
pub(crate) const NODES_PARTITION: &str = NODES_PK;
const FLEET_PK: &str = "fleet";
const DEPLOY_PK: &str = "deploy";
const PROBE_PK: &str = "probe";

/// The DynamoDB JSON protocol version every request names.
const TARGET_PREFIX: &str = "DynamoDB_20120810";

/// Extra attempts for a read. A read changes nothing, so repeating one is
/// always safe. A write is never repeated inside this module: a repeat of a
/// conditional write that already applied answers as a lost race.
const READ_RETRIES: usize = 2;

// ── Configuration ──────────────────────────────────────────────────────────

/// Which store a process was told to use, from `CELLD_CONTROL`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Backend {
    Bucket,
    DynamoDb { table: String },
}

impl Backend {
    /// Parse `bucket` or `dynamodb://TABLE`.
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if value == "bucket" {
            return Ok(Self::Bucket);
        }
        let Some(table) = value.strip_prefix("dynamodb://") else {
            bail!("CELLD_CONTROL must be bucket or dynamodb://TABLE, not {value:?}");
        };
        validate_table_name(table)?;
        Ok(Self::DynamoDb {
            table: table.to_string(),
        })
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Bucket => "bucket",
            Self::DynamoDb { .. } => "dynamodb",
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bucket => f.write_str("bucket"),
            Self::DynamoDb { table } => write!(f, "dynamodb://{table}"),
        }
    }
}

/// DynamoDB's own rule for a table name.
fn validate_table_name(table: &str) -> anyhow::Result<()> {
    ensure!(
        (3..=255).contains(&table.len())
            && table
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c)),
        "a DynamoDB table name is 3 to 255 letters, digits, and _.-, not {table:?}"
    );
    Ok(())
}

/// Everything the environment says about the coordination store.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    /// `CELLD_CONTROL`. `None` when unset, which follows the marker, and
    /// selects the bucket for a fleet that has no marker yet.
    pub backend: Option<Backend>,
    /// `CELLD_CONTROL_REGION`: the table's region when the marker does not
    /// name one.
    pub region: Option<String>,
    /// `CELLD_CONTROL_ENDPOINT`: a DynamoDB endpoint override, for DynamoDB
    /// Local. It is per process and never recorded in the marker.
    pub endpoint: Option<String>,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        let non_empty = |name: &str| -> anyhow::Result<Option<String>> {
            Ok(crate::env_vars::value(name)?.filter(|value| !value.trim().is_empty()))
        };
        Ok(Self {
            backend: non_empty("CELLD_CONTROL")?
                .map(|value| Backend::parse(&value))
                .transpose()?,
            region: non_empty("CELLD_CONTROL_REGION")?,
            endpoint: non_empty("CELLD_CONTROL_ENDPOINT")?,
        })
    }
}

/// Validate the `CELLD_CONTROL*` group before the runtime starts.
pub fn validate_env() -> anyhow::Result<()> {
    let settings = Settings::from_env()?;
    if let Some(endpoint) = &settings.endpoint {
        reqwest::Url::parse(endpoint)
            .with_context(|| format!("CELLD_CONTROL_ENDPOINT is not a URL: {endpoint:?}"))?;
    }
    Ok(())
}

/// Whether this process was configured for a table, read once. A bucket that
/// was never resolved answers as a bucket fleet unless this is set, and then
/// it refuses instead: a node configured for the table must never fall back
/// to the bucket's copy of a record because one code path skipped
/// resolution.
fn configured_for_table() -> bool {
    static CONFIGURED: OnceLock<bool> = OnceLock::new();
    *CONFIGURED.get_or_init(|| {
        matches!(
            Settings::from_env().map(|settings| settings.backend),
            Ok(Some(Backend::DynamoDb { .. }))
        )
    })
}

// ── Records ────────────────────────────────────────────────────────────────

/// One coordination record, named by the bucket key it has always had.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ControlKey {
    /// `cells/<cell>/own.json`
    Owner(String),
    /// `nodes/<node>.json`
    Lease(String),
    /// `drain/token.json`
    Drain,
    /// `wake/waker.json`
    Waker,
    /// `deploy/current.json`
    FleetPointer,
    /// `deploy/<script>/current.json`
    ScriptPointer(String),
    /// `deploy/queues/<queue>/consumer.json`
    QueueAttachment(String),
}

impl ControlKey {
    /// The record a bucket key names, or `None` for every other key.
    pub(crate) fn parse(key: &str) -> Option<Self> {
        match key {
            "drain/token.json" => return Some(Self::Drain),
            "wake/waker.json" => return Some(Self::Waker),
            "deploy/current.json" => return Some(Self::FleetPointer),
            _ => {}
        }
        if let Some(cell) = key
            .strip_prefix("cells/")
            .and_then(|rest| rest.strip_suffix("/own.json"))
        {
            return (!cell.is_empty()).then(|| Self::Owner(cell.to_string()));
        }
        if let Some(node) = key
            .strip_prefix("nodes/")
            .and_then(|rest| rest.strip_suffix(".json"))
        {
            return (!node.is_empty() && !node.contains('/'))
                .then(|| Self::Lease(node.to_string()));
        }
        if let Some(queue) = key
            .strip_prefix("deploy/queues/")
            .and_then(|rest| rest.strip_suffix("/consumer.json"))
        {
            return (!queue.is_empty() && !queue.contains('/'))
                .then(|| Self::QueueAttachment(queue.to_string()));
        }
        if let Some(script) = key
            .strip_prefix("deploy/")
            .and_then(|rest| rest.strip_suffix("/current.json"))
        {
            return (!script.is_empty() && !script.contains('/'))
                .then(|| Self::ScriptPointer(script.to_string()));
        }
        None
    }

    /// The table's partition and sort key for this record.
    fn item_key(&self) -> (String, String) {
        match self {
            Self::Owner(cell) => (format!("cell#{cell}"), "own".to_string()),
            Self::Lease(node) => (NODES_PK.to_string(), node.clone()),
            Self::Drain => (FLEET_PK.to_string(), "drain".to_string()),
            Self::Waker => (FLEET_PK.to_string(), "waker".to_string()),
            Self::FleetPointer => (DEPLOY_PK.to_string(), "current".to_string()),
            Self::ScriptPointer(script) => (DEPLOY_PK.to_string(), format!("script#{script}")),
            Self::QueueAttachment(queue) => (DEPLOY_PK.to_string(), format!("queue#{queue}")),
        }
    }

    /// The bucket key of a listed item, the inverse of [`Self::item_key`]
    /// for the partitions a listing reads.
    fn object_key(pk: &str, sk: &str) -> Option<String> {
        match (pk, sk) {
            (NODES_PK, node) => Some(format!("nodes/{node}.json")),
            (FLEET_PK, "drain") => Some("drain/token.json".to_string()),
            (FLEET_PK, "waker") => Some("wake/waker.json".to_string()),
            (DEPLOY_PK, "current") => Some("deploy/current.json".to_string()),
            (DEPLOY_PK, sk) => {
                if let Some(script) = sk.strip_prefix("script#") {
                    Some(format!("deploy/{script}/current.json"))
                } else {
                    sk.strip_prefix("queue#")
                        .map(|queue| format!("deploy/queues/{queue}/consumer.json"))
                }
            }
            _ => None,
        }
    }
}

/// How a bucket listing of `prefix` meets the table.
pub(crate) struct ListingPlan {
    /// The table partitions whose records fall under the prefix.
    pub(crate) partitions: Vec<&'static str>,
    /// The prefix holds nothing but table records, so the bucket's own
    /// listing is skipped.
    pub(crate) table_only: bool,
}

/// The listing `prefix` needs when the table holds the records. A prefix is
/// matched the way `Bucket::list` matches it: as whole path segments.
pub(crate) fn listing_plan(prefix: &str) -> ListingPlan {
    let prefix = prefix.trim_end_matches('/');
    let covers =
        |root: &str| prefix.is_empty() || root == prefix || root.starts_with(&format!("{prefix}/"));
    let inside = |root: &str| prefix == root || prefix.starts_with(&format!("{root}/"));
    let mut partitions = Vec::new();
    if covers("nodes") || inside("nodes") {
        partitions.push(NODES_PK);
    }
    if covers("drain") || inside("drain") || covers("wake") || prefix == "wake/waker.json" {
        partitions.push(FLEET_PK);
    }
    if covers("deploy") || inside("deploy") {
        partitions.push(DEPLOY_PK);
    }
    ListingPlan {
        partitions,
        table_only: inside("nodes") || inside("drain"),
    }
}

/// Does a listed bucket key fall under a listing prefix?
pub(crate) fn under_prefix(key: &str, prefix: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    prefix.is_empty() || key.starts_with(&format!("{prefix}/"))
}

// ── Errors ─────────────────────────────────────────────────────────────────

/// Whether a failed table request can have changed the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Commit {
    /// The request was refused before any item changed: a throttle, a
    /// rejected credential, a malformed request, or a connection that never
    /// opened.
    No,
    /// The request can have been applied: a timeout, a reset connection, or
    /// a server error.
    Maybe,
}

/// A table request that failed, classified for the self-fence.
#[derive(Debug)]
pub struct TableError {
    pub commit: Commit,
    /// DynamoDB's error type, without its namespace.
    pub code: Option<String>,
    pub message: String,
}

impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.code {
            Some(code) => write!(f, "dynamodb {code}: {}", self.message),
            None => write!(f, "dynamodb: {}", self.message),
        }
    }
}

impl std::error::Error for TableError {}

impl TableError {
    fn not_committed(message: impl Into<String>) -> Self {
        Self {
            commit: Commit::No,
            code: None,
            message: message.into(),
        }
    }

    fn maybe_committed(message: impl Into<String>) -> Self {
        Self {
            commit: Commit::Maybe,
            code: None,
            message: message.into(),
        }
    }

    /// Classify an error response.
    ///
    /// DynamoDB applies a request only after it authenticates, validates and
    /// admits it, so a 4xx answer means nothing changed. That covers
    /// throttling too: a throttled write is rejected before it applies,
    /// which lets a throttled lease renewal retry with the token it holds.
    /// A 5xx answer, including `InternalServerError`, is documented as a
    /// request that may have succeeded.
    fn from_response(status: u16, body: &[u8]) -> Self {
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let code = parsed
            .get("__type")
            .and_then(Value::as_str)
            .map(|kind| kind.rsplit('#').next().unwrap_or(kind).to_string());
        let message = parsed
            .get("message")
            .or_else(|| parsed.get("Message"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("HTTP {status}"));
        Self {
            commit: if (400..500).contains(&status) {
                Commit::No
            } else {
                Commit::Maybe
            },
            code,
            message,
        }
    }

    fn is_condition_failure(&self) -> bool {
        self.code.as_deref() == Some("ConditionalCheckFailedException")
    }
}

/// Did a failed write provably leave the table unchanged? `Bucket`'s
/// classifier consults this for a routed record.
pub(crate) fn table_write_did_not_commit(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<TableError>()
        .is_some_and(|error| error.commit == Commit::No)
}

// ── Transport ──────────────────────────────────────────────────────────────

/// One DynamoDB operation: `op` is the action name, `body` its request.
#[async_trait::async_trait]
pub(crate) trait Transport: Send + Sync {
    async fn call(&self, op: &'static str, body: Value) -> Result<Value, TableError>;
}

/// The DynamoDB JSON protocol over HTTPS, signed with SigV4.
///
/// The signer and the credential chain are `object_store`'s, the same ones
/// the fleet's S3 client uses, so the table authenticates exactly as the
/// bucket does and celld links no AWS SDK.
struct HttpTransport {
    client: reqwest::Client,
    url: String,
    region: String,
    credentials: AwsCredentialProvider,
}

impl HttpTransport {
    fn new(
        url: String,
        region: String,
        credentials: AwsCredentialProvider,
        app: Option<&str>,
    ) -> anyhow::Result<Self> {
        // The bucket's own bounds, and for the same reason: they are part of
        // the self-fence arithmetic, not tuning.
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(3));
        if let Some(app) = app {
            builder = builder.user_agent(format!("celld app/{app}"));
        }
        Ok(Self {
            client: builder.build().context("build the DynamoDB client")?,
            url,
            region,
            credentials,
        })
    }
}

#[async_trait::async_trait]
impl Transport for HttpTransport {
    async fn call(&self, op: &'static str, body: Value) -> Result<Value, TableError> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|error| TableError::not_committed(format!("AWS credentials: {error}")))?;
        let payload = serde_json::to_vec(&body)
            .map_err(|error| TableError::not_committed(format!("encode {op}: {error}")))?;
        let mut request = HttpRequest::new(HttpRequestBody::from(payload.clone()));
        *request.method_mut() = hyper::Method::POST;
        *request.uri_mut() = self.url.parse().map_err(|error| {
            TableError::not_committed(format!("endpoint {}: {error}", self.url))
        })?;
        let headers = request.headers_mut();
        headers.insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            hyper::header::HeaderValue::from_str(&format!("{TARGET_PREFIX}.{op}"))
                .expect("an operation name is a valid header value"),
        );
        AwsAuthorizer::new(&credential, "dynamodb", &self.region).authorize(&mut request, None);

        let response = self
            .client
            .post(&self.url)
            .headers(request.headers().clone())
            .body(payload)
            .send()
            .await
            .map_err(|error| {
                // A connection that never opened sent nothing. Everything
                // else, a timeout above all, can have reached the table.
                let message = format!("{op}: {error}");
                if error.is_connect() {
                    TableError::not_committed(message)
                } else {
                    TableError::maybe_committed(message)
                }
            })?;
        let status = response.status().as_u16();
        let bytes = response.bytes().await.map_err(|error| {
            TableError::maybe_committed(format!("{op}: read the response: {error}"))
        })?;
        if !(200..300).contains(&status) {
            return Err(TableError::from_response(status, &bytes));
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            // The table answered success, so the request applied; only its
            // answer is unreadable.
            TableError::maybe_committed(format!("{op}: decode the response: {error}"))
        })
    }
}

// ── The table ──────────────────────────────────────────────────────────────

/// A record as the table holds it.
#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub(crate) body: Bytes,
    pub(crate) token: String,
    pub(crate) updated_ms: i64,
}

/// A listed record, with the bucket key it answers to.
pub(crate) struct Listed {
    pub(crate) key: String,
    pub(crate) record: Record,
}

/// The precondition of a table write.
enum Condition<'a> {
    None,
    Absent,
    Token(&'a str),
}

/// A fleet's coordination table. Cheap to share; each instance owns its own
/// HTTP connection pool, so the lease lane's instance never queues behind
/// ordinary traffic.
pub struct Table {
    name: String,
    region: String,
    transport: Arc<dyn Transport>,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("region", &self.region)
            .finish()
    }
}

impl Table {
    pub(crate) fn with_transport(
        name: String,
        region: String,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            name,
            region,
            transport,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// A fresh write token. The writer generates it, so after an ambiguous
    /// write a read that returns this exact token proves the write applied.
    fn new_token() -> String {
        let mut rng = crate::asyncrt::rng("control_table_token");
        format!("{:016x}{:016x}", rng.next_u64(), rng.next_u64())
    }

    fn key_attributes(pk: &str, sk: &str) -> Value {
        json!({ "pk": { "S": pk }, "sk": { "S": sk } })
    }

    async fn read(&self, op: &'static str, body: Value) -> Result<Value, TableError> {
        let mut attempt = 0;
        loop {
            match self.transport.call(op, body.clone()).await {
                Ok(answer) => return Ok(answer),
                Err(error) if attempt < READ_RETRIES => {
                    attempt += 1;
                    tracing::debug!(op, %error, attempt, "retrying a DynamoDB read");
                    crate::asyncrt::sleep(Duration::from_millis(25 << attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get(&self, pk: &str, sk: &str) -> anyhow::Result<Option<Record>> {
        let answer = self
            .read(
                "GetItem",
                json!({
                    "TableName": self.name,
                    "Key": Self::key_attributes(pk, sk),
                    "ConsistentRead": true,
                }),
            )
            .await
            .with_context(|| format!("read {pk}/{sk} from dynamodb://{}", self.name))?;
        match answer.get("Item") {
            None | Some(Value::Null) => Ok(None),
            Some(item) => decode_record(item).map(Some),
        }
    }

    /// Write `body`. `Ok(Some(token))` applied, `Ok(None)` the condition
    /// failed; an `Err` carries a [`TableError`] that says whether the write
    /// can have applied.
    async fn put(
        &self,
        pk: &str,
        sk: &str,
        body: &[u8],
        condition: Condition<'_>,
    ) -> anyhow::Result<Option<String>> {
        let doc = std::str::from_utf8(body).map_err(|_| {
            anyhow!(TableError::not_committed(format!(
                "{pk}/{sk}: a coordination record must be UTF-8 JSON"
            )))
        })?;
        let token = Self::new_token();
        let mut request = json!({
            "TableName": self.name,
            "Item": {
                "pk": { "S": pk },
                "sk": { "S": sk },
                "doc": { "S": doc },
                "v": { "S": token },
                "updated_ms": { "N": crate::asyncrt::wall_ms().max(0).to_string() },
            },
        });
        match condition {
            Condition::None => {}
            Condition::Absent => {
                request["ConditionExpression"] = json!("attribute_not_exists(pk)");
            }
            Condition::Token(expected) => {
                request["ConditionExpression"] = json!("v = :v");
                request["ExpressionAttributeValues"] = json!({ ":v": { "S": expected } });
            }
        }
        match self.transport.call("PutItem", request).await {
            Ok(_) => Ok(Some(token)),
            Err(error) if error.is_condition_failure() => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("write {pk}/{sk} to dynamodb://{}", self.name)))
            }
        }
    }

    /// Delete an item, only while it still holds `token` when one is given.
    /// `Ok(false)` means the item changed since the caller read it.
    async fn delete(&self, pk: &str, sk: &str, token: Option<&str>) -> anyhow::Result<bool> {
        let mut request = json!({
            "TableName": self.name,
            "Key": Self::key_attributes(pk, sk),
        });
        if let Some(token) = token {
            request["ConditionExpression"] = json!("v = :v");
            request["ExpressionAttributeValues"] = json!({ ":v": { "S": token } });
        }
        match self.transport.call("DeleteItem", request).await {
            Ok(_) => Ok(true),
            Err(error) if error.is_condition_failure() => Ok(false),
            Err(error) => {
                Err(anyhow!(error)
                    .context(format!("delete {pk}/{sk} from dynamodb://{}", self.name)))
            }
        }
    }

    /// One page of a partition in sort-key order, after `after` when given.
    async fn query_page(
        &self,
        pk: &str,
        after: Option<&str>,
        limit: Option<usize>,
    ) -> anyhow::Result<(Vec<(String, Record)>, Option<String>)> {
        let mut request = json!({
            "TableName": self.name,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": { ":pk": { "S": pk } },
            "ConsistentRead": true,
        });
        if let Some(after) = after {
            request["ExclusiveStartKey"] = Self::key_attributes(pk, after);
        }
        if let Some(limit) = limit {
            request["Limit"] = json!(limit.max(1));
        }
        let answer = self
            .read("Query", request)
            .await
            .with_context(|| format!("list {pk} in dynamodb://{}", self.name))?;
        let items = answer
            .get("Items")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut records = Vec::with_capacity(items.len());
        for item in items {
            let sk = string_attribute(item, "sk")?;
            records.push((sk, decode_record(item)?));
        }
        let next = answer
            .get("LastEvaluatedKey")
            .filter(|key| !key.is_null())
            .map(|key| string_attribute(key, "sk"))
            .transpose()?;
        Ok((records, next))
    }

    /// Every item of a partition.
    async fn query(&self, pk: &str) -> anyhow::Result<Vec<(String, Record)>> {
        let mut records = Vec::new();
        let mut after = None;
        loop {
            let (page, next) = self.query_page(pk, after.as_deref(), None).await?;
            records.extend(page);
            match next {
                Some(next) => after = Some(next),
                None => return Ok(records),
            }
        }
    }

    // ── The routed record surface, in bucket terms ──

    pub(crate) async fn get_record(
        &self,
        key: &ControlKey,
    ) -> anyhow::Result<Option<(Bytes, String)>> {
        let (pk, sk) = key.item_key();
        Ok(self
            .get(&pk, &sk)
            .await?
            .map(|record| (record.body, record.token)))
    }

    pub(crate) async fn head_record(
        &self,
        key: &ControlKey,
    ) -> anyhow::Result<Option<(u64, String)>> {
        let (pk, sk) = key.item_key();
        Ok(self
            .get(&pk, &sk)
            .await?
            .map(|record| (record.body.len() as u64, record.token)))
    }

    pub(crate) async fn put_record(&self, key: &ControlKey, body: &[u8]) -> anyhow::Result<()> {
        let (pk, sk) = key.item_key();
        self.put(&pk, &sk, body, Condition::None).await?;
        Ok(())
    }

    pub(crate) async fn cas_record(
        &self,
        key: &ControlKey,
        body: &[u8],
        token: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let (pk, sk) = key.item_key();
        let condition = match token {
            None => Condition::Absent,
            Some(token) => Condition::Token(token),
        };
        self.put(&pk, &sk, body, condition).await
    }

    pub(crate) async fn delete_record(
        &self,
        key: &ControlKey,
        token: Option<&str>,
    ) -> anyhow::Result<bool> {
        let (pk, sk) = key.item_key();
        self.delete(&pk, &sk, token).await
    }

    /// Every record the plan's partitions hold under `prefix`, as bucket
    /// keys, in key order.
    pub(crate) async fn list_records(
        &self,
        plan: &ListingPlan,
        prefix: &str,
    ) -> anyhow::Result<Vec<Listed>> {
        let mut listed = Vec::new();
        for pk in &plan.partitions {
            for (sk, record) in self.query(pk).await? {
                let Some(key) = ControlKey::object_key(pk, &sk) else {
                    continue;
                };
                if under_prefix(&key, prefix) {
                    listed.push(Listed { key, record });
                }
            }
        }
        listed.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(listed)
    }

    /// One bounded page of the node leases, resumable with the returned
    /// cursor. The cursor is the last node name, which is exact because the
    /// partition is ordered by it.
    pub(crate) async fn lease_page(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> anyhow::Result<(Vec<Listed>, Option<String>)> {
        let (records, next) = self
            .query_page(NODES_PK, cursor.as_deref(), Some(limit))
            .await?;
        Ok((
            records
                .into_iter()
                .map(|(node, record)| Listed {
                    key: format!("nodes/{node}.json"),
                    record,
                })
                .collect(),
            next,
        ))
    }

    // ── Administration ──

    /// The fleet this table serves, or `None` for a table no fleet claimed.
    async fn fleet(&self) -> anyhow::Result<Option<String>> {
        let Some(record) = self.get(META_PK, META_SK).await? else {
            return Ok(None);
        };
        let meta: Meta = serde_json::from_slice(&record.body)
            .with_context(|| format!("decode the meta item of dynamodb://{}", self.name))?;
        Ok(Some(meta.fleet))
    }

    /// Claim the table for `fleet`, or confirm it already serves it.
    async fn claim(&self, fleet: &str) -> anyhow::Result<()> {
        let body = serde_json::to_vec(&Meta {
            format: MARKER_FORMAT,
            fleet: fleet.to_string(),
        })?;
        if self
            .put(META_PK, META_SK, &body, Condition::Absent)
            .await?
            .is_some()
        {
            return Ok(());
        }
        match self.fleet().await? {
            Some(owner) if owner == fleet => Ok(()),
            Some(owner) => bail!(
                "dynamodb://{} serves fleet {owner}, not this bucket's fleet {fleet}",
                self.name
            ),
            None => bail!(
                "dynamodb://{} lost its meta item during the claim",
                self.name
            ),
        }
    }

    /// Check that the table is shaped the way the guarantees need.
    ///
    /// A global table replicates asynchronously, a secondary index answers
    /// eventually consistent reads, and time-to-live deletes items on its
    /// own schedule; each of them can hand a node a record that another
    /// node has already replaced, or remove an authority record outright.
    pub(crate) async fn check_shape(&self) -> anyhow::Result<()> {
        let described = self
            .read("DescribeTable", json!({ "TableName": self.name }))
            .await
            .with_context(|| format!("describe dynamodb://{}", self.name))?;
        let table = described
            .get("Table")
            .context("DescribeTable answered without a table")?;
        let status = table.get("TableStatus").and_then(Value::as_str);
        ensure!(
            matches!(status, Some("ACTIVE") | Some("UPDATING")),
            "dynamodb://{} is {}, not ACTIVE",
            self.name,
            status.unwrap_or("in an unknown state")
        );
        let schema = table
            .get("KeySchema")
            .and_then(Value::as_array)
            .map(|schema| {
                schema
                    .iter()
                    .map(|key| {
                        (
                            key.get("AttributeName")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            key.get("KeyType").and_then(Value::as_str).unwrap_or(""),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        ensure!(
            schema.contains(&("pk", "HASH"))
                && schema.contains(&("sk", "RANGE"))
                && schema.len() == 2,
            "dynamodb://{} must have partition key pk and sort key sk",
            self.name
        );
        let types: Vec<(&str, &str)> = table
            .get("AttributeDefinitions")
            .and_then(Value::as_array)
            .map(|definitions| {
                definitions
                    .iter()
                    .map(|definition| {
                        (
                            definition
                                .get("AttributeName")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            definition
                                .get("AttributeType")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        ensure!(
            types.contains(&("pk", "S")) && types.contains(&("sk", "S")),
            "dynamodb://{} keys must be strings",
            self.name
        );
        for field in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"] {
            ensure!(
                table
                    .get(field)
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty),
                "dynamodb://{} has {field}; a secondary index reads eventually, so celld refuses it",
                self.name
            );
        }
        ensure!(
            table
                .get("Replicas")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty),
            "dynamodb://{} is a global table; its replicas are eventually consistent, so \
             celld refuses it",
            self.name
        );
        let ttl = self
            .read("DescribeTimeToLive", json!({ "TableName": self.name }))
            .await
            .with_context(|| format!("describe the time-to-live of dynamodb://{}", self.name))?;
        let ttl_status = ttl
            .pointer("/TimeToLiveDescription/TimeToLiveStatus")
            .and_then(Value::as_str)
            .unwrap_or("DISABLED");
        ensure!(
            ttl_status == "DISABLED",
            "dynamodb://{} has time-to-live {ttl_status}; it can delete an authority record, \
             so celld refuses it",
            self.name
        );
        Ok(())
    }

    /// Provoke the two rejections a conforming table must produce, as the
    /// bucket probe does, against a probe item nothing else reads.
    pub(crate) async fn probe(&self) -> anyhow::Result<()> {
        let sk = Self::new_token();
        let result = self.probe_steps(&sk).await;
        if let Err(error) = self.delete(PROBE_PK, &sk, None).await {
            tracing::warn!(%error, "the table probe could not delete its item");
        }
        result
    }

    async fn probe_steps(&self, sk: &str) -> anyhow::Result<()> {
        let first = self
            .put(PROBE_PK, sk, b"{\"probe\":1}", Condition::Absent)
            .await?
            .context("the table refused to create an absent probe item")?;
        ensure!(
            self.put(PROBE_PK, sk, b"{\"probe\":2}", Condition::Absent)
                .await?
                .is_none(),
            "the table created an item that already existed"
        );
        let second = self
            .put(PROBE_PK, sk, b"{\"probe\":3}", Condition::Token(&first))
            .await?
            .context("the table refused an update with the current token")?;
        ensure!(
            self.put(PROBE_PK, sk, b"{\"probe\":4}", Condition::Token(&first))
                .await?
                .is_none(),
            "the table applied an update with a stale token"
        );
        let read = self
            .get(PROBE_PK, sk)
            .await?
            .context("the table lost the probe item")?;
        ensure!(
            read.token == second && read.body.as_ref() == b"{\"probe\":3}",
            "the table did not read back its own last write"
        );
        Ok(())
    }

    /// Create the table: on-demand capacity, deletion protection on, then
    /// point-in-time recovery once it is active.
    pub(crate) async fn create(&self) -> anyhow::Result<bool> {
        let created = match self
            .transport
            .call(
                "CreateTable",
                json!({
                    "TableName": self.name,
                    "AttributeDefinitions": [
                        { "AttributeName": "pk", "AttributeType": "S" },
                        { "AttributeName": "sk", "AttributeType": "S" },
                    ],
                    "KeySchema": [
                        { "AttributeName": "pk", "KeyType": "HASH" },
                        { "AttributeName": "sk", "KeyType": "RANGE" },
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                    "DeletionProtectionEnabled": true,
                }),
            )
            .await
        {
            Ok(_) => true,
            Err(error) if error.code.as_deref() == Some("ResourceInUseException") => false,
            Err(error) => {
                return Err(anyhow!(error).context(format!("create dynamodb://{}", self.name)))
            }
        };
        for _ in 0..120 {
            let described = self
                .read("DescribeTable", json!({ "TableName": self.name }))
                .await?;
            if described
                .pointer("/Table/TableStatus")
                .and_then(Value::as_str)
                == Some("ACTIVE")
            {
                break;
            }
            crate::asyncrt::sleep(Duration::from_secs(1)).await;
        }
        if let Err(error) = self
            .transport
            .call(
                "UpdateContinuousBackups",
                json!({
                    "TableName": self.name,
                    "PointInTimeRecoverySpecification": { "PointInTimeRecoveryEnabled": true },
                }),
            )
            .await
        {
            tracing::warn!(table = %self.name, %error, "could not enable point-in-time recovery");
        }
        Ok(created)
    }

    /// Whether point-in-time recovery is on, for diagnostics.
    pub(crate) async fn point_in_time_recovery(&self) -> anyhow::Result<bool> {
        let answer = self
            .read(
                "DescribeContinuousBackups",
                json!({ "TableName": self.name }),
            )
            .await?;
        Ok(answer
            .pointer("/ContinuousBackupsDescription/PointInTimeRecoveryDescription/PointInTimeRecoveryStatus")
            .and_then(Value::as_str)
            == Some("ENABLED"))
    }
}

#[derive(Serialize, Deserialize)]
struct Meta {
    format: u8,
    fleet: String,
}

fn string_attribute(item: &Value, name: &str) -> anyhow::Result<String> {
    item.get(name)
        .and_then(|value| value.get("S"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("a table item has no string attribute {name}"))
}

fn decode_record(item: &Value) -> anyhow::Result<Record> {
    let doc = string_attribute(item, "doc")?;
    let token = string_attribute(item, "v")?;
    ensure!(!token.is_empty(), "a table item has an empty token");
    let updated_ms = item
        .get("updated_ms")
        .and_then(|value| value.get("N"))
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    Ok(Record {
        body: Bytes::from(doc.into_bytes()),
        token,
        updated_ms,
    })
}

impl Listed {
    /// The listed record in the shape `Bucket::list` answers with.
    pub(crate) fn object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(self.key.as_str()),
            last_modified: chrono::DateTime::from_timestamp_millis(self.record.updated_ms)
                .unwrap_or_default(),
            size: self.record.body.len() as u64,
            e_tag: Some(self.record.token.clone()),
            version: None,
        }
    }
}

// ── The fleet marker and resolution ─────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    format: u8,
    backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fleet: Option<String>,
}

impl Marker {
    fn backend(&self) -> anyhow::Result<Backend> {
        ensure!(
            self.format == MARKER_FORMAT,
            "{MARKER_KEY} has format {}; this release reads format {MARKER_FORMAT}",
            self.format
        );
        match self.backend.as_str() {
            "bucket" => Ok(Backend::Bucket),
            "dynamodb" => {
                let table = self
                    .table
                    .clone()
                    .with_context(|| format!("{MARKER_KEY} selects dynamodb without a table"))?;
                validate_table_name(&table)?;
                Ok(Backend::DynamoDb { table })
            }
            other => bail!("{MARKER_KEY} names an unknown backend {other:?}"),
        }
    }
}

/// Who is resolving the route, which decides what resolution may write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// A serving node. It records the fleet's choice when the bucket has
    /// none, claims and probes the table, and refuses to start on any
    /// disagreement.
    Node,
    /// The node's second, lease-lane client. The node has already resolved;
    /// this one follows the marker and opens its own connection pool.
    Lease,
    /// An operator command. It writes nothing and follows the marker, or
    /// `CELLD_CONTROL` for a fleet that has none yet.
    Operator,
}

/// Where a bucket's coordination records live, shared by every clone of one
/// opened bucket.
pub(crate) struct Route {
    resolved: OnceLock<Option<Arc<Table>>>,
}

impl Route {
    /// A route that has not been resolved: a production bucket before
    /// startup reads the marker.
    pub(crate) fn unresolved() -> Arc<Self> {
        Arc::new(Self {
            resolved: OnceLock::new(),
        })
    }

    /// The bucket route, fixed. Development and test buckets use it: they
    /// have no table to resolve and must issue the requests they always did.
    pub(crate) fn bucket() -> Arc<Self> {
        let route = Self::unresolved();
        let _ = route.resolved.set(None);
        route
    }

    /// The table this bucket's records are routed to, `None` for the bucket.
    pub(crate) fn table(&self) -> anyhow::Result<Option<&Arc<Table>>> {
        match self.resolved.get() {
            Some(table) => Ok(table.as_ref()),
            None if configured_for_table() => bail!(
                "this process is configured for a DynamoDB control table, but this bucket \
                 client was never resolved against {MARKER_KEY}"
            ),
            None => Ok(None),
        }
    }

    fn install(&self, table: Option<Arc<Table>>) -> anyhow::Result<()> {
        match self.resolved.get() {
            None => {
                let _ = self.resolved.set(table);
                Ok(())
            }
            Some(existing) => {
                let same = match (existing, &table) {
                    (None, None) => true,
                    (Some(a), Some(b)) => a.name == b.name && a.region == b.region,
                    _ => false,
                };
                ensure!(
                    same,
                    "this bucket client was already resolved to another store"
                );
                Ok(())
            }
        }
    }
}

/// What resolution found, for the startup banner and `celld control show`.
#[derive(Clone, Debug)]
pub struct Resolved {
    pub backend: Backend,
    pub region: Option<String>,
    pub fleet: Option<String>,
}

impl std::fmt::Display for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.backend)?;
        if let Some(region) = &self.region {
            write!(f, " in {region}")?;
        }
        Ok(())
    }
}

/// The inputs a table needs from its bucket client.
pub(crate) struct AwsAccess {
    pub(crate) credentials: AwsCredentialProvider,
    pub(crate) region: String,
}

async fn read_marker(bucket: &Bucket) -> anyhow::Result<Option<Marker>> {
    let Some((bytes, _)) = bucket.get(MARKER_KEY).await? else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_slice(&bytes).with_context(|| format!("decode {MARKER_KEY}"))?,
    ))
}

/// The table a marker names, opened for this process.
fn open_table(
    bucket: &Bucket,
    table: &str,
    region: &str,
    settings: &Settings,
    app: Option<&str>,
) -> anyhow::Result<Table> {
    let access = bucket.aws_access().with_context(|| {
        format!(
            "a DynamoDB control table needs an s3:// fleet bucket, and {}:// has no AWS credentials",
            bucket.scheme()
        )
    })?;
    let url = settings
        .endpoint
        .clone()
        .unwrap_or_else(|| format!("https://dynamodb.{region}.amazonaws.com"));
    let transport = HttpTransport::new(url, region.to_string(), access.credentials.clone(), app)?;
    Ok(Table::with_transport(
        table.to_string(),
        region.to_string(),
        Arc::new(transport),
    ))
}

fn random_fleet_id() -> String {
    let mut rng = crate::asyncrt::rng("control_fleet_id");
    format!("{:016x}{:016x}", rng.next_u64(), rng.next_u64())
}

/// Does the bucket still hold an unexpired node lease? A table fleet cannot
/// start beside a running bucket fleet: the two would each believe they
/// alone decide who owns a cell.
async fn live_bucket_lease(bucket: &Bucket) -> anyhow::Result<Option<String>> {
    #[derive(Deserialize)]
    struct Lease {
        expires_ms: u64,
    }
    let now = crate::asyncrt::wall_ms().max(0) as u64;
    for object in bucket.list_bucket_objects("nodes/").await? {
        let key = object.location.as_ref().to_string();
        let Some((bytes, _)) = bucket.get_bucket_object(&key).await? else {
            continue;
        };
        let lease: Lease =
            serde_json::from_slice(&bytes).with_context(|| format!("decode {key}"))?;
        if lease.expires_ms > now {
            return Ok(Some(key));
        }
    }
    Ok(None)
}

/// Resolve which store `bucket` routes its coordination records to, and
/// install the answer on every clone of it.
pub async fn resolve(bucket: &Bucket, role: Role) -> anyhow::Result<Resolved> {
    let settings = Settings::from_env()?;
    resolve_with(bucket, role, &settings, None).await
}

/// [`resolve`] with explicit settings, and optionally a transport, which
/// tests use in place of HTTPS.
pub(crate) async fn resolve_with(
    bucket: &Bucket,
    role: Role,
    settings: &Settings,
    transport: Option<Arc<dyn Transport>>,
) -> anyhow::Result<Resolved> {
    let mut marker = read_marker(bucket).await?;
    if marker.is_none() && role == Role::Node {
        marker = Some(create_marker(bucket, settings).await?);
    }
    if marker.is_none() && role == Role::Lease {
        bail!("{MARKER_KEY} is missing; the node resolves its store before its lease lane");
    }

    let (backend, region, fleet) = match &marker {
        Some(marker) => {
            let backend = marker.backend()?;
            if let Some(configured) = &settings.backend {
                ensure!(
                    *configured == backend,
                    "CELLD_CONTROL is {configured}, but this fleet's {MARKER_KEY} selects \
                     {backend}; a fleet keeps its coordination records in one store"
                );
            }
            (backend, marker.region.clone(), marker.fleet.clone())
        }
        // An operator command against a fleet that has not started yet.
        None => (
            settings.backend.clone().unwrap_or(Backend::Bucket),
            settings.region.clone(),
            None,
        ),
    };

    let table = match &backend {
        Backend::Bucket => None,
        Backend::DynamoDb { table } => {
            let region = region
                .clone()
                .or_else(|| settings.region.clone())
                .or_else(|| bucket.aws_access().map(|access| access.region.clone()))
                .context("no region for the DynamoDB control table")?;
            let app = (role == Role::Lease).then_some("celld-lease");
            let table = match &transport {
                Some(transport) => Table::with_transport(table.clone(), region, transport.clone()),
                None => open_table(bucket, table, &region, settings, app)?,
            };
            if role == Role::Node {
                table.check_shape().await?;
                let fleet = fleet
                    .as_deref()
                    .with_context(|| format!("{MARKER_KEY} selects a table without a fleet id"))?;
                table.claim(fleet).await?;
                table.probe().await?;
            } else if let Some(fleet) = &fleet {
                match table.fleet().await? {
                    Some(owner) if &owner == fleet => {}
                    Some(owner) => bail!(
                        "dynamodb://{} serves fleet {owner}, not this bucket's fleet {fleet}",
                        table.name
                    ),
                    // A table the fleet named but no node has claimed yet.
                    None => {}
                }
            }
            Some(Arc::new(table))
        }
    };
    bucket.control_route().install(table)?;
    Ok(Resolved {
        backend,
        region,
        fleet,
    })
}

/// Record this node's configured store as the fleet's choice. Two nodes can
/// race; the loser reads the winner's marker and follows it or refuses.
async fn create_marker(bucket: &Bucket, settings: &Settings) -> anyhow::Result<Marker> {
    let backend = settings.backend.clone().unwrap_or(Backend::Bucket);
    let marker = match &backend {
        Backend::Bucket => Marker {
            format: MARKER_FORMAT,
            backend: backend.name().to_string(),
            table: None,
            region: None,
            fleet: None,
        },
        Backend::DynamoDb { table } => {
            if let Some(key) = live_bucket_lease(bucket).await? {
                bail!(
                    "{key} is a live node lease in the bucket; a fleet moves to a DynamoDB \
                     control table only after every node has stopped"
                );
            }
            Marker {
                format: MARKER_FORMAT,
                backend: backend.name().to_string(),
                table: Some(table.clone()),
                region: settings
                    .region
                    .clone()
                    .or_else(|| bucket.aws_access().map(|access| access.region.clone())),
                fleet: Some(random_fleet_id()),
            }
        }
    };
    let body = serde_json::to_vec(&marker)?;
    if bucket.put_cas(MARKER_KEY, body, None).await?.is_some() {
        tracing::info!(event = "control_marker_created", backend = %backend, "recorded the fleet's coordination store");
        return Ok(marker);
    }
    read_marker(bucket)
        .await?
        .with_context(|| format!("{MARKER_KEY} was created and then removed"))
}

/// `celld control init`: create the table if it is absent, claim it for this
/// fleet, and record the choice in the bucket.
pub async fn init(
    bucket: &Bucket,
    settings: &Settings,
    create_table: bool,
) -> anyhow::Result<Resolved> {
    let Some(Backend::DynamoDb { table }) = &settings.backend else {
        bail!("celld control init needs --table NAME or CELLD_CONTROL=dynamodb://NAME");
    };
    if let Some(marker) = read_marker(bucket).await? {
        let existing = marker.backend()?;
        ensure!(
            existing
                == Backend::DynamoDb {
                    table: table.clone()
                },
            "this fleet's {MARKER_KEY} already selects {existing}"
        );
    }
    let region = settings
        .region
        .clone()
        .or_else(|| bucket.aws_access().map(|access| access.region.clone()))
        .context("no region for the DynamoDB control table")?;
    let opened = open_table(bucket, table, &region, settings, None)?;
    if create_table && opened.create().await? {
        tracing::info!(table = %table, "created the control table");
    }
    opened.check_shape().await?;
    let marker = match read_marker(bucket).await? {
        Some(marker) => marker,
        None => {
            let mut settings = settings.clone();
            settings.region = Some(region);
            create_marker(bucket, &settings).await?
        }
    };
    let fleet = marker.fleet.clone().context("the marker has no fleet id")?;
    opened.claim(&fleet).await?;
    opened.probe().await?;
    Ok(Resolved {
        backend: marker.backend()?,
        region: marker.region,
        fleet: Some(fleet),
    })
}

/// `celld control show`: the marker, and the table's health when there is one.
pub async fn show(bucket: &Bucket) -> anyhow::Result<Map<String, Value>> {
    let settings = Settings::from_env()?;
    let mut out = Map::new();
    let Some(marker) = read_marker(bucket).await? else {
        out.insert("marker".into(), Value::Null);
        out.insert(
            "backend".into(),
            json!(settings.backend.unwrap_or(Backend::Bucket).to_string()),
        );
        return Ok(out);
    };
    out.insert("marker".into(), serde_json::to_value(&marker)?);
    let backend = marker.backend()?;
    out.insert("backend".into(), json!(backend.to_string()));
    if let Backend::DynamoDb { table } = &backend {
        let region = marker
            .region
            .clone()
            .or_else(|| settings.region.clone())
            .or_else(|| bucket.aws_access().map(|access| access.region.clone()))
            .context("no region for the DynamoDB control table")?;
        let opened = open_table(bucket, table, &region, &settings, None)?;
        out.insert(
            "shape".into(),
            json!(match opened.check_shape().await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("{error:#}"),
            }),
        );
        out.insert("table_fleet".into(), json!(opened.fleet().await?));
        out.insert(
            "point_in_time_recovery".into(),
            json!(opened.point_in_time_recovery().await.ok()),
        );
        out.insert(
            "node_leases".into(),
            json!(opened.query(NODES_PK).await?.len()),
        );
    }
    Ok(out)
}
