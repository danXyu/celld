//! Landing rows through Snowpipe Streaming's REST API (high-performance
//! architecture), on the elastic channel of [`LANDING_PIPE`].
//!
//! The elastic channel suits the export: an append answered 200 is durably
//! buffered by Snowflake, and appends are not ordered, which the tables never
//! needed, since completeness comes from positions and watermarks. There is
//! no channel to open and no offset token to track: the loader commits its
//! topic offsets once every append of a batch is acknowledged. Snowpipe
//! Streaming bills per GB and runs on no warehouse of ours.
//!
//! Connecting: the key-pair JWT the SQL API uses asks the account's host for
//! the ingest host (`GET /v2/streaming/hostname`), then trades itself for a
//! token scoped to that host (`POST /oauth/token`). Appends go to the ingest
//! host with the scoped token. A 401 connects again.
//!
//! An append carries at most [`MAX_REQUEST_BYTES`] of NDJSON, so a batch
//! may take several. A failed append is sent again with the same request id
//! and the next `retryCount`, which Snowflake may still apply twice; a row
//! landed twice is a duplicate every reader drops.

use std::io::Read as _;
use std::time::Duration;

use serde::Deserialize;

use crate::consume::Land;
use crate::loader::WarehouseError;
use crate::sql_api::{
    request_id, Clock, Connection, KeyPair, ATTEMPTS, MAX_BODY, TOKEN_LIFETIME, TOKEN_MARGIN,
};
use crate::{LandingRow, LANDING_PIPE};

/// Snowpipe Streaming's limit on one append's payload, less room to spare.
pub const MAX_REQUEST_BYTES: usize = 4_000_000;

/// How long a scoped token is used before a new one is fetched. Snowflake
/// does not document its lifetime; a 401 also fetches one.
const SCOPED_TOKEN_REFRESH: u64 = 30 * 60;

pub struct Streaming {
    control: String,
    account: String,
    user: String,
    database: String,
    schema: String,
    pipe: String,
    key: KeyPair,
    clock: Clock,
    agent: ureq::Agent,
    jwt: Option<(String, u64)>,
    ingest: Option<Ingest>,
    /// Waits between retries; a test makes it a no-op.
    pub pause: fn(Duration),
    /// The largest append payload; tests make it small.
    pub max_request_bytes: usize,
}

struct Ingest {
    /// Scheme and host, such as `https://xy12345.snowflakecomputing.com`.
    base: String,
    token: String,
    fetched: u64,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: Option<String>,
    message: Option<String>,
}

impl Streaming {
    /// Land through [`LANDING_PIPE`] in `connection`'s database and schema,
    /// as `connection`'s user with `key`.
    pub fn new(connection: &Connection, key: KeyPair, clock: Clock) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Streaming {
            control: connection.base_url(),
            account: connection.account.clone(),
            user: connection.user.clone(),
            database: connection.database.clone(),
            schema: connection.schema.clone(),
            pipe: LANDING_PIPE.to_string(),
            key,
            clock,
            agent,
            jwt: None,
            ingest: None,
            pause: std::thread::sleep,
            max_request_bytes: MAX_REQUEST_BYTES,
        }
    }

    fn jwt(&mut self) -> String {
        let now = (self.clock)();
        match &self.jwt {
            Some((t, expires)) if now + TOKEN_MARGIN < *expires => t.clone(),
            _ => {
                let t = self.key.jwt(&self.account, &self.user, now);
                self.jwt = Some((t.clone(), now + TOKEN_LIFETIME));
                t
            }
        }
    }

    /// Find the ingest host and fetch a token scoped to it.
    fn connect(&mut self) -> Result<(), WarehouseError> {
        let auth = format!("Bearer {}", self.jwt());
        let url = format!("{}/v2/streaming/hostname", self.control);
        let sent = self
            .agent
            .get(&url)
            .header("Authorization", &auth)
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header("User-Agent", "celld-export-loader")
            .call();
        let host = answer(sent, "the ingest host")?;
        // Documented as a JSON object and shown as bare text; take either.
        let host = match serde_json::from_str::<serde_json::Value>(&host) {
            Ok(serde_json::Value::Object(o)) => o
                .get("hostname")
                .and_then(|h| h.as_str())
                .unwrap_or_default()
                .to_string(),
            Ok(serde_json::Value::String(s)) => s,
            _ => host.trim().to_string(),
        };
        // An account name's underscores are dashes in a host name.
        let host = host.trim().replace('_', "-");
        if host.is_empty() {
            return Err(WarehouseError::other(
                "Snowpipe Streaming returned no ingest host",
            ));
        }
        let form = format!(
            "grant_type={}&scope={}",
            form_encode("urn:ietf:params:oauth:grant-type:jwt-bearer"),
            form_encode(&host)
        );
        let sent = self
            .agent
            .post(&format!("{}/oauth/token", self.control))
            .header("Authorization", &auth)
            .header("User-Agent", "celld-export-loader")
            .content_type("application/x-www-form-urlencoded")
            .send(form.as_bytes());
        let token = answer(sent, "a scoped token")?;
        let token = match serde_json::from_str::<serde_json::Value>(&token) {
            Ok(serde_json::Value::Object(o)) => o
                .get("token")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string(),
            _ => token.trim().to_string(),
        };
        if token.is_empty() {
            return Err(WarehouseError::other(
                "Snowpipe Streaming returned no scoped token",
            ));
        }
        let scheme = self.control.split("://").next().unwrap_or("https");
        self.ingest = Some(Ingest {
            base: format!("{scheme}://{host}"),
            token,
            fetched: (self.clock)(),
        });
        Ok(())
    }

    /// Append one NDJSON payload, retrying what may be retried.
    fn append(&mut self, payload: &[u8]) -> Result<(), WarehouseError> {
        let request = request_id()?;
        let mut last = String::new();
        let mut delay = Duration::from_secs(1);
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                (self.pause)(delay);
                delay *= 2;
            }
            let stale = self
                .ingest
                .as_ref()
                .is_none_or(|i| (self.clock)() >= i.fetched + SCOPED_TOKEN_REFRESH);
            if stale {
                if let Err(e) = self.connect() {
                    last = e.message;
                    continue;
                }
            }
            let ingest = self.ingest.as_ref().expect("connected above");
            let url = format!(
                "{}/v2/streaming/data/databases/{}/schemas/{}/pipes/{}/channels/ELASTIC/rows?requestId={request}&retryCount={attempt}",
                ingest.base, self.database, self.schema, self.pipe
            );
            let sent = self
                .agent
                .post(&url)
                .header("Authorization", &format!("Bearer {}", ingest.token))
                .header("User-Agent", "celld-export-loader")
                .content_type("application/x-ndjson")
                .send(payload);
            let mut response = match sent {
                Ok(r) => r,
                Err(e) => {
                    last = e.to_string();
                    continue;
                }
            };
            let status = response.status().as_u16();
            let mut text = String::new();
            let _ = response
                .body_mut()
                .with_config()
                .limit(MAX_BODY)
                .reader()
                .read_to_string(&mut text);
            match status {
                200 => return Ok(()),
                401 => {
                    // The scoped token expired; fetch another.
                    self.ingest = None;
                    last = format!("401: {text}");
                }
                408 | 429 | 500 | 502 | 503 | 504 => last = format!("{status}: {text}"),
                _ => {
                    return Err(match serde_json::from_str::<ErrorBody>(&text) {
                        Ok(b) => WarehouseError {
                            code: b.code,
                            sql_state: None,
                            message: format!(
                                "Snowpipe Streaming answered {status}: {}",
                                b.message.unwrap_or(text)
                            ),
                        },
                        Err(_) => WarehouseError::other(format!(
                            "Snowpipe Streaming answered {status}: {text}"
                        )),
                    })
                }
            }
        }
        Err(WarehouseError::other(format!(
            "Snowpipe Streaming gave up after {ATTEMPTS} attempts: {last}"
        )))
    }
}

impl Land for Streaming {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
        for payload in payloads(rows, self.max_request_bytes)? {
            self.append(&payload)?;
        }
        Ok(())
    }
}

/// `rows` as NDJSON payloads of at most `limit` bytes each, in order.
pub fn payloads(rows: &[LandingRow], limit: usize) -> Result<Vec<Vec<u8>>, WarehouseError> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for row in rows {
        let mut line = serde_json::to_vec(row).expect("landing rows always encode");
        line.push(b'\n');
        if line.len() > limit {
            return Err(WarehouseError::other(format!(
                "the record from {} is {} bytes, more than one append may carry ({limit})",
                row.source,
                line.len()
            )));
        }
        if current.len() + line.len() > limit {
            out.push(std::mem::take(&mut current));
        }
        current.extend_from_slice(&line);
    }
    if !current.is_empty() {
        out.push(current);
    }
    Ok(out)
}

fn answer(
    sent: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<String, WarehouseError> {
    let mut response =
        sent.map_err(|e| WarehouseError::other(format!("asking for {what}: {e}")))?;
    let status = response.status().as_u16();
    let mut text = String::new();
    response
        .body_mut()
        .with_config()
        .limit(MAX_BODY)
        .reader()
        .read_to_string(&mut text)
        .map_err(|e| WarehouseError::other(format!("asking for {what}: {e}")))?;
    if status != 200 {
        return Err(WarehouseError::other(format!(
            "asking for {what}: Snowflake answered {status}: {text}"
        )));
    }
    Ok(text)
}

/// `application/x-www-form-urlencoded` for a value.
fn form_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, body_bytes: usize) -> LandingRow {
        LandingRow {
            kind: "watermark".into(),
            script: "app".into(),
            class: "Room".into(),
            cell: "r1".into(),
            cell_name: None,
            facet: None,
            incarnation: u64::MAX,
            epoch: 1,
            txid: 2,
            commit: 3,
            committed_at: 1_790_000_000_000,
            node: "n".into(),
            origin: "live".into(),
            fragment: 1,
            fragments: 1,
            body: "x".repeat(body_bytes),
            source: source.into(),
        }
    }

    #[test]
    fn payloads_split_at_the_limit_and_keep_every_row_whole() {
        let rows: Vec<LandingRow> = (0..10).map(|i| row(&format!("s{i}"), 100)).collect();
        let line = serde_json::to_vec(&rows[0]).unwrap().len() + 1;
        let out = payloads(&rows, line * 3 + 1).unwrap();
        assert_eq!(out.len(), 4);
        let mut back = Vec::new();
        for p in &out {
            assert!(p.len() <= line * 3 + 1);
            for l in std::str::from_utf8(p).unwrap().lines() {
                back.push(serde_json::from_str::<LandingRow>(l).unwrap());
            }
        }
        assert_eq!(back, rows);
        // u64::MAX is a JSON number the pipe casts to NUMBER(20, 0).
        assert!(std::str::from_utf8(&out[0])
            .unwrap()
            .contains("\"incarnation\":18446744073709551615"));
        assert!(payloads(&[], 10).unwrap().is_empty());
        assert!(payloads(&[row("big", 100)], 50).is_err());
    }

    #[test]
    fn form_values_are_encoded() {
        assert_eq!(
            form_encode("urn:ietf:params:oauth:grant-type:jwt-bearer"),
            "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
        );
        assert_eq!(form_encode("a-b.c_d:80"), "a-b.c_d%3A80");
    }
}
