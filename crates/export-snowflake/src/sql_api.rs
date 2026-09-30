//! A [`Warehouse`] on Snowflake's SQL API (`/api/v2/statements`), with
//! key-pair authentication.
//!
//! One statement per request. A statement that runs past the request is
//! polled at its status URL; a result of several partitions is fetched
//! partition by partition. A request that fails with 429, 503 or 504, or
//! never gets an answer, is sent again with the same request id and
//! `retry=true`, so Snowflake runs it at most once.

use std::io::Read as _;
use std::time::Duration;

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use rsa::pkcs1::DecodeRsaPrivateKey as _;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::{DecodePrivateKey as _, EncodePublicKey as _};
use rsa::signature::{SignatureEncoding as _, Signer as _};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest as _, Sha256};

use crate::loader::{Rows, Warehouse, WarehouseError};

/// Where and as whom statements run.
#[derive(Clone, Debug)]
pub struct Connection {
    /// The account identifier, such as `myorg-myaccount` or `xy12345.us-east-2.aws`.
    pub account: String,
    pub user: String,
    pub role: Option<String>,
    pub database: String,
    pub schema: String,
    pub warehouse: String,
    /// The API's base URL; by default `https://<account>.snowflakecomputing.com`.
    pub url: Option<String>,
    /// How long one statement may run, in seconds.
    pub statement_timeout: u64,
}

impl Connection {
    pub(crate) fn base_url(&self) -> String {
        match &self.url {
            Some(u) => u.trim_end_matches('/').to_string(),
            None => format!(
                "https://{}.snowflakecomputing.com",
                self.account.to_ascii_lowercase().replace('_', "-")
            ),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("the private key is neither PKCS#8 nor PKCS#1 PEM: {0}")]
    Parse(String),
    #[error("the public key does not encode: {0}")]
    Public(String),
}

/// The user's RSA key, as registered with `ALTER USER ... SET RSA_PUBLIC_KEY`.
pub struct KeyPair {
    signing: SigningKey<Sha256>,
    /// `SHA256:` and the base64 SHA-256 of the public key's DER, which is
    /// how Snowflake names the key (`RSA_PUBLIC_KEY_FP`).
    fingerprint: String,
}

impl KeyPair {
    /// A PKCS#8 (optionally encrypted) or PKCS#1 PEM private key.
    pub fn from_pem(pem: &str, passphrase: Option<&str>) -> Result<Self, KeyError> {
        let key = match passphrase {
            Some(p) => RsaPrivateKey::from_pkcs8_encrypted_pem(pem, p.as_bytes())
                .map_err(|e| KeyError::Parse(e.to_string()))?,
            None => RsaPrivateKey::from_pkcs8_pem(pem)
                .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
                .map_err(|e| KeyError::Parse(e.to_string()))?,
        };
        Self::from_key(key)
    }

    pub fn from_key(key: RsaPrivateKey) -> Result<Self, KeyError> {
        let der = key
            .to_public_key()
            .to_public_key_der()
            .map_err(|e| KeyError::Public(e.to_string()))?;
        let fingerprint = format!("SHA256:{}", STANDARD.encode(Sha256::digest(der.as_bytes())));
        Ok(KeyPair {
            signing: SigningKey::<Sha256>::new(key),
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The RS256 JWT the SQL API takes as a bearer token, valid from `now`
    /// (seconds since the Unix epoch) for an hour, Snowflake's limit.
    pub fn jwt(&self, account: &str, user: &str, now: u64) -> String {
        // The account part of the claims has no region and is upper case.
        let account = account
            .split('.')
            .next()
            .unwrap_or(account)
            .to_ascii_uppercase();
        let user = user.to_ascii_uppercase();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = json!({
            "iss": format!("{account}.{user}.{}", self.fingerprint),
            "sub": format!("{account}.{user}"),
            "iat": now,
            "exp": now + TOKEN_LIFETIME,
        });
        let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
        let signed = format!("{header}.{claims}");
        let signature = URL_SAFE_NO_PAD.encode(self.signing.sign(signed.as_bytes()).to_bytes());
        format!("{signed}.{signature}")
    }
}

pub(crate) const TOKEN_LIFETIME: u64 = 3600;
/// A token is replaced this long before it expires.
pub(crate) const TOKEN_MARGIN: u64 = 300;
pub(crate) const ATTEMPTS: u32 = 6;
pub(crate) const MAX_BODY: u64 = 1 << 30;

/// A random (version 4) UUID, as the SQL API and Snowpipe Streaming take a
/// request id.
pub(crate) fn request_id() -> Result<String, WarehouseError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| WarehouseError::other(e.to_string()))?;
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    let h: String = id.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

/// Seconds since the Unix epoch.
pub type Clock = Box<dyn Fn() -> u64 + Send>;

pub struct SqlApi {
    connection: Connection,
    key: KeyPair,
    clock: Clock,
    agent: ureq::Agent,
    token: Option<(String, u64)>,
    /// Waits between retries and polls; a test makes it a no-op.
    pub pause: fn(Duration),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    result_set_meta_data: Option<Meta>,
    data: Option<Vec<Vec<Option<String>>>>,
    code: Option<String>,
    message: Option<String>,
    sql_state: Option<String>,
    statement_handle: Option<String>,
    statement_status_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Meta {
    #[serde(default)]
    row_type: Vec<Column>,
    #[serde(default)]
    partition_info: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
}

enum Reply {
    Done(Response),
    Running(Response),
}

impl SqlApi {
    pub fn new(connection: Connection, key: KeyPair, clock: Clock) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(connection.statement_timeout + 60)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        SqlApi {
            connection,
            key,
            clock,
            agent,
            token: None,
            pause: std::thread::sleep,
        }
    }

    fn token(&mut self) -> String {
        let now = (self.clock)();
        match &self.token {
            Some((t, expires)) if now + TOKEN_MARGIN < *expires => t.clone(),
            _ => {
                let t = self
                    .key
                    .jwt(&self.connection.account, &self.connection.user, now);
                self.token = Some((t.clone(), now + TOKEN_LIFETIME));
                t
            }
        }
    }

    /// Send one request, retrying what may be retried. `body` is `None` for
    /// a GET.
    fn request(&mut self, url: &str, body: Option<&str>) -> Result<Reply, WarehouseError> {
        let mut last = String::new();
        let mut delay = Duration::from_secs(1);
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                (self.pause)(delay);
                delay *= 2;
            }
            let url = if attempt > 0 && body.is_some() {
                format!("{url}&retry=true")
            } else {
                url.to_string()
            };
            let token = self.token();
            let auth = format!("Bearer {token}");
            let sent = match body {
                Some(b) => self
                    .agent
                    .post(&url)
                    .header("Authorization", &auth)
                    .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                    .header("Accept", "application/json")
                    .header("User-Agent", "celld-export-loader")
                    .content_type("application/json")
                    .send(b),
                None => self
                    .agent
                    .get(&url)
                    .header("Authorization", &auth)
                    .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                    .header("Accept", "application/json")
                    .header("User-Agent", "celld-export-loader")
                    .call(),
            };
            let mut response = match sent {
                Ok(r) => r,
                Err(e) => {
                    last = e.to_string();
                    continue;
                }
            };
            let status = response.status().as_u16();
            let mut text = String::new();
            if let Err(e) = response
                .body_mut()
                .with_config()
                .limit(MAX_BODY)
                .reader()
                .read_to_string(&mut text)
            {
                last = e.to_string();
                continue;
            }
            match status {
                200 | 202 => {
                    let parsed: Response = serde_json::from_str(&text).map_err(|e| {
                        WarehouseError::other(format!("SQL API answered {status} with {e}"))
                    })?;
                    return Ok(if status == 200 {
                        Reply::Done(parsed)
                    } else {
                        Reply::Running(parsed)
                    });
                }
                401 if attempt == 0 => {
                    // A token the clock thought fresh; mint another.
                    self.token = None;
                    last = text;
                }
                429 | 500 | 503 | 504 => last = format!("{status}: {text}"),
                _ => {
                    return Err(match serde_json::from_str::<Response>(&text) {
                        Ok(r) => WarehouseError {
                            code: r.code,
                            sql_state: r.sql_state,
                            message: r.message.unwrap_or(text),
                        },
                        Err(_) => {
                            WarehouseError::other(format!("SQL API answered {status}: {text}"))
                        }
                    })
                }
            }
        }
        Err(WarehouseError::other(format!(
            "SQL API gave up after {ATTEMPTS} attempts: {last}"
        )))
    }
}

/// The SQL API's `bindings`: `{"1": {"type": ..., "value": ...}, ...}`, every
/// value as text.
pub fn bindings(binds: &[serde_json::Value]) -> Result<serde_json::Value, WarehouseError> {
    use serde_json::Value as J;
    let mut out = serde_json::Map::new();
    for (i, b) in binds.iter().enumerate() {
        let (ty, value) = match b {
            J::Null => ("TEXT", J::Null),
            J::Bool(v) => ("BOOLEAN", json!(v.to_string())),
            J::Number(n) if n.is_f64() => ("REAL", json!(n.to_string())),
            J::Number(n) => ("FIXED", json!(n.to_string())),
            J::String(v) => ("TEXT", json!(v)),
            other => {
                return Err(WarehouseError::other(format!(
                    "bind {} is {other}, not a scalar",
                    i + 1
                )))
            }
        };
        out.insert((i + 1).to_string(), json!({"type": ty, "value": value}));
    }
    Ok(J::Object(out))
}

impl Warehouse for SqlApi {
    fn execute_bound(
        &mut self,
        sql: &str,
        binds: &[serde_json::Value],
    ) -> Result<Rows, WarehouseError> {
        let base = self.connection.base_url();
        let c = &self.connection;
        let mut body = json!({
            "statement": sql,
            "timeout": c.statement_timeout,
            "database": c.database,
            "schema": c.schema,
            "warehouse": c.warehouse,
        });
        if let Some(role) = &c.role {
            body["role"] = json!(role);
        }
        if !binds.is_empty() {
            body["bindings"] = bindings(binds)?;
        }
        let request_id = request_id()?;
        let url = format!("{base}/api/v2/statements?requestId={request_id}");
        let mut reply = self.request(&url, Some(&body.to_string()))?;
        let mut delay = Duration::from_millis(250);
        let done = loop {
            match reply {
                Reply::Done(r) => break r,
                Reply::Running(r) => {
                    let status = match (r.statement_status_url, r.statement_handle) {
                        (Some(u), _) if u.starts_with('/') => format!("{base}{u}"),
                        (Some(u), _) => u,
                        (None, Some(h)) => format!("{base}/api/v2/statements/{h}"),
                        (None, None) => {
                            return Err(WarehouseError::other(
                                "SQL API answered 202 with no statement handle",
                            ))
                        }
                    };
                    (self.pause)(delay);
                    delay = (delay * 2).min(Duration::from_secs(5));
                    reply = self.request(&status, None)?;
                }
            }
        };
        let meta = done.result_set_meta_data.unwrap_or(Meta {
            row_type: Vec::new(),
            partition_info: Vec::new(),
        });
        let mut rows = Rows {
            columns: meta.row_type.into_iter().map(|c| c.name).collect(),
            data: done.data.unwrap_or_default(),
        };
        if meta.partition_info.len() > 1 {
            let handle = done
                .statement_handle
                .ok_or_else(|| WarehouseError::other("a partitioned result has no handle"))?;
            for p in 1..meta.partition_info.len() {
                let url = format!("{base}/api/v2/statements/{handle}?partition={p}");
                match self.request(&url, None)? {
                    Reply::Done(r) => rows.data.extend(r.data.unwrap_or_default()),
                    Reply::Running(_) => {
                        return Err(WarehouseError::other(format!(
                            "partition {p} of a finished statement is still running"
                        )))
                    }
                }
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1v15::VerifyingKey;
    use rsa::signature::Verifier as _;

    fn key() -> RsaPrivateKey {
        // A fixed seed keeps the test fast and repeatable; 1024 bits is
        // plenty to check the encoding.
        use rand_chacha::rand_core::SeedableRng as _;
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(7);
        RsaPrivateKey::new(&mut rng, 1024).unwrap()
    }

    #[test]
    fn jwt_carries_snowflakes_claims_and_verifies() {
        let private = key();
        let public = private.to_public_key();
        let pair = KeyPair::from_key(private).unwrap();
        let jwt = pair.jwt("xy12345.us-east-2.aws", "loader", 1_790_000_000);
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["sub"], "XY12345.LOADER");
        assert_eq!(
            claims["iss"],
            format!("XY12345.LOADER.{}", pair.fingerprint())
        );
        assert_eq!(claims["exp"], 1_790_003_600u64);
        let sig = rsa::pkcs1v15::Signature::try_from(
            URL_SAFE_NO_PAD.decode(parts[2]).unwrap().as_slice(),
        )
        .unwrap();
        VerifyingKey::<Sha256>::new(public)
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .unwrap();
        assert!(pair.fingerprint().starts_with("SHA256:"));
    }

    #[test]
    fn pem_keys_load_in_both_encodings() {
        use rsa::pkcs1::EncodeRsaPrivateKey as _;
        use rsa::pkcs8::EncodePrivateKey as _;
        let k = key();
        let fp = KeyPair::from_key(k.clone()).unwrap().fingerprint;
        let pkcs8 = k.to_pkcs8_pem(Default::default()).unwrap();
        let pkcs1 = k.to_pkcs1_pem(Default::default()).unwrap();
        assert_eq!(KeyPair::from_pem(&pkcs8, None).unwrap().fingerprint, fp);
        assert_eq!(KeyPair::from_pem(&pkcs1, None).unwrap().fingerprint, fp);
        assert!(KeyPair::from_pem("nonsense", None).is_err());
    }

    #[test]
    fn binds_become_typed_text() {
        let b = bindings(&[json!("a"), json!(7), json!(1.5), json!(null), json!(true)]).unwrap();
        assert_eq!(
            b,
            json!({
                "1": {"type": "TEXT", "value": "a"},
                "2": {"type": "FIXED", "value": "7"},
                "3": {"type": "REAL", "value": "1.5"},
                "4": {"type": "TEXT", "value": null},
                "5": {"type": "BOOLEAN", "value": "true"},
            })
        );
        assert!(bindings(&[json!([1])]).is_err());
    }

    #[test]
    fn the_default_url_is_the_accounts_host() {
        let c = Connection {
            account: "MyOrg-My_Account".into(),
            user: "u".into(),
            role: None,
            database: "d".into(),
            schema: "s".into(),
            warehouse: "w".into(),
            url: None,
            statement_timeout: 60,
        };
        assert_eq!(
            c.base_url(),
            "https://myorg-my-account.snowflakecomputing.com"
        );
    }
}
