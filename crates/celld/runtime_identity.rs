// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Process identity retained for the operator's capacity and application observations.

/// Keep the existing identity envelope without advertising a disk-removal API.
/// A malformed actor snapshot must not turn into a successful, empty state response.
pub fn state_json(snapshot: &str, generation: &str) -> anyhow::Result<serde_json::Value> {
    let mut state: serde_json::Value = serde_json::from_str(snapshot)?;
    let object = state
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("actor state is not an object"))?;
    object.insert(
        "shutdown".into(),
        serde_json::json!({
            "schema_version": 1,
            "runtime_generation": generation,
            "capabilities": {"strict_disk_removal": false},
        }),
    );
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_keep_process_identity_without_retired_state() {
        let snapshot = r#"{"node_load":{"rss_bytes":42},"deployment":{"version":"v1"}}"#;
        let first = state_json(snapshot, "process-a").unwrap();
        let restarted = state_json(snapshot, "process-b").unwrap();
        assert_eq!(first["node_load"]["rss_bytes"], 42);
        assert_eq!(first["deployment"]["version"], "v1");
        assert_eq!(first["shutdown"]["schema_version"], 1);
        assert_eq!(first["shutdown"]["runtime_generation"], "process-a");
        assert_eq!(restarted["shutdown"]["runtime_generation"], "process-b");
        assert_eq!(
            first["shutdown"]["capabilities"]["strict_disk_removal"],
            false
        );
        assert!(first.get("node_log").is_none());
        assert!(first["shutdown"].get("operation").is_none());
        assert!(first["shutdown"].get("control_only").is_none());
    }

    #[test]
    fn invalid_actor_snapshots_cannot_masquerade_as_valid_observations() {
        for snapshot in ["not json", "null", "[]", "42"] {
            assert!(state_json(snapshot, "process-a").is_err());
        }
    }
}
