#[test]
fn bucket_proof_survives_recovery_but_new_epochs_require_a_new_barrier() {
    use celld_logic::log_tier::*;
    let mut log = create_record(["follower".to_string()].into_iter().collect(), 0).unwrap();
    log.bucket_complete = true;
    let recovering = start_recovery(&log, "survivor", 1).unwrap();
    assert!(recovering.bucket_complete);
    assert_eq!(takeover_gate(Some(&log)), TakeoverGate::RecoverFirst);
    assert!(finish_recovery(&recovering, 0).unwrap().bucket_complete);
    let replacement =
        plan_reconfigure(&log, 0, ["other".to_string()].into_iter().collect()).unwrap();
    assert!(!replacement.record.bucket_complete);
}
