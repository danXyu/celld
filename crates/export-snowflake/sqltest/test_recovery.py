"""Regression checks for recovery identity and declared data loss."""

import json

from test_sql import load, warehouse


def stage(kind, txid, body, script="app", incarnation=1):
    return dict(
        kind=kind,
        script=script,
        **{"class": "Room"},
        cell="Room:a",
        facet=None,
        incarnation=incarnation,
        epoch=1,
        txid=txid,
        commit=1,
        committed_at=1790000000000,
        node="n",
        origin="live",
        fragment=1,
        fragments=1,
        body=json.dumps(body),
    )


def setup(w, records):
    load(w, dict(tombstones=[], stage_rows=records, dynamic_tables=[]))


def test_scriptless_recovery_targets_existing_stream(warehouse):
    setup(warehouse, [
        stage("link", 0, dict(start_txid=0, prev_epoch=None, prev_txid=None, mode="fresh")),
        stage("recovered", 5, dict(
            session="dead/1", head=dict(epoch=1, txid=5, commit=0), loss=False, cells=1,
        ), script="", incarnation=0),
    ])
    assert not warehouse.rows("SELECT * FROM CELL_STREAMS WHERE script = ''")
    got = warehouse.rows("SELECT script, incarnation, gap_kind FROM EXPORT_GAPS")
    assert any(r["script"] == "app" and r["incarnation"] == 1 for r in got), got


def test_recovered_loss_below_certification_is_detected(warehouse):
    setup(warehouse, [
        stage("watermark", 10, dict(
            through=dict(epoch=1, txid=10, commit=1), commits=0, records=0,
        )),
        stage("recovered", 5, dict(
            session="dead/1", head=dict(epoch=1, txid=5, commit=0), loss=True, cells=1,
        )),
    ])
    assert len(warehouse.rows("SELECT * FROM CELL_CERTIFIED")) == 1
    got = warehouse.rows("SELECT gap_kind, bound_txid FROM EXPORT_GAPS")
    assert len(got) == 1, got
    assert got[0]["gap_kind"] == "recovered"
    assert got[0]["bound_txid"] == 10
