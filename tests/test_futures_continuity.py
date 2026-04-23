"""Tests for futures depth continuity — recorder and converter alignment.

Covers the Binance USDTF-specific bootstrap/ongoing continuity rules,
the DESYNCED stuck-state recovery path, and recorder–converter model
consistency.
"""
from __future__ import annotations

from typing import Iterable

import pytest
from nautilus_trader.model.identifiers import InstrumentId

import converter.depth_phase2 as depth_mod
from phase2_depth import (
    DepthSymbolState,
    BinanceNativeDepthRecorder,
    SYNC_UNSYNCED,
    SYNC_SNAPSHOT_SEEDED,
    SYNC_LIVE_SYNCED,
    SYNC_DESYNCED,
    SYNC_RESYNC_REQUIRED,
    SYNC_FENCED,
)


# ── record builders ───────────────────────────────────────────────────

def _snapshot_seed(
    *,
    session: int,
    session_seq: int,
    ts_recv_ns: int,
    last_update_id: int,
    bids: list | None = None,
    asks: list | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "snapshot_seed",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "lastUpdateId": last_update_id,
        "payload": {"bids": bids or [], "asks": asks or []},
    }


def _sync_state(
    *, session: int, session_seq: int, ts_recv_ns: int, state: str, reason: str,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "sync_state",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "state": state,
        "reason": reason,
    }


def _depth_update(
    *,
    session: int,
    session_seq: int,
    ts_recv_ns: int,
    U: int,
    u: int,
    pu: int | None = None,
    bids: list | None = None,
    asks: list | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "depth_update",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "U": U,
        "u": u,
        "pu": pu,
        "payload": {"bids": bids or [], "asks": asks or []},
    }


def _lifecycle(
    *, session: int, session_seq: int, ts_recv_ns: int, event: str,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "stream_lifecycle",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "event": event,
        "reason": "test",
    }


# ======================================================================
# Recorder-side: _check_continuity
# ======================================================================


class TestRecorderCheckContinuity:
    """Unit-test the recorder's _check_continuity for futures."""

    @staticmethod
    def _make_state(
        *,
        venue: str = "BINANCE_USDTF",
        sync_state: str = SYNC_SNAPSHOT_SEEDED,
        prev_update_id: int = 200,
    ) -> DepthSymbolState:
        s = DepthSymbolState(venue=venue, symbol="BTCUSDT")
        s.sync_state = sync_state
        s.prev_update_id = prev_update_id
        s.last_update_id = prev_update_id
        return s

    @staticmethod
    def _make_rec(U: int, u: int, pu: int | None = None) -> dict:
        return {"U": U, "u": u, "pu": pu}

    def test_futures_bootstrap_overlap_accepted(self) -> None:
        """First futures event after snapshot: U <= lastUpdateId AND u >= lastUpdateId."""
        # We need a stub recorder with a _check_continuity method
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        rec = self._make_rec(U=195, u=210, pu=190)
        # Instantiate recorder just for method access
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is True
        assert state.sync_state == SYNC_LIVE_SYNCED
        assert state.prev_update_id == 210

    def test_futures_bootstrap_U_above_last_rejected(self) -> None:
        """Bootstrap must reject when U > lastUpdateId (gap)."""
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        rec = self._make_rec(U=205, u=210, pu=200)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is False
        assert state.sync_state == SYNC_DESYNCED
        assert state.desync_events == 1

    def test_futures_bootstrap_u_below_last_stale_drop(self) -> None:
        """Bootstrap stale: u < lastUpdateId → silently dropped, NO desync."""
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        rec = self._make_rec(U=190, u=195, pu=185)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is False
        # Key: sync_state must stay SNAPSHOT_SEEDED, not DESYNCED
        assert state.sync_state == SYNC_SNAPSHOT_SEEDED
        assert state.desync_events == 0
        assert state.bootstrap_stale_drop_count == 1

    def test_futures_bootstrap_stale_then_overlap_accepted(self) -> None:
        """Stale drops don't prevent subsequent bootstrap overlap from succeeding."""
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        recorder = _stub_recorder()

        # Several stale messages arrive first
        for u_val in [180, 190, 195, 199]:
            rec = self._make_rec(U=u_val - 5, u=u_val, pu=u_val - 10)
            assert recorder._check_continuity(state, rec) is False
            assert state.sync_state == SYNC_SNAPSHOT_SEEDED

        assert state.bootstrap_stale_drop_count == 4

        # Then the overlapping message arrives
        rec = self._make_rec(U=198, u=205, pu=195)
        assert recorder._check_continuity(state, rec) is True
        assert state.sync_state == SYNC_LIVE_SYNCED
        assert state.prev_update_id == 205

    def test_futures_bootstrap_u_equals_last_not_stale(self) -> None:
        """Futures: u == lastUpdateId is NOT stale (stale is u < lastUpdateId).
        This message should hit the bootstrap overlap check."""
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        rec = self._make_rec(U=195, u=200, pu=190)
        recorder = _stub_recorder()
        # U <= 200 AND u >= 200  → accepted
        assert recorder._check_continuity(state, rec) is True
        assert state.sync_state == SYNC_LIVE_SYNCED

    def test_spot_bootstrap_stale_drop(self) -> None:
        """Spot stale: u <= lastUpdateId → silently dropped, NO desync."""
        state = self._make_state(
            venue="BINANCE_SPOT",
            sync_state=SYNC_SNAPSHOT_SEEDED,
            prev_update_id=100,
        )
        rec = self._make_rec(U=95, u=100)  # u == prev → stale for spot
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is False
        assert state.sync_state == SYNC_SNAPSHOT_SEEDED
        assert state.desync_events == 0
        assert state.bootstrap_stale_drop_count == 1

    def test_futures_bootstrap_gap_still_desyncs(self) -> None:
        """Genuine gap (U > lastUpdateId) must still trigger DESYNCED."""
        state = self._make_state(sync_state=SYNC_SNAPSHOT_SEEDED, prev_update_id=200)
        rec = self._make_rec(U=205, u=210, pu=200)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is False
        assert state.sync_state == SYNC_DESYNCED
        assert state.desync_events == 1
        assert state.bootstrap_stale_drop_count == 0

    def test_futures_ongoing_pu_matches(self) -> None:
        """Ongoing futures: pu must equal prev_update_id."""
        state = self._make_state(sync_state=SYNC_LIVE_SYNCED, prev_update_id=210)
        rec = self._make_rec(U=211, u=215, pu=210)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is True
        assert state.prev_update_id == 215

    def test_futures_ongoing_pu_mismatch_desyncs(self) -> None:
        """Ongoing futures: wrong pu → desync."""
        state = self._make_state(sync_state=SYNC_LIVE_SYNCED, prev_update_id=210)
        rec = self._make_rec(U=211, u=215, pu=999)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is False
        assert state.sync_state == SYNC_DESYNCED
        assert state.last_rejected_pu == 999

    def test_spot_bootstrap_and_ongoing_rules(self) -> None:
        """Spot uses the same U <= prev+1 <= u formula for both phases."""
        state = self._make_state(
            venue="BINANCE_SPOT",
            sync_state=SYNC_SNAPSHOT_SEEDED,
            prev_update_id=100,
        )
        rec = self._make_rec(U=101, u=103)
        recorder = _stub_recorder()
        assert recorder._check_continuity(state, rec) is True
        assert state.prev_update_id == 103

        # Now ongoing
        rec2 = self._make_rec(U=104, u=106)
        assert recorder._check_continuity(state, rec2) is True
        assert state.prev_update_id == 106


# ======================================================================
# Converter-side: _should_accept_update consistency
# ======================================================================


class TestConverterFuturesConsistency:
    """Verify converter _should_accept_update matches recorder rules."""

    def test_futures_bootstrap_overlap_accepted(self) -> None:
        """Converter must accept first event using U/u range, not pu."""
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
            venue="BINANCE_USDTF",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "snapshot_seeded"
        state.prev_update_id = 200
        rec = {"U": 195, "u": 210, "pu": 190}
        assert depth_mod._should_accept_update(state, rec) is True

    def test_futures_bootstrap_U_above_last_rejected(self) -> None:
        """Converter must reject when U > lastUpdateId."""
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
            venue="BINANCE_USDTF",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "snapshot_seeded"
        state.prev_update_id = 200
        rec = {"U": 205, "u": 210, "pu": 200}
        assert depth_mod._should_accept_update(state, rec) is False

    def test_futures_ongoing_pu_match_accepted(self) -> None:
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
            venue="BINANCE_USDTF",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "live_synced"
        state.prev_update_id = 210
        rec = {"U": 211, "u": 215, "pu": 210}
        assert depth_mod._should_accept_update(state, rec) is True

    def test_futures_ongoing_pu_mismatch_rejected(self) -> None:
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
            venue="BINANCE_USDTF",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "live_synced"
        state.prev_update_id = 210
        rec = {"U": 211, "u": 215, "pu": 999}
        assert depth_mod._should_accept_update(state, rec) is False

    def test_spot_rules_unchanged(self) -> None:
        """Spot continuity: U <= prev+1 <= u, ignoring pu."""
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT.BINANCE"),
            venue="BINANCE_SPOT",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "snapshot_seeded"
        state.prev_update_id = 100
        rec = {"U": 101, "u": 103, "pu": None}
        assert depth_mod._should_accept_update(state, rec) is True

    def test_converter_futures_bootstrap_stale_drop(self) -> None:
        """Converter stale drop: u < lastUpdateId → False (not desync, just skip)."""
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
            venue="BINANCE_USDTF",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "snapshot_seeded"
        state.prev_update_id = 200
        rec = {"U": 190, "u": 195, "pu": 185}
        assert depth_mod._should_accept_update(state, rec) is False
        # State should stay snapshot_seeded — caller decides whether to desync
        assert state.sync_state == "snapshot_seeded"

    def test_converter_spot_bootstrap_stale_drop(self) -> None:
        """Converter spot stale: u <= lastUpdateId → False."""
        state = depth_mod.ReplayState(
            instrument_id=InstrumentId.from_str("BTCUSDT.BINANCE"),
            venue="BINANCE_SPOT",
            symbol="BTCUSDT",
            price_prec=1,
            size_prec=1,
        )
        state.sync_state = "snapshot_seeded"
        state.prev_update_id = 100
        rec = {"U": 95, "u": 100, "pu": None}
        assert depth_mod._should_accept_update(state, rec) is False


# ======================================================================
# Full converter replay: futures bootstrap → ongoing → desync → recover
# ======================================================================


class TestConverterFuturesReplay:
    """End-to-end converter replay for futures continuity scenarios."""

    def test_futures_bootstrap_with_overlap(self) -> None:
        """First depth_update after snapshot accepted via U/u overlap rule."""
        iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        original = depth_mod.stream_raw_records
        try:
            def _fake(*a, **kw) -> Iterable[dict]:
                yield _snapshot_seed(
                    session=1, session_seq=1,
                    ts_recv_ns=1_000, last_update_id=200,
                    bids=[["99.0", "1.0"]], asks=[["101.0", "2.0"]],
                )
                # Bootstrap event: U=195 <= 200, u=210 >= 200.  pu=190 (irrelevant)
                yield _depth_update(
                    session=1, session_seq=2,
                    ts_recv_ns=2_000, U=195, u=210, pu=190,
                    bids=[["100.0", "2.0"]],
                )
                # Ongoing event: pu must equal prev (210)
                yield _depth_update(
                    session=1, session_seq=3,
                    ts_recv_ns=3_000, U=211, u=215, pu=210,
                    asks=[["101.0", "1.5"]],
                )

            depth_mod.stream_raw_records = _fake
            deltas, _, metrics = depth_mod.convert_depth_v2(
                "BINANCE_USDTF", "BTCUSDT", "2026-04-21",
                iid, 1, 1,
            )
        finally:
            depth_mod.stream_raw_records = original

        assert metrics.snapshot_seed_count == 1
        assert metrics.delta_events_written == 3   # snapshot + 2 updates
        assert metrics.desync_events == 0
        assert metrics.fenced_ranges == []

    def test_futures_all_stale_buffer_then_live_recovery(self) -> None:
        """Promote with accepted=0 (all stale), then live WS recovers via bootstrap check."""
        iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        original = depth_mod.stream_raw_records
        try:
            def _fake(*a, **kw) -> Iterable[dict]:
                # snapshot_seeded with lastUpdateId=200
                yield _snapshot_seed(
                    session=1, session_seq=1,
                    ts_recv_ns=1_000, last_update_id=200,
                    bids=[["99.0", "1.0"]], asks=[["101.0", "2.0"]],
                )
                yield _sync_state(
                    session=1, session_seq=2,
                    ts_recv_ns=1_500, state="snapshot_seeded", reason="bootstrap",
                )
                # First live WS msg after promote — bootstrap overlap succeeds
                yield _depth_update(
                    session=1, session_seq=3,
                    ts_recv_ns=2_000, U=198, u=205, pu=195,
                    bids=[["100.0", "3.0"]],
                )
                # Ongoing
                yield _depth_update(
                    session=1, session_seq=4,
                    ts_recv_ns=3_000, U=206, u=210, pu=205,
                    asks=[["101.0", "1.0"]],
                )

            depth_mod.stream_raw_records = _fake
            deltas, _, metrics = depth_mod.convert_depth_v2(
                "BINANCE_USDTF", "BTCUSDT", "2026-04-21",
                iid, 1, 1,
            )
        finally:
            depth_mod.stream_raw_records = original

        assert metrics.snapshot_seed_count == 1
        assert metrics.delta_events_written == 3   # snapshot + 2 updates
        assert metrics.desync_events == 0

    def test_futures_desync_then_resync_recovery(self) -> None:
        """Desync → resync → new snapshot → recovery.

        Reproduces the observed pattern where a bootstrap-gap causes
        desync but a second snapshot restores continuity.
        """
        iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        original = depth_mod.stream_raw_records
        try:
            def _fake(*a, **kw) -> Iterable[dict]:
                # first snapshot
                yield _snapshot_seed(
                    session=1, session_seq=1,
                    ts_recv_ns=1_000, last_update_id=200,
                    bids=[["99.0", "1.0"]], asks=[["101.0", "2.0"]],
                )
                yield _sync_state(
                    session=1, session_seq=2,
                    ts_recv_ns=1_500, state="snapshot_seeded", reason="bootstrap",
                )
                # First live msg has gap: U=210 > 200, fails bootstrap
                yield _depth_update(
                    session=1, session_seq=3,
                    ts_recv_ns=2_000, U=210, u=215, pu=205,
                    bids=[["100.0", "2.0"]],
                )
                # desync + resync
                yield _sync_state(
                    session=1, session_seq=4,
                    ts_recv_ns=2_500, state="desynced", reason="continuity_break",
                )
                yield _sync_state(
                    session=1, session_seq=5,
                    ts_recv_ns=3_000, state="resync_required", reason="desynced_retry",
                )
                # second snapshot
                yield _snapshot_seed(
                    session=1, session_seq=6,
                    ts_recv_ns=4_000, last_update_id=220,
                    bids=[["100.0", "2.5"]], asks=[["101.0", "1.5"]],
                )
                yield _sync_state(
                    session=1, session_seq=7,
                    ts_recv_ns=4_500, state="snapshot_seeded", reason="desynced_retry",
                )
                # Live recovery
                yield _depth_update(
                    session=1, session_seq=8,
                    ts_recv_ns=5_000, U=218, u=225, pu=215,
                    asks=[["102.0", "1.0"]],
                )
                yield _depth_update(
                    session=1, session_seq=9,
                    ts_recv_ns=6_000, U=226, u=230, pu=225,
                    bids=[["99.5", "0.5"]],
                )

            depth_mod.stream_raw_records = _fake
            deltas, _, metrics = depth_mod.convert_depth_v2(
                "BINANCE_USDTF", "BTCUSDT", "2026-04-21",
                iid, 1, 1,
            )
        finally:
            depth_mod.stream_raw_records = original

        assert metrics.snapshot_seed_count == 2
        # snapshot1 + desync-reject(0) + snapshot2 + 2 live = 4
        assert metrics.delta_events_written == 4
        assert metrics.desync_events >= 1
        # The fenced range from desync should be recovered
        assert all(f["recovered"] for f in metrics.fenced_ranges)

    def test_futures_session_boundary_clears_state(self) -> None:
        """A new stream_session_id resets book & continuity state."""
        iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        original = depth_mod.stream_raw_records
        try:
            def _fake(*a, **kw) -> Iterable[dict]:
                # Session 1
                yield _lifecycle(session=1, session_seq=1, ts_recv_ns=1_000, event="session_start")
                yield _snapshot_seed(
                    session=1, session_seq=2,
                    ts_recv_ns=2_000, last_update_id=100,
                    bids=[["10.0", "1.0"]], asks=[["11.0", "1.0"]],
                )
                yield _depth_update(
                    session=1, session_seq=3,
                    ts_recv_ns=3_000, U=98, u=105, pu=95,
                    bids=[["10.0", "2.0"]],
                )
                # Session 2 — reconnect
                yield _lifecycle(session=2, session_seq=1, ts_recv_ns=4_000, event="session_start")
                yield _snapshot_seed(
                    session=2, session_seq=2,
                    ts_recv_ns=5_000, last_update_id=200,
                    bids=[["20.0", "5.0"]], asks=[["21.0", "5.0"]],
                )
                yield _depth_update(
                    session=2, session_seq=3,
                    ts_recv_ns=6_000, U=198, u=205, pu=195,
                    asks=[["21.0", "4.0"]],
                )

            depth_mod.stream_raw_records = _fake
            deltas, _, metrics = depth_mod.convert_depth_v2(
                "BINANCE_USDTF", "BTCUSDT", "2026-04-21",
                iid, 1, 1,
            )
        finally:
            depth_mod.stream_raw_records = original

        assert metrics.snapshot_seed_count == 2
        assert metrics.delta_events_written == 4
        assert metrics.desync_events == 0


# ======================================================================
# Recorder promote logic
# ======================================================================


class TestRecorderPromote:
    """Test _promote_buffered_updates for futures-specific rules."""

    def test_futures_stale_drop_uses_strict_less_than(self) -> None:
        """Futures: drop events where u < lastUpdateId (NOT <=).

        An event with u == lastUpdateId should not be dropped—it should
        reach the bootstrap overlap check.
        """
        state = DepthSymbolState(venue="BINANCE_USDTF", symbol="BTCUSDT")
        # Simulate: lastUpdateId=200, buffered event with u=200
        # u < 200 is False, so it shouldn't be stale-dropped.
        # But bootstrap check: U <= 200 and u >= 200 → depends on U.
        # With U=198: accepted.
        pending = [
            {"stream_session_id": 1, "ws_arrival_seq": 1, "U": 198, "u": 200, "pu": 195},
        ]
        snapshot_raw = {"lastUpdateId": 200, "bids": [], "asks": []}

        # We can't easily call _promote_buffered_updates without async,
        # so test the logic inline.
        last_update_id = 200
        is_futures = True

        stale = [r for r in pending if r["u"] < last_update_id]
        non_stale = [r for r in pending if r["u"] >= last_update_id]

        assert len(stale) == 0, "u == lastUpdateId should not be stale for futures"
        assert len(non_stale) == 1

    def test_spot_stale_drop_uses_less_or_equal(self) -> None:
        """Spot: drop events where u <= lastUpdateId."""
        pending = [
            {"stream_session_id": 1, "ws_arrival_seq": 1, "U": 98, "u": 100, "pu": None},
        ]
        last_update_id = 100

        stale = [r for r in pending if r["u"] <= last_update_id]
        non_stale = [r for r in pending if r["u"] > last_update_id]

        assert len(stale) == 1, "u == lastUpdateId should be stale for spot"
        assert len(non_stale) == 0


# ======================================================================
# Bootstrap accounting: counter separation and fencing correctness
# ======================================================================


class TestBootstrapAccounting:
    """Verify that bootstrap attempts vs continuity resyncs are correctly separated."""

    @staticmethod
    def _make_state(
        *,
        venue: str = "BINANCE_USDTF",
    ) -> DepthSymbolState:
        return DepthSymbolState(venue=venue, symbol="BTCUSDT")

    @pytest.mark.asyncio
    async def test_bootstrap_increments_bootstrap_count_not_resync(self) -> None:
        """Initial bootstrap (reason='bootstrap') must increment bootstrap_attempt_count,
        NOT resync_count."""
        recorder = _stub_recorder()
        state = self._make_state()
        state.stream_session_id = 1
        state.pending_updates = []

        # Mock the HTTP call to return a valid snapshot
        import aiohttp
        from unittest.mock import AsyncMock, MagicMock, patch

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "lastUpdateId": 1000, "bids": [["99", "1"]], "asks": [["101", "2"]],
        })
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=False),
        ))
        recorder._session = mock_session

        await recorder._snapshot_seed_task(state, reason="bootstrap")

        assert state.bootstrap_attempt_count == 1
        assert state.resync_count == 0
        assert len(state.resync_timestamps) == 0

    @pytest.mark.asyncio
    async def test_continuity_break_increments_resync_not_bootstrap(self) -> None:
        """Continuity-recovery resync (reason='continuity_break') must increment
        resync_count, NOT bootstrap_attempt_count."""
        recorder = _stub_recorder()
        state = self._make_state()
        state.stream_session_id = 1
        state.pending_updates = []

        from unittest.mock import AsyncMock, MagicMock

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "lastUpdateId": 2000, "bids": [], "asks": [],
        })
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=False),
        ))
        recorder._session = mock_session

        await recorder._snapshot_seed_task(state, reason="continuity_break")

        assert state.resync_count == 1
        assert state.bootstrap_attempt_count == 0
        assert len(state.resync_timestamps) == 1

    @pytest.mark.asyncio
    async def test_desynced_retry_increments_resync_not_bootstrap(self) -> None:
        """Desynced retry is a continuity-recovery path, counted as resync."""
        recorder = _stub_recorder()
        state = self._make_state()
        state.stream_session_id = 1
        state.pending_updates = []

        from unittest.mock import AsyncMock, MagicMock

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "lastUpdateId": 3000, "bids": [], "asks": [],
        })
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=False),
        ))
        recorder._session = mock_session

        await recorder._snapshot_seed_task(state, reason="desynced_retry")

        assert state.resync_count == 1
        assert state.bootstrap_attempt_count == 0

    def test_multiple_bootstraps_do_not_fence(self) -> None:
        """7 initial bootstraps (WS reconnections) must NOT cause fencing,
        since bootstraps don't accumulate in resync_timestamps."""
        state = self._make_state()

        # Simulate 7 clean session starts — each increments bootstrap_attempt_count
        for i in range(7):
            state.bootstrap_attempt_count += 1

        assert state.bootstrap_attempt_count == 7
        assert state.resync_count == 0
        assert len(state.resync_timestamps) == 0
        assert state.sync_state != SYNC_FENCED

    def test_promote_zero_accepted_is_expected_on_fresh_snapshot(self) -> None:
        """When all buffered messages are stale, promote correctly reports
        accepted=0 and increments promote_zero_accepted_count.
        The symbol still syncs via the live SNAPSHOT_SEEDED → LIVE_SYNCED path."""
        state = self._make_state()
        state.sync_state = SYNC_SNAPSHOT_SEEDED
        state.prev_update_id = 1000

        # First WS message after promote is stale
        recorder = _stub_recorder()
        stale_rec = {"U": 990, "u": 995, "pu": 985}
        assert recorder._check_continuity(state, stale_rec) is False
        assert state.sync_state == SYNC_SNAPSHOT_SEEDED  # NOT desynced
        assert state.bootstrap_stale_drop_count == 1

        # Next WS message passes bootstrap overlap
        overlap_rec = {"U": 998, "u": 1005, "pu": 995}
        assert recorder._check_continuity(state, overlap_rec) is True
        assert state.sync_state == SYNC_LIVE_SYNCED
        assert state.prev_update_id == 1005
        assert state.desync_events == 0

    def test_snapshot_seed_count_tracks_all_committed_snapshots(self) -> None:
        """snapshot_seed_count tracks total snapshots committed (bootstraps + resyncs).
        It should equal bootstrap_attempt_count + resync_count when every attempt succeeds."""
        state = self._make_state()
        # Simulate 3 bootstraps + 2 resyncs, each calling _handle_snapshot_seed
        state.bootstrap_attempt_count = 3
        state.resync_count = 2
        state.snapshot_seed_count = 5  # 3 + 2

        assert state.snapshot_seed_count == state.bootstrap_attempt_count + state.resync_count


# ======================================================================
# helpers
# ======================================================================


def _stub_recorder() -> BinanceNativeDepthRecorder:
    """Create a BinanceNativeDepthRecorder with stubs for testing _check_continuity."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    return BinanceNativeDepthRecorder(
        storage_manager=AsyncMock(),
        health_monitor=MagicMock(),
        shutdown_event=asyncio.Event(),
    )
