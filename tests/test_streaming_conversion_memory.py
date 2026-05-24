from __future__ import annotations

from dataclasses import asdict
from typing import Iterable

from nautilus_trader.model.identifiers import InstrumentId

import converter.depth_phase2 as depth_mod
from converter.spool import RawRecordSpool
import converter.trades as trade_mod


def _trade(*, session: int, seq: int, ts_recv_ns: int, price: str) -> dict:
    return {
        "record_type": "trade",
        "trade_stream_session_id": session,
        "trade_session_seq": seq,
        "ts_recv_ns": ts_recv_ns,
        "ts_trade_ms": ts_recv_ns // 1_000_000,
        "price": price,
        "quantity": "1.0",
        "is_buyer_maker": False,
        "exchange_trade_id": seq,
    }


def _snapshot(*, session: int, seq: int, ts_recv_ns: int, last_update_id: int) -> dict:
    return {
        "record_type": "snapshot_seed",
        "stream_session_id": session,
        "session_seq": seq,
        "ts_recv_ns": ts_recv_ns,
        "lastUpdateId": last_update_id,
        "payload": {
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.0"]],
        },
    }


def _depth_update(*, session: int, seq: int, ts_recv_ns: int, u: int) -> dict:
    return {
        "record_type": "depth_update",
        "stream_session_id": session,
        "session_seq": seq,
        "ts_recv_ns": ts_recv_ns,
        "U": u,
        "u": u,
        "payload": {
            "bids": [["100.0", "2.0"]],
            "asks": [["101.0", "1.0"]],
        },
    }


def test_trade_wrapper_matches_streaming_output(monkeypatch) -> None:
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    records = [
        _trade(session=1, seq=2, ts_recv_ns=2_000_000_000, price="101.0"),
        {"record_type": "trade_stream_lifecycle", "trade_stream_session_id": 1},
        _trade(session=1, seq=1, ts_recv_ns=1_000_000_000, price="100.0"),
    ]

    def fake_stream(*args, **kwargs) -> Iterable[dict]:
        yield from records

    monkeypatch.setattr(trade_mod, "stream_raw_records", fake_stream)

    wrapper_ticks, wrapper_bad, wrapper_first, wrapper_last, wrapper_diag = (
        trade_mod.convert_trades_with_diagnostics(
            "BINANCE_SPOT",
            "BTCUSDT",
            "2026-04-21",
            iid,
            1,
            1,
        )
    )
    streaming_ticks = []
    streaming_bad, streaming_first, streaming_last, streaming_diag = (
        trade_mod.convert_trades_streaming(
            "BINANCE_SPOT",
            "BTCUSDT",
            "2026-04-21",
            iid,
            1,
            1,
            on_ticks_batch=streaming_ticks.extend,
            batch_size=1,
        )
    )

    assert streaming_ticks == wrapper_ticks
    assert (streaming_bad, streaming_first, streaming_last, streaming_diag) == (
        wrapper_bad,
        wrapper_first,
        wrapper_last,
        wrapper_diag,
    )


def test_depth_wrapper_matches_streaming_output(monkeypatch) -> None:
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    records_by_date = {
        "2026-04-21": [
            _depth_update(session=1, seq=2, ts_recv_ns=2_000, u=101),
            _snapshot(session=1, seq=1, ts_recv_ns=1_000, last_update_id=100),
        ],
    }

    def fake_stream(venue: str, symbol: str, channel: str, date_str: str):
        yield from records_by_date.get(date_str, [])

    monkeypatch.setattr(depth_mod, "stream_raw_records", fake_stream)

    wrapper_deltas, wrapper_depth10s, wrapper_metrics = depth_mod.convert_depth_v2(
        "BINANCE_SPOT",
        "BTCUSDT",
        "2026-04-21",
        iid,
        1,
        1,
        emit_depth10=True,
        depth10_interval_sec=0.0,
    )
    streaming_deltas = []
    streaming_depth10s = []
    streaming_metrics = depth_mod.convert_depth_v2_streaming(
        "BINANCE_SPOT",
        "BTCUSDT",
        "2026-04-21",
        iid,
        1,
        1,
        on_deltas_batch=streaming_deltas.extend,
        on_depth10_batch=streaming_depth10s.extend,
        batch_size=1,
        emit_depth10=True,
        depth10_interval_sec=0.0,
    )

    assert streaming_deltas == wrapper_deltas
    assert streaming_depth10s == wrapper_depth10s
    assert asdict(streaming_metrics) == asdict(wrapper_metrics)


def test_converter_spool_uses_configured_temp_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("CRYPTO_RECORDER_CONVERTER_TEMP_DIR", str(tmp_path))

    with RawRecordSpool(prefix="cryptorecorder-test-") as spool:
        path = spool.path
        assert path.parent == tmp_path
        assert path.exists()

    assert not path.exists()
