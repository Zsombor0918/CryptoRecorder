"""Real-Parquet correctness boundaries for the bounded catalog reader.

The reader deliberately uses Nautilus's private
``ParquetDataCatalog._query_files()`` under an exact 1.225.0 compatibility
pin, then bypasses the memory-unbounded global DataFusion sort query.
These tests exercise real Nautilus-written Parquet files and prove the
supported layout is accepted while every unsupported ordering/identity
layout fails before the first object is yielded.
"""
from __future__ import annotations

import gc
import shutil
import sys
import weakref
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from nautilus_trader.model.data import OrderBookDelta, TradeTick
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer

from converter.instruments import build_instruments
from validation import catalog_compare
from validation.catalog_compare import (
    CatalogFileLayoutError,
    CatalogStreamingCompatibilityError,
    _iter_catalog_files_bounded,
    compare_trade_ticks_exhaustive,
    iter_order_book_deltas_bounded,
    iter_trade_ticks_bounded,
)


def _instrument(symbol: str = "ADAUSDT"):
    return build_instruments("BINANCE_SPOT", [symbol], {})[0]


def _tick(
    instrument,
    *,
    trade_id: str,
    ts: int,
    price: str = "1.0000",
) -> TradeTick:
    return TradeTick(
        instrument_id=instrument.id,
        price=Price.from_str(price),
        size=Quantity.from_str("1"),
        aggressor_side=AggressorSide.BUYER,
        trade_id=TradeId(trade_id),
        ts_event=ts,
        ts_init=ts,
    )


def _write_trade_catalog(
    root: Path,
    instrument,
    chunks: list[list[TradeTick]],
    *,
    max_rows_per_group: int = 5_000,
    skip_disjoint_check: bool = False,
) -> ParquetDataCatalog:
    catalog = ParquetDataCatalog(str(root), max_rows_per_group=max_rows_per_group)
    catalog.write_data([instrument])
    for chunk in chunks:
        catalog.write_data(chunk, skip_disjoint_check=skip_disjoint_check)
    return catalog


def _trade_files(root: Path, instrument) -> list[Path]:
    return sorted((root / "data" / "trade_tick" / str(instrument.id)).glob("*.parquet"))


def _real_delta(instrument_id, ts: int, *, is_last: bool, price: str) -> OrderBookDelta:
    from nautilus_trader.model.data import BookOrder

    return OrderBookDelta(
        instrument_id=instrument_id,
        action=BookAction.ADD,
        order=BookOrder(
            side=OrderSide.BUY,
            price=Price.from_str(price),
            size=Quantity.from_str("1"),
            order_id=1,
        ),
        flags=128 if is_last else 0,
        sequence=0,
        ts_event=ts,
        ts_init=ts,
    )


def test_multiple_non_overlapping_files_use_actual_time_order(tmp_path: Path) -> None:
    """Creation/glob order is irrelevant; validated actual ranges are canonical."""
    instrument = _instrument()
    chunks = [
        [_tick(instrument, trade_id="late-0", ts=100), _tick(instrument, trade_id="late-1", ts=101)],
        [_tick(instrument, trade_id="early-0", ts=0), _tick(instrument, trade_id="early-1", ts=1)],
        [_tick(instrument, trade_id="middle-0", ts=50), _tick(instrument, trade_id="middle-1", ts=51)],
    ]
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        chunks,
        max_rows_per_group=1,
    )

    streamed = list(
        _iter_catalog_files_bounded(
            catalog,
            TradeTick,
            [str(instrument.id)],
            0,
            200,
            batch_size=1,
        )
    )

    assert [int(item.ts_init) for item in streamed] == [0, 1, 50, 51, 100, 101]
    assert [str(item.trade_id) for item in streamed] == [
        "early-0",
        "early-1",
        "middle-0",
        "middle-1",
        "late-0",
        "late-1",
    ]


def test_overlapping_files_fail_before_first_yield(tmp_path: Path) -> None:
    instrument = _instrument()
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        [
            [_tick(instrument, trade_id="a", ts=0), _tick(instrument, trade_id="b", ts=100)],
            [_tick(instrument, trade_id="c", ts=50), _tick(instrument, trade_id="d", ts=150)],
        ],
        skip_disjoint_check=True,
    )

    stream = _iter_catalog_files_bounded(
        catalog,
        TradeTick,
        [str(instrument.id)],
        0,
        200,
    )
    with pytest.raises(CatalogFileLayoutError, match="overlapping ts_init ranges"):
        next(stream)


def test_equal_ts_init_at_file_boundary_fails_closed_before_yield(tmp_path: Path) -> None:
    """Closed intervals sharing an endpoint have no supported cross-file tie order."""
    instrument = _instrument()
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        [
            [_tick(instrument, trade_id="a", ts=0), _tick(instrument, trade_id="b", ts=100)],
            [_tick(instrument, trade_id="c", ts=100), _tick(instrument, trade_id="d", ts=200)],
        ],
        skip_disjoint_check=True,
    )

    stream = _iter_catalog_files_bounded(
        catalog,
        TradeTick,
        [str(instrument.id)],
        0,
        200,
    )
    with pytest.raises(CatalogFileLayoutError, match="equal ts_init file boundary"):
        next(stream)


def test_equal_ts_init_inside_one_file_preserves_physical_tie_order(tmp_path: Path) -> None:
    instrument = _instrument()
    ticks = [
        _tick(instrument, trade_id="first", ts=100),
        _tick(instrument, trade_id="second", ts=100),
        _tick(instrument, trade_id="third", ts=100),
    ]
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        [ticks],
        max_rows_per_group=1,
    )

    streamed = list(
        _iter_catalog_files_bounded(
            catalog,
            TradeTick,
            [str(instrument.id)],
            100,
            100,
            batch_size=1,
        )
    )
    assert [str(item.trade_id) for item in streamed] == ["first", "second", "third"]


def test_internally_unsorted_later_file_fails_before_first_yield(tmp_path: Path) -> None:
    instrument = _instrument()
    catalog_root = tmp_path / "catalog"
    catalog = _write_trade_catalog(
        catalog_root,
        instrument,
        [
            [_tick(instrument, trade_id="a", ts=0), _tick(instrument, trade_id="b", ts=1)],
            [
                _tick(instrument, trade_id="c", ts=10),
                _tick(instrument, trade_id="d", ts=11),
                _tick(instrument, trade_id="e", ts=12),
                _tick(instrument, trade_id="f", ts=13),
            ],
        ],
        max_rows_per_group=2,
    )
    later_file = _trade_files(catalog_root, instrument)[1]
    table = pq.read_table(later_file)
    # Keep the filename endpoints truthful while making the interior order
    # decrease across a row-group boundary: 10, 12, 11, 13.
    pq.write_table(table.take(pa.array([0, 2, 1, 3])), later_file, row_group_size=2)

    stream = _iter_catalog_files_bounded(
        catalog,
        TradeTick,
        [str(instrument.id)],
        0,
        20,
        batch_size=2,
    )
    with pytest.raises(CatalogFileLayoutError, match="internally unsorted"):
        next(stream)


def test_multiple_instruments_return_only_requested_instrument(tmp_path: Path) -> None:
    ada = _instrument("ADAUSDT")
    btc = _instrument("BTCUSDT")
    root = tmp_path / "catalog"
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([ada, btc])
    catalog.write_data([_tick(ada, trade_id="ada", ts=1)])
    catalog.write_data([_tick(btc, trade_id="btc", ts=2)])

    streamed = list(
        _iter_catalog_files_bounded(catalog, TradeTick, [str(ada.id)], 0, 10)
    )
    assert [str(item.instrument_id) for item in streamed] == [str(ada.id)]
    assert [str(item.trade_id) for item in streamed] == ["ada"]


def test_misplaced_wrong_instrument_file_fails_closed(tmp_path: Path) -> None:
    ada = _instrument("ADAUSDT")
    btc = _instrument("BTCUSDT")
    root = tmp_path / "catalog"
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([ada, btc])
    catalog.write_data([_tick(ada, trade_id="ada", ts=1)])
    catalog.write_data([_tick(btc, trade_id="btc", ts=10)])

    btc_file = _trade_files(root, btc)[0]
    misplaced = root / "data" / "trade_tick" / str(ada.id) / btc_file.name
    shutil.copy2(btc_file, misplaced)

    stream = _iter_catalog_files_bounded(catalog, TradeTick, [str(ada.id)], 0, 20)
    with pytest.raises(CatalogFileLayoutError, match="not requested instrument"):
        next(stream)


def test_misplaced_wrong_data_class_file_fails_closed(tmp_path: Path) -> None:
    instrument = _instrument()
    root = tmp_path / "catalog"
    catalog = _write_trade_catalog(
        root,
        instrument,
        [[_tick(instrument, trade_id="trade", ts=10)]],
    )
    # Include an earlier, valid requested-class file. Whole-selection
    # preflight must still reject the later wrong-class file before this
    # valid delta can be yielded.
    catalog.write_data(
        [_real_delta(instrument.id, 1, is_last=True, price="1.0000")]
    )
    trade_file = _trade_files(root, instrument)[0]
    wrong_class_dir = root / "data" / "order_book_deltas" / str(instrument.id)
    wrong_class_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(trade_file, wrong_class_dir / trade_file.name)

    stream = _iter_catalog_files_bounded(
        catalog,
        OrderBookDelta,
        [str(instrument.id)],
        0,
        20,
    )
    with pytest.raises(
        CatalogFileLayoutError,
        match="physical Arrow schema does not match requested data class OrderBookDelta",
    ):
        next(stream)


def test_exact_inclusive_start_and_end_rows_match_catalog_contract(tmp_path: Path) -> None:
    instrument = _instrument()
    root = tmp_path / "catalog"
    catalog = _write_trade_catalog(
        root,
        instrument,
        [[
            _tick(instrument, trade_id="before", ts=99),
            _tick(instrument, trade_id="start", ts=100),
            _tick(instrument, trade_id="middle", ts=150),
            _tick(instrument, trade_id="end", ts=200),
            _tick(instrument, trade_id="after", ts=201),
        ]],
    )

    inclusive = list(
        _iter_catalog_files_bounded(
            catalog,
            TradeTick,
            [str(instrument.id)],
            100,
            200,
        )
    )
    assert [str(item.trade_id) for item in inclusive] == ["start", "middle", "end"]

    # Public wrappers keep their historical half-open [start, end) contract
    # by converting it to the private reader's inclusive [start, end - 1].
    half_open = list(iter_trade_ticks_bounded(root, str(instrument.id), 100, 201))
    assert [str(item.trade_id) for item in half_open] == ["start", "middle", "end"]


def test_decoded_batch_reordering_fails_before_first_yield(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = _instrument()
    root = tmp_path / "catalog"
    catalog = _write_trade_catalog(
        root,
        instrument,
        [[
            _tick(instrument, trade_id="first", ts=1),
            _tick(instrument, trade_id="second", ts=2),
            _tick(instrument, trade_id="third", ts=3),
        ]],
    )
    original_deserialize = ArrowSerializer.deserialize

    def reordered_deserialize(*, data_cls, batch):
        decoded = original_deserialize(data_cls=data_cls, batch=batch)
        if isinstance(batch, pa.RecordBatch) and batch.num_rows == 3:
            return [decoded[0], decoded[2], decoded[1]]
        return decoded

    monkeypatch.setattr(
        ArrowSerializer,
        "deserialize",
        staticmethod(reordered_deserialize),
    )
    stream = _iter_catalog_files_bounded(
        catalog,
        TradeTick,
        [str(instrument.id)],
        0,
        10,
        batch_size=3,
    )
    with pytest.raises(CatalogFileLayoutError, match="changed Arrow row order"):
        next(stream)


def test_many_files_keep_open_count_and_batch_lifetime_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = _instrument()
    root = tmp_path / "catalog"
    chunks: list[list[TradeTick]] = []
    next_ts = 0
    for file_index in range(40):
        chunk = [
            _tick(instrument, trade_id=f"{file_index}-{row}", ts=next_ts + row)
            for row in range(25)
        ]
        chunks.append(chunk)
        next_ts += len(chunk)
    catalog = _write_trade_catalog(root, instrument, chunks, max_rows_per_group=9)

    real_parquet_file = catalog_compare.pq.ParquetFile
    active_files = 0
    max_active_files = 0

    class TrackingParquetFile:
        def __init__(self, *args, **kwargs):
            nonlocal active_files, max_active_files
            self._inner = real_parquet_file(*args, **kwargs)
            self._closed = False
            active_files += 1
            max_active_files = max(max_active_files, active_files)

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def close(self, *args, **kwargs):
            nonlocal active_files
            if not self._closed:
                self._inner.close(*args, **kwargs)
                self._closed = True
                active_files -= 1

    monkeypatch.setattr(catalog_compare.pq, "ParquetFile", TrackingParquetFile)

    original_deserialize = ArrowSerializer.deserialize
    arrow_batch_refs: list[weakref.ReferenceType] = []
    observed_decode_sizes: list[int] = []
    pyo3_list_refcounts_after_conversion: list[int] = []

    def tracking_deserialize(*, data_cls, batch):
        if isinstance(batch, pa.RecordBatch) and batch.num_rows:
            gc.collect()
            assert all(
                reference() is None for reference in arrow_batch_refs
            ), "a previous Arrow batch remained live at the next decode"
        result = original_deserialize(data_cls=data_cls, batch=batch)
        if isinstance(batch, pa.RecordBatch) and batch.num_rows:
            arrow_batch_refs.append(weakref.ref(batch))
            observed_decode_sizes.append(batch.num_rows)
        return result

    monkeypatch.setattr(
        ArrowSerializer,
        "deserialize",
        staticmethod(tracking_deserialize),
    )
    real_cython_trade_cls = catalog_compare._pyo3_to_cython_cls(TradeTick)

    class TrackingCythonTrade:
        @staticmethod
        def from_pyo3_list(values):
            converted = real_cython_trade_cls.from_pyo3_list(values)
            # At this point the only expected references are the caller's
            # current-batch local, this argument, and getrefcount's temporary
            # argument. A higher count would mean conversion retained the
            # supplied batch list.
            pyo3_list_refcounts_after_conversion.append(sys.getrefcount(values))
            return converted

    real_cython_resolver = catalog_compare._pyo3_to_cython_cls
    monkeypatch.setattr(
        catalog_compare,
        "_pyo3_to_cython_cls",
        lambda data_cls: (
            TrackingCythonTrade
            if data_cls is TradeTick
            else real_cython_resolver(data_cls)
        ),
    )

    count = 0
    for tick in _iter_catalog_files_bounded(
        catalog,
        TradeTick,
        [str(instrument.id)],
        0,
        next_ts,
        batch_size=7,
    ):
        count += 1
        del tick
    gc.collect()

    assert count == 1_000
    assert max_active_files == 1
    assert active_files == 0
    assert observed_decode_sizes
    assert max(observed_decode_sizes) <= 7
    assert all(ref() is None for ref in arrow_batch_refs)
    # ArrowSerializer.deserialize() receives only the current RecordBatch.
    # from_pyo3_list() converts that current returned list, and no conversion
    # or iterator state retains it after the batch has been exhausted.
    assert pyo3_list_refcounts_after_conversion
    assert max(pyo3_list_refcounts_after_conversion) == 3


@pytest.mark.parametrize("mutation", ["changed", "missing", "extra", "reordered"])
def test_real_parquet_near_end_faults_are_exhaustively_detected(
    tmp_path: Path,
    mutation: str,
) -> None:
    instrument = _instrument()
    reference_ticks = [
        _tick(
            instrument,
            trade_id=str(index),
            # The final pair deliberately shares ts_init so "reordered" is
            # still internally sorted and reaches the positional comparator.
            ts=index if index < 98 else 99,
        )
        for index in range(100)
    ]
    candidate_ticks = list(reference_ticks)

    if mutation == "changed":
        candidate_ticks[98] = _tick(
            instrument,
            trade_id="98",
            ts=99,
            price="2.0000",
        )
    elif mutation == "missing":
        candidate_ticks.pop(98)
    elif mutation == "extra":
        candidate_ticks.append(_tick(instrument, trade_id="extra", ts=100))
    elif mutation == "reordered":
        candidate_ticks[-2:] = reversed(candidate_ticks[-2:])

    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _write_trade_catalog(old_root, instrument, [reference_ticks])
    _write_trade_catalog(new_root, instrument, [candidate_ticks])

    comparison = compare_trade_ticks_exhaustive(
        iter_trade_ticks_bounded(old_root, str(instrument.id), 0, 101),
        iter_trade_ticks_bounded(new_root, str(instrument.id), 0, 101),
    )
    assert comparison["passed"] is False
    if mutation in {"changed", "reordered"}:
        assert comparison["position_mismatches"][0]["position"] >= 98
    elif mutation == "missing":
        assert comparison["first_length_divergence_position"] == 99
    else:
        assert comparison["first_length_divergence_position"] == 100


def test_bounded_deltas_reader_matches_full_query(tmp_path: Path) -> None:
    instrument = _instrument()
    root = tmp_path / "delta_catalog"
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([instrument])
    deltas = []
    ts = 0
    for _group in range(5):
        for index in range(3):
            ts += 1
            deltas.append(
                _real_delta(
                    instrument.id,
                    ts,
                    is_last=index == 2,
                    price="1.0",
                )
            )
    catalog.write_data(deltas)

    reference = catalog.order_book_deltas(
        instrument_ids=[str(instrument.id)],
        batched=False,
        start=0,
        end=ts,
    )
    streamed = list(
        iter_order_book_deltas_bounded(root, str(instrument.id), 0, ts + 1)
    )

    assert len(streamed) == len(reference) == 15
    assert [int(item.ts_init) for item in streamed] == [
        int(item.ts_init) for item in reference
    ]


def test_unverified_nautilus_version_fails_without_datafusion_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = _instrument()
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        [[_tick(instrument, trade_id="one", ts=1)]],
    )
    backend_called = False

    def forbidden_backend(*args, **kwargs):
        nonlocal backend_called
        backend_called = True
        raise AssertionError("DataFusion fallback must never run")

    monkeypatch.setattr(
        catalog_compare.importlib.metadata,
        "version",
        lambda _name: "1.226.0",
    )
    monkeypatch.setattr(ParquetDataCatalog, "backend_session", forbidden_backend)

    with pytest.raises(
        CatalogStreamingCompatibilityError,
        match="Refusing to use an unverified private API",
    ):
        list(
            _iter_catalog_files_bounded(
                catalog,
                TradeTick,
                [str(instrument.id)],
                0,
                2,
            )
        )
    assert backend_called is False


def test_missing_private_selector_fails_without_datafusion_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = _instrument()
    catalog = _write_trade_catalog(
        tmp_path / "catalog",
        instrument,
        [[_tick(instrument, trade_id="one", ts=1)]],
    )
    backend_called = False

    def forbidden_backend(*args, **kwargs):
        nonlocal backend_called
        backend_called = True
        raise AssertionError("DataFusion fallback must never run")

    monkeypatch.setattr(ParquetDataCatalog, "_query_files", None)
    monkeypatch.setattr(ParquetDataCatalog, "backend_session", forbidden_backend)

    with pytest.raises(CatalogStreamingCompatibilityError, match="does not expose"):
        list(
            _iter_catalog_files_bounded(
                catalog,
                TradeTick,
                [str(instrument.id)],
                0,
                2,
            )
        )
    assert backend_called is False


def test_multi_instrument_direct_request_is_rejected(tmp_path: Path) -> None:
    ada = _instrument("ADAUSDT")
    btc = _instrument("BTCUSDT")
    root = tmp_path / "catalog"
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([ada, btc])
    catalog.write_data([_tick(ada, trade_id="ada", ts=1)])
    catalog.write_data([_tick(btc, trade_id="btc", ts=2)])

    with pytest.raises(CatalogFileLayoutError, match="exactly one"):
        list(
            _iter_catalog_files_bounded(
                catalog,
                TradeTick,
                [str(ada.id), str(btc.id)],
                0,
                10,
            )
        )
