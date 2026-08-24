"""Tests for the Decimal-based canonicalization used by
``compare_book_checkpoints_streaming()``'s checkpoint hash/comparison (issue
#20 Phase 5 correction).

Discovered during the ADAUSDT Tier-2 local semantic run: v1's fixed-point
replay schema formats prices/quantities at the instrument's exact required
scale (e.g. 4 decimals, from Binance's PRICE_FILTER.tickSize), while legacy
v0 preserves the literal Binance wire-format string (which always carries 8
fractional digits for spot pairs, regardless of tick size). Both represent
the exact same numeric value, but the book-checkpoint comparison's literal
string equality treated the trailing-zero-padding difference as a semantic
mismatch. This file proves the canonicalization fix is value-aware (not
merely string-aware) without ever rounding/quantizing genuinely different
values into equality, and without a float intermediate.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from validation.catalog_compare import (
    _canonical_decimal_str,
    _canonical_book_state,
    _book_state_hash,
)


@pytest.mark.parametrize(
    "value_str,expected",
    [
        ("0.17130000", "0.1713"),
        ("0.1713", "0.1713"),
        ("139377.00000000", "139377"),
        ("139377.0", "139377"),
        ("0.00000000", "0"),
        ("0", "0"),
        ("100.00", "100"),
        ("0.00010000", "0.0001"),
    ],
)
def test_canonical_decimal_str_strips_insignificant_padding(value_str, expected):
    assert _canonical_decimal_str(value_str) == expected


def test_canonical_decimal_str_never_uses_scientific_notation():
    # Decimal.normalize() alone would produce "1E+2" for "100.00" — the
    # canonicalization must always return a plain fixed-point string.
    result = _canonical_decimal_str("100.00")
    assert "E" not in result and "e" not in result
    assert Decimal(result) == Decimal("100.00")


@pytest.mark.parametrize(
    "a,b",
    [
        ("0.17130000", "0.1713"),
        ("139377.00000000", "139377.0"),
        ("100.00", "100"),
    ],
)
def test_canonical_decimal_str_treats_equal_values_as_equal(a, b):
    assert _canonical_decimal_str(a) == _canonical_decimal_str(b)


@pytest.mark.parametrize(
    "a,b",
    [
        ("0.17130000", "0.17131"),  # genuinely different value, same digit count
        ("0.1713", "0.17131"),      # genuinely different value, different digit count
        ("100.00", "100.01"),
        ("0", "0.00001"),
    ],
)
def test_canonical_decimal_str_never_collapses_genuinely_different_values(a, b):
    assert _canonical_decimal_str(a) != _canonical_decimal_str(b)
    assert Decimal(a) != Decimal(b)  # sanity: the test fixtures really are different


def test_canonical_decimal_str_no_float_intermediate():
    """A value not exactly representable as a binary float must still
    canonicalize/compare exactly — a float intermediate would risk
    collapsing or corrupting values silently."""
    a = "9007199254740993.12345678"  # > 2**53, not exactly float64-representable
    b = "9007199254740993.1234567800"
    assert _canonical_decimal_str(a) == _canonical_decimal_str(b)
    # And a value one ULP away in the fractional part must remain distinct.
    c = "9007199254740993.12345679"
    assert _canonical_decimal_str(a) != _canonical_decimal_str(c)


def test_canonical_book_state_normalizes_all_levels_both_sides():
    book = {
        "bids": [["0.17130000", "139377.00000000"], ["0.17120000", "500.00000000"]],
        "asks": [["0.17140000", "12261.10000000"]],
    }
    canonical = _canonical_book_state(book)
    assert canonical == {
        "bids": [["0.1713", "139377"], ["0.1712", "500"]],
        "asks": [["0.1714", "12261.1"]],
    }


def test_canonical_book_state_preserves_level_order_and_count():
    book = {
        "bids": [["1.00", "1.00"], ["0.99", "2.00"], ["0.98", "3.00"]],
        "asks": [],
    }
    canonical = _canonical_book_state(book)
    assert len(canonical["bids"]) == 3
    assert [p for p, _ in canonical["bids"]] == ["1", "0.99", "0.98"]


def test_book_state_hash_equal_for_differently_padded_equal_books():
    book_a = {"bids": [["0.17130000", "139377.00000000"]], "asks": [["0.17140000", "12261.10000000"]]}
    book_b = {"bids": [["0.1713", "139377.0"]], "asks": [["0.1714", "12261.1"]]}
    hash_a = _book_state_hash(_canonical_book_state(book_a))
    hash_b = _book_state_hash(_canonical_book_state(book_b))
    assert hash_a == hash_b


def test_book_state_hash_differs_for_genuinely_different_books():
    book_a = {"bids": [["0.17130000", "139377.00000000"]], "asks": []}
    book_b = {"bids": [["0.17131000", "139377.00000000"]], "asks": []}
    hash_a = _book_state_hash(_canonical_book_state(book_a))
    hash_b = _book_state_hash(_canonical_book_state(book_b))
    assert hash_a != hash_b


def test_book_state_hash_differs_when_level_count_differs():
    book_a = {"bids": [["1.00", "1.00"]], "asks": []}
    book_b = {"bids": [["1.00", "1.00"], ["0.99", "1.00"]], "asks": []}
    hash_a = _book_state_hash(_canonical_book_state(book_a))
    hash_b = _book_state_hash(_canonical_book_state(book_b))
    assert hash_a != hash_b
