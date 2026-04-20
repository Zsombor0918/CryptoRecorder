#!/usr/bin/env python3
"""Preferred CLI entrypoint for day-based conversion.

This wrapper keeps backward compatibility with the legacy filename
`convert_yesterday.py` while exposing a less misleading command name.
"""

from convert_yesterday import main as convert_main


if __name__ == "__main__":
    raise SystemExit(convert_main(legacy_entrypoint=False))
