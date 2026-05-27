#!/usr/bin/env python3
"""
Compile expected schema_fingerprint.json FROM the source-team contract.

Run when the source team publishes a new schema_contract_spec.json — not from live DB.
Data engineering does not bootstrap truth from production; the contract is authoritative.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.schema_check import (
    expected_fingerprint_from_contract,
    export_contract_file,
    save_expected_fingerprint,
)


def main() -> int:
    fp = expected_fingerprint_from_contract()
    save_expected_fingerprint(fp)
    export_contract_file()
    print(
        f"Compiled expected fingerprint for {len(fp)} tables from schema_contract_spec.json."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
