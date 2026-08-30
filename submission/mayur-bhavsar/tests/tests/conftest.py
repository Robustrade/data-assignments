"""Shared fixtures.

Building the warehouse costs a few seconds, so the read-only warehouse tests
share one session-scoped reference run. Tests that need to interfere with
ingestion (crash, replay, out-of-order, schema drift) get their own throwaway
platform so they cannot contaminate each other.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from cdclake import source as source_mod
from cdclake.pipeline import Platform, build_platform, run_reference_pipeline


@pytest.fixture(scope="session")
def session_tmp() -> Path:
    path = Path(tempfile.mkdtemp(prefix="cdclake-tests-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def reference_platform(session_tmp: Path) -> Platform:
    """Seed + full workload + ingest + warehouse build, done once."""
    platform = run_reference_pipeline(session_tmp / "reference")
    yield platform
    platform.close()


@pytest.fixture
def workspace_root(tmp_path: Path) -> Path:
    return tmp_path / "workspace"


@pytest.fixture
def seeded_platform(workspace_root: Path) -> Platform:
    """A fresh source with seed data, nothing ingested yet."""
    platform = build_platform(workspace_root, fresh=True, seed=True)
    yield platform
    platform.close()


@pytest.fixture
def empty_platform(workspace_root: Path) -> Platform:
    """A fresh, empty source. No rows, no events."""
    platform = build_platform(workspace_root, fresh=True, seed=False)
    yield platform
    platform.close()


@pytest.fixture
def workload():
    return source_mod
