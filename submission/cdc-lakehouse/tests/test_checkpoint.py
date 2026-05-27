"""Checkpoint persistence tests."""

from pipeline.checkpoint import load_checkpoint, save_checkpoint


def test_checkpoint_roundtrip(tmp_path):
    cp = tmp_path / "checkpoint.json"
    assert load_checkpoint(cp) == 0
    save_checkpoint(42, cp)
    assert load_checkpoint(cp) == 42
