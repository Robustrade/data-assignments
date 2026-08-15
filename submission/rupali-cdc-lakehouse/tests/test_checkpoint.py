from src.cdc.checkpoint import Checkpoint


def test_checkpoint_starts_at_zero(tmp_path):
    checkpoint = Checkpoint(tmp_path / "checkpoint.json")

    assert checkpoint.get() == 0


def test_checkpoint_can_be_saved_and_loaded(tmp_path):
    checkpoint_file = tmp_path / "checkpoint.json"

    checkpoint = Checkpoint(checkpoint_file)

    checkpoint.save(5)

    assert checkpoint.get() == 5


def test_checkpoint_survives_restart(tmp_path):
    checkpoint_file = tmp_path / "checkpoint.json"

    checkpoint = Checkpoint(checkpoint_file)

    checkpoint.save(10)

    restarted_checkpoint = Checkpoint(checkpoint_file)

    assert restarted_checkpoint.get() == 10