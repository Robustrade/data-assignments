"""Root conftest — adds submission/ to sys.path so tests can import source/pipeline."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
