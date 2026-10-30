"""Root conftest: make this folder importable (``source``, ``pipeline``, ``demo``)."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
