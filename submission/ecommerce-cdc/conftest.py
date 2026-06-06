"""Root conftest — puts the candidate dir on sys.path so packages import cleanly."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
