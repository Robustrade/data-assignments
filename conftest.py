"""Root conftest — adds project root and submission/chetan to sys.path."""

import os
import sys

root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

submission_chetan = os.path.join(root_dir, "submission", "chetan")
if os.path.exists(submission_chetan) and submission_chetan not in sys.path:
    sys.path.insert(0, submission_chetan)
