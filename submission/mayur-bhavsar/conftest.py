"""Make the submission importable when pytest is invoked from the repo root.

CI runs ``pytest submission/<candidate>/ -q`` with the repository root as the
working directory, so ``import cdclake`` would otherwise fail.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
