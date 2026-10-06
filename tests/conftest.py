import sys
from pathlib import Path

# pipeline_src/ is not an installed package (it's zipped as-is and deployed to
# Glue as the SDP pipeline source), so tests need it added to sys.path explicitly
# to import f1_pipeline_lib.
REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_SRC_DIR = REPO_ROOT / "pipeline_src"
if str(PIPELINE_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_SRC_DIR))
