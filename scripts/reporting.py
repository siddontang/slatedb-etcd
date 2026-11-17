"""Write portable reports without workstation paths."""

import json
from pathlib import Path
import re
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def portable(value):
    if isinstance(value, dict):
        return {key: portable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [portable(item) for item in value]
    if isinstance(value, str):
        for path, label in [(ROOT, "<repo>"), (Path.home(), "<home>"),
                            (Path(tempfile.gettempdir()), "<tmp>")]:
            pattern = r"(?<![\w./<>-])" + re.escape(str(path)) + r"(?=/|$)"
            value = re.sub(pattern, label, value)
        value = re.sub(r"(?<![\w./<>-])/(?:Users|home)/[^/\s\"'<>]+", "<home>", value)
        value = re.sub(r"(?<![\w./<>-])/(?:private/)?var/folders/[^/\s]+/[^/\s]+/T(?=/|$)",
                       "<tmp>", value)
        return value
    return value


def write_report(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(portable(value), indent=2) + "\n")
