import json
from pathlib import Path
from typing import Any, Dict, Optional


RESULTS_DIRECTORY = Path(__file__).parent / "results"


def save_result(request_id: str, data: Dict[str, Any]) -> None:
    RESULTS_DIRECTORY.mkdir(exist_ok=True)

    result_file = RESULTS_DIRECTORY / f"{request_id}.json"
    temporary_file = RESULTS_DIRECTORY / f"{request_id}.tmp"

    temporary_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary_file.replace(result_file)


def get_result(request_id: str) -> Optional[Dict[str, Any]]:
    result_file = RESULTS_DIRECTORY / f"{request_id}.json"

    if not result_file.exists():
        return None

    return json.loads(result_file.read_text(encoding="utf-8"))
