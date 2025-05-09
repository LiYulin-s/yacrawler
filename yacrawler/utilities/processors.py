import json
from typing import Dict

from yacrawler.core.pipeline import Processor
from yacrawler.core.response import Response

import re


def _parse_to_dict(response: Response) -> Dict:
    title_re = re.compile(r'<title>(.*?)</title>')
    return {
        "url": response.request.url,
        "status_code": response.status_code,
        "title": title_re.findall(response.body.decode())[0] if title_re.findall(response.body.decode()) else "N/A"
    }

async def _write_dict_to_file(data: Dict) -> str:
    with open("output.jsonl", "a") as f:
        f.write(json.dumps(data) + "\n")
    return f"Saved data for {data.get('url', 'N/A')}"

parse_to_dict = Processor(_parse_to_dict, input_type=Response, output_type=Dict)
write_dict_to_file = Processor(_write_dict_to_file, input_type=Dict, output_type=str)