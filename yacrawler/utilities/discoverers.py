import re
from typing import Optional

from yacrawler.core import DiscovererAdapter, Response, Engine



class SimpleRegexDiscoverer(DiscovererAdapter):
    URL_REGEX = re.compile(r'href=["\'](https?://[^"\']+)["\']', re.IGNORECASE)

    def discover(self, response: Response) -> list[str]:
        try:
            try:
                html_content = response.body.decode('utf-8')
            except UnicodeDecodeError:
                 try:
                     html_content = response.body.decode('latin-1')
                 except Exception:
                     html_content = response.body.decode(errors="ignore")
                     if self.engine:
                          self.engine.log_adapter.log(f"Could not decode content from {response.request.url} with utf-8 or latin-1, ignoring errors.", level="WARNING")
                     else:
                          print(f"Could not decode content from {response.request.url} with utf-8 or latin-1, ignoring errors.")

            urls = list(set(self.URL_REGEX.findall(html_content)))

            valid_urls = []
            for url in urls:
                if url and url.startswith("http"):
                    parsed_url = url.split('#')[0]
                    valid_urls.append(parsed_url)

            return valid_urls
        except Exception as e:
            if self.engine:
                self.engine.log_adapter.log(f"Error during discovery for {response.request.url}: {e}", level="ERROR")
            else:
                print(f"Error during discovery for {response.request.url}: {e}")
            return []

    def __init__(self):
        self.engine: Optional[Engine] = None

    def set_engine(self, engine: Engine):
        self.engine = engine
