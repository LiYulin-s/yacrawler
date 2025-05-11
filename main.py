from yacrawler.core import Pipeline
from yacrawler.tui import CrawlerApp
from yacrawler.utilities.aioadapter import AioRequest
from yacrawler.utilities.discoverers import SimpleRegexDiscoverer
from yacrawler.utilities.processors import parse_to_dict, write_dict_to_file

pipeline = Pipeline(
    processors=[
        parse_to_dict,
        write_dict_to_file,
    ]
)
app = CrawlerApp(start_url="https://blog.yurin.top", max_depth=2, max_workers=100, request_adapter=AioRequest(),
                 discoverer_adapter=SimpleRegexDiscoverer(), pipeline=pipeline)

app.run()