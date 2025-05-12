from yacrawler.core import Pipeline
from yacrawler.tui import CrawlerTuiApp
from yacrawler.utilities.aioadapter import AioRequest
from yacrawler.utilities.discoverers import FilteredRegexDiscoverer
from yacrawler.utilities.processors import parse_to_dict, write_dict_to_file

pipeline = Pipeline(
    processors=[
        parse_to_dict,
        write_dict_to_file,
    ]
)

discoverer = FilteredRegexDiscoverer(predict=lambda url: "https://blog.lilydjwg.me/" in url)

app = CrawlerTuiApp(start_urls=["https://blog.lilydjwg.me/"], max_depth=2, max_workers=100,
                    request_adapter=AioRequest(),
                    discoverer_adapter=discoverer, pipeline=pipeline)

app.run()