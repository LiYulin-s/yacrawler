# YACRAWLER - Yet Another Internet Crawler

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/LiYulin-s/yacrawler)
[![Publish Python Package to PyPI with uv](https://github.com/LiYulin-s/yacrawler/actions/workflows/publish.yml/badge.svg)](https://github.com/LiYulin-s/yacrawler/actions/workflows/publish.yml)
[![PyPI](https://img.shields.io/pypi/v/yacrawler)](https://pypi.org/project/yacrawler/)
[![wakatime](https://wakatime.com/badge/user/06081d4f-14d6-4660-b1eb-445d8791e389/project/93a72be9-294e-4a8b-a561-e1b08cc2c549.svg)](https://wakatime.com/badge/user/06081d4f-14d6-4660-b1eb-445d8791e389/project/93a72be9-294e-4a8b-a561-e1b08cc2c549)

## Introduction

YACRAWLER is a simple web crawler written in Python. It is designed to be easy to use and flexible, allowing users to customize the crawling behavior and output format.

YACRAWLER is fully asynchronous, making it efficient and capable of handling large amounts of data quickly. It uses the `aiohttp` library for making HTTP requests and `asyncio` for managing the asynchronous tasks.

YACRAWLER is built using the `Textual` library, which is a modern and powerful library for building rich text-based user interfaces in Python. It provides a simple and intuitive API for creating interactive applications with rich text and widgets.

## Example Usage

To use YACRAWLER, you need to create an instance of the `CrawlerApp` class and pass it the necessary parameters. Here is an example:

```python
from yacrawler.core import Pipeline
from yacrawler.tui import CrawlerTuiApp
from yacrawler.utilities.aioadapter import AioRequest
from yacrawler.utilities.discoverers import SimpleRegexDiscoverer
from yacrawler.utilities.processors import parse_to_dict, write_dict_to_file

pipeline = Pipeline(
    processors=[
        parse_to_dict,
        write_dict_to_file,
    ]
)
app = CrawlerTuiApp(start_url="https://blog.yurin.top", max_depth=3, max_workers=10, request_adapter=AioRequest(),
                    discoverer_adapter=SimpleRegexDiscoverer(), pipeline=pipeline)

```

Then, you can start the crawling process by calling the `run` method:

```shell
python -m yacrawler run YOUR_FILE.app
```

or run a simple crawling process by calling the `crawl` method 😊:

```shell
python -m yacrawler crawl -d 3 -w 10 -u https://blog.yurin.top
```

![Screenshot For CLI](https://github.com/LiYulin-s/yacrawler/blob/main/docs/images/screenshot-cli.png)

![Screenshot For TUI](https://github.com/LiYulin-s/yacrawler/blob/main/docs/images/screenshot-tui.png)

## Features

### Pipelines

Pipelines are a powerful feature of YACRAWLER that allow users to customize the processing of the crawled data. Users can define their own processors and add them to the pipeline to perform tasks such as parsing the HTML content, extracting specific information, and writing the data to a file.

PROCESSORS OF PIPELINES HAVE STRONG TYPE CHECKING, SO YOU CAN'T ADD A PROCESSOR THAT DOESN'T MATCH THE TYPE OF THE DATA IT IS EXPECTED TO PROCESS.

### Customizable Request Adapters

YACRAWLER allows users to customize the request adapter to use their own HTTP client or library. The default request adapter is `AioRequest`, which uses the `aiohttp` library to make HTTP requests asynchronously.

### Customizable Discoverer Adapters

YACRAWLER allows users to customize the discoverer adapter to use their own method for discovering new URLs to crawl. The default discoverer adapter is `SimpleRegexDiscoverer`, which uses regular expressions to discover new URLs from the HTML content of the crawled pages.

## License

YACRAWLER is licensed under the MIT License. See the LICENSE file for more information.

## Acknowledgments

YACRAWLER is built using the following libraries:

- `asyncio`: A library for managing asynchronous tasks.
- `typer`: A library for building command-line interfaces in Python.
- `rich`: A library for formatting and printing text in a terminal-friendly way.
- `textual`: A library for building rich text-based user interfaces in Python.
- `aiofiles`: A library for handling file I/O operations asynchronously.
- `aiohttp`: A library for making HTTP requests asynchronously.

## Contributing

Contributions are welcome! If you have any ideas for improvements or features, please open an issue or submit a pull request.
