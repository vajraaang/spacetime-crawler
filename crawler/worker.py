from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            self.frontier.wait_for_politeness(tbd_url)
            try:
                resp = download(tbd_url, self.config, self.logger)
            except Exception:
                self.logger.exception(f"Download crashed for {tbd_url}; re-queueing")
                self.frontier.mark_url_failed(tbd_url, requeue=True)
                continue

            if getattr(resp, "status", 0) == 0:
                # Transient cache/network error: retry later.
                self.logger.warning(f"Download failed for {tbd_url}; re-queueing")
                self.frontier.mark_url_failed(tbd_url, requeue=True)
                continue

            try:
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            except Exception:
                # Never let a worker thread die; log and move on.
                self.logger.exception(f"Unhandled worker error while processing {tbd_url}")
            finally:
                self.frontier.mark_url_complete(tbd_url)
