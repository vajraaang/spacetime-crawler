import os
import sqlite3
import time
from collections import deque
from threading import Condition, RLock
from urllib.parse import urldefrag, urlparse

from scraper import is_valid
from utils import get_logger, get_urlhash, normalize


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        self._lock = RLock()
        self._cv = Condition(self._lock)
        self._in_progress = 0
        self._closed = False
        self._domain_next_allowed_at = {}
        self._seen_hashes = set()

        self.to_be_downloaded = deque()

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            for suffix in ("", "-wal", "-shm"):
                path = f"{self.config.save_file}{suffix}"
                if os.path.exists(path):
                    os.remove(path)

        self._db = sqlite3.connect(
            self.config.save_file,
            check_same_thread=False,
            isolation_level=None,  # autocommit
            timeout=30,
        )
        # Best-effort performance settings; safe defaults if unsupported.
        try:
            self._db.execute("PRAGMA journal_mode=WAL;")
            self._db.execute("PRAGMA synchronous=NORMAL;")
        except sqlite3.DatabaseError:
            pass

        self._db.execute(
            "CREATE TABLE IF NOT EXISTS urls ("
            "urlhash TEXT PRIMARY KEY, "
            "url TEXT NOT NULL, "
            "completed INTEGER NOT NULL"
            ");"
        )

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self._seen_hashes:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        """This function can be overridden for alternate saving techniques."""
        tbd_count = 0
        total_count = 0
        for urlhash, url, completed in self._db.execute(
            "SELECT urlhash, url, completed FROM urls;"
        ):
            total_count += 1
            self._seen_hashes.add(urlhash)
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self._cv:
            while not self._closed and not self.to_be_downloaded:
                # If no work is queued and no worker is processing a URL, we're done.
                if self._in_progress == 0:
                    self._closed = True
                    self._cv.notify_all()
                    return None
                self._cv.wait()

            if self._closed:
                return None

            url = self.to_be_downloaded.pop()
            self._in_progress += 1
            return url

    def wait_for_politeness(self, url):
        parsed = urlparse(url)
        domain = (parsed.hostname or "").lower()
        if not domain:
            return

        while True:
            with self._cv:
                now = time.monotonic()
                allowed_at = self._domain_next_allowed_at.get(domain, 0.0)
                if now >= allowed_at:
                    self._domain_next_allowed_at[domain] = now + self.config.time_delay
                    return
                wait_s = allowed_at - now
            time.sleep(wait_s)

    def add_url(self, url):
        url, _frag = urldefrag(url)
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self._cv:
            if self._closed:
                return
            if urlhash not in self._seen_hashes:
                self._seen_hashes.add(urlhash)
                self._db.execute(
                    "INSERT OR IGNORE INTO urls(urlhash, url, completed) VALUES(?, ?, 0);",
                    (urlhash, url),
                )
                self.to_be_downloaded.append(url)
                self._cv.notify()

    def mark_url_complete(self, url):
        url, _frag = urldefrag(url)
        urlhash = get_urlhash(url)
        with self._cv:
            if urlhash not in self._seen_hashes:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            else:
                self._db.execute(
                    "UPDATE urls SET completed = 1 WHERE urlhash = ?;",
                    (urlhash,),
                )

            self._in_progress = max(0, self._in_progress - 1)
            if self._in_progress == 0 and not self.to_be_downloaded:
                self._closed = True
                self._cv.notify_all()
                return

            self._cv.notify_all()
