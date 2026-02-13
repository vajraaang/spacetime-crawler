import os
import logging
from hashlib import sha256
from urllib.parse import urldefrag, urlparse, urlunparse

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    return sha256(normalize(url).encode("utf-8")).hexdigest()

def normalize(url):
    """
    Canonicalize a URL for crawl-deduping.

    For this project we only *require* defragmenting for uniqueness, but we
    also normalize scheme/host casing and strip default ports.
    """
    if not url:
        return url

    url, _frag = urldefrag(url)
    parsed = urlparse(url)

    scheme = (parsed.scheme or "").lower()

    hostname = (parsed.hostname or "").lower()
    if hostname and ":" in hostname and not hostname.startswith("["):
        # IPv6 host
        hostname = f"[{hostname}]"

    port = parsed.port
    is_default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)

    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
        userinfo += "@"

    if port and not is_default_port:
        netloc = f"{userinfo}{hostname}:{port}"
    else:
        netloc = f"{userinfo}{hostname}"

    return urlunparse((scheme, netloc, parsed.path or "", parsed.params or "", parsed.query or "", ""))
