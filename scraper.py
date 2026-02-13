import re
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urldefrag, urljoin, urlparse

from utils.analytics import analytics
from utils.pythonpath import add_base_site_packages

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

class _FallbackHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: list[str] = []
        self._skip_depth = 0
        self._text_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        t = tag.lower()
        if t == "a":
            for k, v in attrs:
                if k.lower() == "href" and v:
                    self.links.append(v)
                    break
        if t in {"script", "style", "noscript"}:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        t = tag.lower()
        if t in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0 and data:
            self._text_parts.append(data)

    def text(self) -> str:
        return " ".join(self._text_parts)

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if not resp or resp.status != 200 or not resp.raw_response:
        return []

    content = getattr(resp.raw_response, "content", None)
    if not content:
        return []

    page_url = getattr(resp.raw_response, "url", None) or resp.url or url
    page_url, _frag = urldefrag(page_url)
    base_url = page_url

    out = set()
    extracted_text = ""

    headers = getattr(resp.raw_response, "headers", None)
    content_type = ""
    if headers and hasattr(headers, "get"):
        content_type = (headers.get("Content-Type") or "").lower()
    if content_type and "text/html" not in content_type:
        return []

    # Avoid parsing extremely large HTML pages.
    if isinstance(content, (bytes, bytearray)) and len(content) > 5_000_000:
        return []

    soup = None
    try:
        from bs4 import BeautifulSoup, FeatureNotFound  # type: ignore
    except ModuleNotFoundError:
        add_base_site_packages()
        try:
            from bs4 import BeautifulSoup, FeatureNotFound  # type: ignore
        except ModuleNotFoundError:
            BeautifulSoup = None  # type: ignore
            FeatureNotFound = Exception  # type: ignore

    if soup is None and "BeautifulSoup" in locals() and BeautifulSoup is not None:  # type: ignore
        try:
            soup = BeautifulSoup(content, "lxml")  # type: ignore
        except Exception:
            try:
                soup = BeautifulSoup(content, "html.parser")  # type: ignore
            except Exception:
                soup = None

    if soup is not None:
        base_tag = soup.find("base", href=True)
        if base_tag and base_tag.get("href"):
            base_url = urljoin(page_url, base_tag.get("href").strip())

        for kill in soup(["script", "style", "noscript"]):
            kill.decompose()
        extracted_text = soup.get_text(separator=" ", strip=True)

        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not href:
                continue
            href = href.strip()
            if not href:
                continue
            lower = href.lower()
            if lower.startswith(("mailto:", "javascript:", "tel:")):
                continue

            next_url = urljoin(base_url, href)
            next_url, _frag = urldefrag(next_url)
            if next_url:
                out.add(next_url)
    else:
        # Dependency-free fallback, so the crawler still works without bs4/lxml.
        if isinstance(content, (bytes, bytearray)):
            try:
                html = content.decode("utf-8", errors="ignore")
            except Exception:
                html = content.decode(errors="ignore")
        else:
            html = str(content)

        parser = _FallbackHTMLParser()
        try:
            parser.feed(html)
        except Exception:
            return []

        extracted_text = parser.text()
        for href in parser.links:
            href = (href or "").strip()
            if not href:
                continue
            lower = href.lower()
            if lower.startswith(("mailto:", "javascript:", "tel:")):
                continue
            next_url = urljoin(base_url, href)
            next_url, _frag = urldefrag(next_url)
            if next_url:
                out.add(next_url)

    # Update analytics for this (defragmented) URL.
    is_new = analytics.record_url(page_url)

    words: list[str] = []
    if extracted_text:
        words = analytics.tokenize(extracted_text)

    # Heuristic: avoid expanding "thin" pages that look like link directories.
    # Still count the URL itself as crawled, but don't use it for word stats.
    word_count = len(words)
    outlink_count = len(out)
    if outlink_count > 1000:
        analytics.mark_lowinfo_skipped()
        return []
    if word_count < 10:
        analytics.mark_lowinfo_skipped()
        return []
    if outlink_count > 200 and (word_count / (outlink_count + 1)) < 0.05:
        analytics.mark_lowinfo_skipped()
        return []

    if is_new and word_count >= 50:
        # Similarity detection (exact + near). If the page is a duplicate, avoid
        # expanding its outlinks and keep word stats focused on distinct content.
        if analytics.is_duplicate_text(words):
            return []
        analytics.record_words(page_url, words)

    return list(out)

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        url, _frag = urldefrag(url)
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        host = (parsed.hostname or "").lower()
        if not host:
            return False

        allowed = (
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        )
        if not any(host == d or host.endswith("." + d) for d in allowed):
            return False

        # Basic trap/garbage filters.
        if len(url) > 300:
            return False
        if len(parsed.path) > 200:
            return False
        path_segs = [seg for seg in parsed.path.split("/") if seg]
        if len(path_segs) > 25:
            return False
        # Repeated segments can indicate auto-generated traps.
        if path_segs:
            seg_counts = {}
            for seg in path_segs:
                seg_l = seg.lower()
                seg_counts[seg_l] = seg_counts.get(seg_l, 0) + 1
                if seg_counts[seg_l] > 5:
                    return False

        if parsed.query and len(parsed.query) > 200:
            return False

        if parsed.query:
            q = parsed.query.lower()
            if re.search(r"(?:replytocom=|session=|sid=|phpsessid=|jsessionid=|utm_)", q):
                return False
            params = parse_qsl(parsed.query, keep_blank_values=True)
            if len(params) > 8:
                return False
            key_counts = {}
            for k, v in params:
                k = (k or "").lower()
                v = (v or "")
                key_counts[k] = key_counts.get(k, 0) + 1
                if key_counts[k] > 2:
                    return False
                if len(v) > 100:
                    return False
            # Calendar-like pages can explode combinatorially.
            if ("calendar" in parsed.path.lower() or "event" in parsed.path.lower()) and len(params) >= 4:
                return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        return False
