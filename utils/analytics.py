from __future__ import annotations

import atexit
import json
import os
import pickle
import re
import time
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from urllib.parse import urldefrag, urlparse


_WORD_RE = re.compile(r"[a-zA-Z]{2,}(?:[’'][a-zA-Z]+)*")


def _default_stopwords() -> set[str]:
    # A compact English stopword set (no external dependencies).
    return {
        "a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "all",
        "am",
        "an",
        "and",
        "any",
        "are",
        "aren't",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "both",
        "but",
        "by",
        "can",
        "can't",
        "cannot",
        "could",
        "couldn't",
        "did",
        "didn't",
        "do",
        "does",
        "doesn't",
        "doing",
        "don't",
        "down",
        "during",
        "each",
        "few",
        "for",
        "from",
        "further",
        "had",
        "hadn't",
        "has",
        "hasn't",
        "have",
        "haven't",
        "having",
        "he",
        "he'd",
        "he'll",
        "he's",
        "her",
        "here",
        "here's",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "how's",
        "i",
        "i'd",
        "i'll",
        "i'm",
        "i've",
        "if",
        "in",
        "into",
        "is",
        "isn't",
        "it",
        "it's",
        "its",
        "itself",
        "let's",
        "may",
        "me",
        "more",
        "most",
        "mustn't",
        "my",
        "myself",
        "no",
        "nor",
        "not",
        "of",
        "off",
        "on",
        "once",
        "only",
        "or",
        "other",
        "ought",
        "our",
        "ours",
        "ourselves",
        "out",
        "over",
        "own",
        "please",
        "same",
        "shan't",
        "she",
        "she'd",
        "she'll",
        "she's",
        "should",
        "shouldn't",
        "so",
        "some",
        "such",
        "than",
        "that",
        "that's",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "there's",
        "these",
        "they",
        "they'd",
        "they'll",
        "they're",
        "they've",
        "this",
        "those",
        "through",
        "to",
        "too",
        "under",
        "until",
        "up",
        "us",
        "very",
        "was",
        "wasn't",
        "we",
        "we'd",
        "we'll",
        "we're",
        "we've",
        "were",
        "weren't",
        "what",
        "what's",
        "when",
        "when's",
        "where",
        "where's",
        "which",
        "while",
        "who",
        "who's",
        "whom",
        "why",
        "why's",
        "with",
        "won't",
        "would",
        "wouldn't",
        "will",
        "you",
        "you'd",
        "you'll",
        "you're",
        "you've",
        "your",
        "yours",
        "yourself",
        "yourselves",
    }


def _defrag_url(url: str) -> str:
    url, _frag = urldefrag(url)
    return url


@dataclass(slots=True)
class LongestPage:
    url: str = ""
    words: int = 0


class Analytics:
    """
    Thread-safe crawl analytics needed for the report.

    Uniqueness for counting is based on the URL with fragment removed only.
    """

    def __init__(
        self,
        out_dir: str = "analytics",
        state_file: str = "state.pkl",
        save_every_pages: int = 250,
        save_every_seconds: float = 60.0,
    ):
        self._lock = RLock()
        self.out_dir = out_dir
        self.state_path = os.path.join(out_dir, state_file)
        self.save_every_pages = save_every_pages
        self.save_every_seconds = save_every_seconds

        self.stopwords = _default_stopwords()
        self._load_stopwords_file()

        # Report metrics
        self.unique_url_hashes: set[bytes] = set()
        self.subdomain_counts: Counter[str] = Counter()
        self.word_frequencies: Counter[str] = Counter()
        self.longest_page = LongestPage()

        # Similarity detection (extra credit): exact + near duplicates
        self._exact_digests: set[bytes] = set()
        self._simhash_buckets: dict[int, set[int]] = {}
        self.duplicate_exact = 0
        self.duplicate_near = 0
        self.skipped_lowinfo = 0

        self._dirty_pages = 0
        self._last_save_at = time.monotonic()

        self._load_if_present()
        atexit.register(self.save)

    def reset(self) -> None:
        with self._lock:
            self.unique_url_hashes.clear()
            self.subdomain_counts.clear()
            self.word_frequencies.clear()
            self.longest_page = LongestPage()
            self._exact_digests.clear()
            self._simhash_buckets.clear()
            self.duplicate_exact = 0
            self.duplicate_near = 0
            self.skipped_lowinfo = 0
            self._dirty_pages = 0
            self._last_save_at = time.monotonic()

    def _load_if_present(self) -> None:
        try:
            with open(self.state_path, "rb") as f:
                state = pickle.load(f)
        except FileNotFoundError:
            return
        except Exception:
            # Corrupt/partial state file: ignore and start fresh.
            return

        try:
            self.unique_url_hashes = state.get("unique_url_hashes", set())
            self.subdomain_counts = state.get("subdomain_counts", Counter())
            self.word_frequencies = state.get("word_frequencies", Counter())
            self.longest_page = state.get("longest_page", LongestPage())
            self._exact_digests = state.get("exact_digests", set())
            self._simhash_buckets = state.get("simhash_buckets", {})
            self.duplicate_exact = int(state.get("duplicate_exact", 0))
            self.duplicate_near = int(state.get("duplicate_near", 0))
            self.skipped_lowinfo = int(state.get("skipped_lowinfo", 0))
        except Exception:
            # If the pickle has unexpected shape, ignore.
            return

    def tokenize(self, text: str) -> list[str]:
        words = [w.lower().replace("’", "'") for w in _WORD_RE.findall(text)]
        return [w for w in words if w not in self.stopwords]

    def _load_stopwords_file(self) -> None:
        """
        Optional: load stopwords from a file (one word per line).

        Candidates (first found wins):
          - $STOPWORDS_PATH
          - ./stopwords.txt (cwd)
          - <repo_root>/stopwords.txt
        """
        candidates: list[str] = []
        env_path = os.environ.get("STOPWORDS_PATH")
        if env_path:
            candidates.append(env_path)
        candidates.append(os.path.join(os.getcwd(), "stopwords.txt"))
        candidates.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "stopwords.txt")))

        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
            except FileNotFoundError:
                continue
            except OSError:
                continue

            extra: set[str] = set()
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                extra.add(line.lower().replace("’", "'"))

            if extra:
                self.stopwords |= extra
            return

    def _seen_key(self, url: str) -> bytes:
        key = _defrag_url(url)
        return sha256(key.encode("utf-8")).digest()

    def record_url(self, url: str) -> bool:
        """Record a successfully fetched URL for uniqueness/subdomain counts."""
        if not url:
            return False

        url_key = _defrag_url(url)
        url_hash = self._seen_key(url_key)

        with self._lock:
            if url_hash in self.unique_url_hashes:
                return False
            self.unique_url_hashes.add(url_hash)

            parsed = urlparse(url_key)
            host = (parsed.hostname or "").lower()
            if host.endswith(".uci.edu"):
                self.subdomain_counts[host] += 1

            self._dirty_pages += 1
            self._maybe_save_locked()

        return True

    def record_words(self, url: str, words: list[str]) -> None:
        """Record tokenized (stopword-filtered) words for report stats."""
        if not url or not words:
            return
        url_key = _defrag_url(url)
        with self._lock:
            word_count = len(words)
            if word_count > self.longest_page.words:
                self.longest_page = LongestPage(url=url_key, words=word_count)
            self.word_frequencies.update(words)
            self._dirty_pages += 1
            self._maybe_save_locked()

    def unique_pages(self) -> int:
        with self._lock:
            return len(self.unique_url_hashes)

    def top_words(self, n: int = 50) -> list[tuple[str, int]]:
        with self._lock:
            items = ((w, c) for w, c in self.word_frequencies.items() if w not in self.stopwords)
            return sorted(items, key=lambda wc: wc[1], reverse=True)[:n]

    def mark_lowinfo_skipped(self) -> None:
        with self._lock:
            self.skipped_lowinfo += 1
            self._dirty_pages += 1
            self._maybe_save_locked()

    @staticmethod
    def _simhash(features: list[str]) -> int:
        # 64-bit SimHash from scratch.
        # https://doi.org/10.1145/1327452.1327492 (Charikar, 2002)
        if not features:
            return 0
        acc = [0] * 64
        for f in features:
            h = int.from_bytes(sha256(f.encode("utf-8")).digest()[:8], "big", signed=False)
            for i in range(64):
                acc[i] += 1 if (h >> i) & 1 else -1
        out = 0
        for i, v in enumerate(acc):
            if v >= 0:
                out |= 1 << i
        return out

    @staticmethod
    def _shingles(words: list[str], k: int = 3) -> list[str]:
        if k <= 1 or len(words) < k:
            return words
        return [" ".join(words[i : i + k]) for i in range(len(words) - k + 1)]

    def is_duplicate_text(self, words: list[str], *, near_threshold_bits: int = 3) -> bool:
        """
        Returns True if text is an exact or near-duplicate of previously accepted pages.

        Side effect: registers this page's fingerprint if it's not a duplicate.
        """
        if not words:
            return False

        digest = sha256((" ".join(words)).encode("utf-8")).digest()
        shingles = self._shingles(words, k=3)
        sim = self._simhash(shingles)

        def bucket_keys(simhash: int) -> list[int]:
            # 4 bands of 16 bits each => key = (band_index << 16) | band_value
            keys: list[int] = []
            for i in range(4):
                band = (simhash >> (i * 16)) & 0xFFFF
                keys.append((i << 16) | band)
            return keys

        with self._lock:
            if digest in self._exact_digests:
                self.duplicate_exact += 1
                return True

            keys = bucket_keys(sim)
            candidates: set[int] = set()
            for k in keys:
                bucket = self._simhash_buckets.get(k)
                if bucket:
                    candidates.update(bucket)
            for cand in candidates:
                if (sim ^ cand).bit_count() <= near_threshold_bits:
                    self.duplicate_near += 1
                    return True

            # Register as a new, non-duplicate page.
            self._exact_digests.add(digest)
            for k in keys:
                self._simhash_buckets.setdefault(k, set()).add(sim)

            self._dirty_pages += 1
            self._maybe_save_locked()
            return False

    def _maybe_save_locked(self) -> None:
        now = time.monotonic()
        if self._dirty_pages >= self.save_every_pages or (now - self._last_save_at) >= self.save_every_seconds:
            self._save_locked()
            self._dirty_pages = 0
            self._last_save_at = now

    def save(self) -> None:
        with self._lock:
            self._save_locked()

    def _save_locked(self) -> None:
        os.makedirs(self.out_dir, exist_ok=True)
        tmp_path = f"{self.state_path}.tmp"
        state = {
            "unique_url_hashes": self.unique_url_hashes,
            "subdomain_counts": self.subdomain_counts,
            "word_frequencies": self.word_frequencies,
            "longest_page": self.longest_page,
            "exact_digests": self._exact_digests,
            "simhash_buckets": self._simhash_buckets,
            "duplicate_exact": self.duplicate_exact,
            "duplicate_near": self.duplicate_near,
            "skipped_lowinfo": self.skipped_lowinfo,
        }
        with open(tmp_path, "wb") as f:
            pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp_path, self.state_path)

        summary_path = os.path.join(self.out_dir, "summary.json")
        tmp_summary_path = f"{summary_path}.tmp"
        summary = {
            "unique_pages": len(self.unique_url_hashes),
            "longest_page": {"url": self.longest_page.url, "words": self.longest_page.words},
            "top_words": self.top_words(50),
            "subdomains": dict(self.subdomain_counts),
            "duplicates": {"exact": self.duplicate_exact, "near": self.duplicate_near, "lowinfo": self.skipped_lowinfo},
        }
        with open(tmp_summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        os.replace(tmp_summary_path, summary_path)


analytics = Analytics()
