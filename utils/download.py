import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError

import cbor

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

from utils.response import Response

def download(url, config, logger=None):
    host, port = config.cache_server
    max_attempts = 3
    backoff_s = 1.0
    last_err = None

    for attempt in range(1, max_attempts + 1):
        try:
            if requests is not None:
                resp = requests.get(
                    f"http://{host}:{port}/",
                    params=[("q", f"{url}"), ("u", f"{config.user_agent}")],
                    timeout=(10, 60),
                )
                raw = resp.content
                status_code = resp.status_code
            else:
                qs = urllib.parse.urlencode({"q": url, "u": config.user_agent})
                req_url = f"http://{host}:{port}/?{qs}"
                try:
                    with urllib.request.urlopen(req_url, timeout=60) as resp:  # nosec B310
                        raw = resp.read()
                        status_code = getattr(resp, "status", 200)
                except HTTPError as e:
                    raw = e.read()
                    status_code = e.code

            if raw:
                try:
                    return Response(cbor.loads(raw))
                except (EOFError, ValueError) as e:
                    last_err = e
                    if logger:
                        logger.error(f"Failed to decode cache response for {url}: {e!r}")
            else:
                last_err = ValueError("Empty response body from cache server")
                if logger:
                    logger.error(f"Empty cache response for {url}")

        except (URLError, TimeoutError) as e:
            last_err = e
            if logger:
                logger.warning(
                    f"Cache request failed (attempt {attempt}/{max_attempts}) for {url}: {e!r}"
                )
        except Exception as e:  # requests timeouts, etc.
            last_err = e
            if logger:
                logger.warning(
                    f"Cache request failed (attempt {attempt}/{max_attempts}) for {url}: {e!r}"
                )

        if attempt < max_attempts:
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2, 10.0)
            continue

        # Exhausted retries.
        if logger:
            logger.error(f"Spacetime Response error {last_err!r} with url {url}.")
        return Response({"error": str(last_err), "status": 0, "url": url})

    if logger:
        logger.error(f"Spacetime Response error with url {url}.")
    return Response({
        "error": f"Spacetime Response error with url {url}.",
        "status": status_code if "status_code" in locals() else 0,
        "url": url})
