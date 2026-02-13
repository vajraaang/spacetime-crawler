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
    try:
        if requests is not None:
            resp = requests.get(
                f"http://{host}:{port}/",
                params=[("q", f"{url}"), ("u", f"{config.user_agent}")],
                timeout=30,
            )
            raw = resp.content
            status_code = resp.status_code
        else:
            qs = urllib.parse.urlencode({"q": url, "u": config.user_agent})
            req_url = f"http://{host}:{port}/?{qs}"
            try:
                with urllib.request.urlopen(req_url, timeout=30) as resp:  # nosec B310
                    raw = resp.read()
                    status_code = getattr(resp, "status", 200)
            except HTTPError as e:
                raw = e.read()
                status_code = e.code

        if raw:
            return Response(cbor.loads(raw))
    except (EOFError, ValueError, URLError, TimeoutError) as e:
        if logger:
            logger.error(f"Spacetime Response error {e!r} with url {url}.")
        return Response({"error": str(e), "status": 0, "url": url})

    if logger:
        logger.error(f"Spacetime Response error with url {url}.")
    return Response({
        "error": f"Spacetime Response error with url {url}.",
        "status": status_code if "status_code" in locals() else 0,
        "url": url})
