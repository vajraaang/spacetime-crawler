import pickle
import os
import sys


def _maybe_add_base_site_packages() -> bool:
    """
    In some lab setups, the crawler runs inside a venv created from a Python
    distro (e.g., conda) that already has dependencies like `requests`
    installed, but the venv does not include system site-packages.

    The cache server pickles a `requests.Response` object; unpickling requires
    that the `requests` package is importable. If it's missing, try adding the
    base interpreter's site-packages to sys.path as a best-effort fallback.
    """
    base_prefix = getattr(sys, "base_prefix", sys.prefix)
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = (
        os.path.join(base_prefix, "lib", version, "site-packages"),
        os.path.join(base_prefix, "lib64", version, "site-packages"),
    )
    added = False
    for path in candidates:
        if os.path.isdir(path) and path not in sys.path:
            sys.path.append(path)
            added = True
    return added

class Response(object):
    def __init__(self, resp_dict):
        self.url = resp_dict["url"]
        self.status = resp_dict["status"]
        self.error = resp_dict["error"] if "error" in resp_dict else None
        self.raw_response = None
        raw_pickled = resp_dict.get("response")
        if not raw_pickled:
            return
        try:
            self.raw_response = pickle.loads(raw_pickled)
        except ModuleNotFoundError:
            # Likely missing `requests` (or a dependency) inside the venv.
            if _maybe_add_base_site_packages():
                try:
                    self.raw_response = pickle.loads(raw_pickled)
                    return
                except Exception:
                    self.raw_response = None
            else:
                self.raw_response = None
        except Exception:
            self.raw_response = None
