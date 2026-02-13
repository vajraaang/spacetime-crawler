import os
import sys


def add_base_site_packages() -> bool:
    """
    Best-effort: add the base interpreter's site-packages to sys.path.

    Useful when running inside a venv created from a Python distribution (e.g.,
    conda) where common dependencies may exist in the base environment but are
    not installed in the venv.
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

