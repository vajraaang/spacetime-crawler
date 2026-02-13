import multiprocessing as mp
import os
import shutil
import sys

from configparser import ConfigParser
from argparse import ArgumentParser


def main(config_file, restart):
    # On macOS, Python's default multiprocessing start method is "spawn", which
    # requires pickling the target. The spacetime library constructs a local
    # class (not pickleable), so we force "fork" when available.
    #
    # IMPORTANT: This must run before importing spacetime (via
    # utils.server_registration), otherwise the Process class will already be
    # bound to the spawn context.
    if sys.platform == "darwin":
        try:
            mp.set_start_method("fork", force=True)
        except RuntimeError:
            pass

    from utils.server_registration import get_cache_server
    from utils.config import Config
    from crawler import Crawler

    if restart:
        # Keep analytics in sync with a fresh crawl.
        try:
            shutil.rmtree("analytics")
        except FileNotFoundError:
            pass
        except OSError:
            # Directory may be in-use; fall back to clearing the known state file.
            try:
                os.remove(os.path.join("analytics", "state.pkl"))
            except FileNotFoundError:
                pass

    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
