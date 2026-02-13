import os
from argparse import ArgumentParser


def main(state_path: str, out_path: str) -> int:
    try:
        from utils.analytics import Analytics
    except Exception as e:
        print(f"Failed to import analytics: {e!r}")
        return 2

    analytics = Analytics(out_dir=os.path.dirname(state_path) or "analytics", state_file=os.path.basename(state_path))

    unique_pages = analytics.unique_pages()
    longest_url = analytics.longest_page.url
    longest_words = analytics.longest_page.words
    top_words = analytics.top_words(50)
    subdomains = sorted(analytics.subdomain_counts.items(), key=lambda kv: kv[0])

    lines: list[str] = []
    lines.append(f"Unique pages (URL defragmented only): {unique_pages}")
    lines.append("")
    lines.append("Longest page (by word count):")
    lines.append(f"{longest_url}, {longest_words}")
    lines.append("")
    lines.append("Top 50 words (stopwords removed):")
    for word, count in top_words:
        lines.append(f"{word}, {count}")
    lines.append("")
    lines.append(f"Subdomains in uci.edu: {len(subdomains)}")
    for host, count in subdomains:
        lines.append(f"{host}, {count}")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")

    print(f"Wrote report to {out_path}")
    return 0


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--state", default=os.path.join("analytics", "state.pkl"))
    parser.add_argument("--out", default=os.path.join("analytics", "report.txt"))
    args = parser.parse_args()
    raise SystemExit(main(args.state, args.out))
