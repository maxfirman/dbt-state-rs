"""The harness browser: what the dbt client's sign-in URL opens in the
driver's person scenarios.

The driver names this script in `BROWSER`, so the client's own
`webbrowser.open(url)` runs it with the URL as its argument. It does what a
person's browser does when they are already signed in at the Upstream IdP:
it follows the redirects from the Token broker's sign-in URL, through the
IdP and back through the broker, to the client's loopback handler, which
answers its page. It trusts the CA in `REQUESTS_CA_BUNDLE`, as the client
does.

Each visit is appended as one JSON line to `DRIVER_BROWSER_LOG`: the URL
opened, every redirect followed and where it landed (without queries,
which carry the authorization code), and the final status.

Usage: `browser.py <url>`.
"""

import json
import os
import sys
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests


def without_query(url: str) -> str:
    return urlunsplit(urlsplit(url)._replace(query="", fragment=""))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: browser.py <url>", file=sys.stderr)
        return 2
    url = sys.argv[1]
    response = requests.get(url, timeout=30)
    redirects = [
        without_query(urljoin(hop.url, hop.headers["location"])) for hop in response.history
    ]
    visit = {
        "opened": url,
        "redirects": redirects,
        "landed": without_query(response.url),
        "status": response.status_code,
    }
    with open(os.environ["DRIVER_BROWSER_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps(visit) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
