#!/usr/bin/env python3
"""
mcpd_crime_bot.py

Polls Montgomery County, MD's public "Police Dispatched Incidents" open
data feed and auto-posts new matching incidents to an X (Twitter) account.

Data source (free, public, no key required for basic use):
  https://data.montgomerycountymd.gov/Public-Safety/Police-Dispatched-Incidents/98cc-bc7d
  API endpoint: https://data.montgomerycountymd.gov/resource/98cc-bc7d.json

Posting: X API v2 `POST /2/tweets` via OAuth 1.0a user context (tweepy).
Cost: pay-per-use, $0.015 per post created (no link), see README.md.

State: a small JSON file (state.json) tracks the timestamp of the last
incident processed, so re-running the script never double-posts.

Run this on a schedule (cron, GitHub Actions, etc.) — see README.md.
"""

import json
import logging
import os
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import tweepy
except ImportError:
    tweepy = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Configuration — tune this section for what counts as "alert-worthy"
# ---------------------------------------------------------------------------

SOCRATA_ENDPOINT = "https://data.montgomerycountymd.gov/resource/98cc-bc7d.json"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")  # optional, free, raises rate limit

STATE_FILE = Path(__file__).parent / "state.json"
LOG_FILE = Path(__file__).parent / "bot.log"

# Only post incidents whose initial_type contains one of these substrings
# (case-insensitive). This is a dispatch-call category, assigned before
# investigation, so it can occasionally be reclassified later (e.g. an
# "ALARM - ROBBERY" that turns out to be a false alarm). Keep this list
# focused on genuinely alert-worthy categories rather than every dispatch.
INCLUDE_TYPE_KEYWORDS = [
    "ROBBERY",
    "ASSAULT",
    "BURGLARY",
    "SHOOTING",
    "HOMICIDE",
    "STABBING",
    "WEAPON",
    "CARJACK",
    "SEX OFFENSE",
    "KIDNAP",
    "ARSON",
    "HOSTAGE",
    "AUTOTHEFT",
]

# If True, skip anything whose category contains "ALARM" even if it also
# matches a keyword above (alarm calls are frequently false alarms/malfunctions).
EXCLUDE_ALARMS = True

# On the very first run (no saved state yet), only look back this many hours
# instead of fetching from the start of the entire dataset. This dataset's
# history goes back years, and ordering ASC with no starting point means
# "first record ever" — not "most recent" — without this bound.
INITIAL_LOOKBACK_HOURS = 6

# Safety valves
MAX_POSTS_PER_RUN = 10          # never spam more than this in one run
SECONDS_BETWEEN_POSTS = 5        # pause between consecutive posts
REQUEST_TIMEOUT = 15

# Tweet template. Available fields: incident_id, initial_type, address,
# city, start_time (formatted). Keep total length under 280 chars.
POST_TEMPLATE = "MCPD dispatched: {initial_type}\n{address}, {city}\n{time_str} | #MontgomeryCountyMD"

# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("mcpd_crime_bot")


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_start_time": None, "seen_ids": []}


def save_state(state: dict) -> None:
    # Keep the seen_ids list bounded so the file doesn't grow forever.
    state["seen_ids"] = state["seen_ids"][-500:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def fetch_new_incidents(last_start_time: str | None) -> list[dict]:
    """Query Socrata for incidents dispatched after last_start_time, oldest first."""
    params = {
        "$order": "start_time ASC",
        "$limit": 200,
    }
    if last_start_time:
        # >= (not >): the watermark can land exactly on an incident that
        # previously failed to post, and we need that one included again.
        # seen_ids (in main()) handles de-duping anything already fully handled.
        params["$where"] = f"start_time >= '{last_start_time}'"
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    url = SOCRATA_ENDPOINT + "?" + urllib.parse.urlencode(params)

    # data.montgomerycountymd.gov sits behind bot-protection that fingerprints
    # the TLS handshake, not just headers — Python's `requests` (urllib3) gets
    # blocked with a 403 even with a spoofed User-Agent, while plain `curl`
    # (a different TLS signature) passes. So we shell out to curl instead of
    # using `requests` for this call.
    #
    # No -f flag here on purpose: -f hides the response body and status code
    # on HTTP errors, which makes failures impossible to diagnose. Instead we
    # append the status code with -w and check it ourselves.
    status_marker = "\n__STATUS__:"
    cmd = [
        "curl", "-s", "--max-time", str(REQUEST_TIMEOUT),
        "-H", "Accept: application/json",
        "-w", status_marker + "%{http_code}",
        url,
    ]
    if SOCRATA_APP_TOKEN:
        cmd[-1:-1] = ["-H", f"X-App-Token: {SOCRATA_APP_TOKEN}"]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=REQUEST_TIMEOUT + 10, check=False)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("curl request timed out") from e

    if result.returncode != 0:
        raise RuntimeError(f"curl could not execute the request (exit {result.returncode}): {result.stderr.strip()}")

    body, _, status_code = result.stdout.rpartition(status_marker)
    status_code = status_code.strip()

    if not status_code.startswith("2"):
        raise RuntimeError(f"HTTP {status_code} from Socrata. Response body: {body[:500]!r}")

    return json.loads(body)


def matches_filter(incident: dict) -> bool:
    itype = (incident.get("initial_type") or "").upper()
    if EXCLUDE_ALARMS and "ALARM" in itype:
        return False
    return any(kw in itype for kw in INCLUDE_TYPE_KEYWORDS)


def format_post(incident: dict) -> str:
    try:
        dt = datetime.fromisoformat(incident["start_time"])
        time_str = dt.strftime("%b %d, %I:%M %p")
    except Exception:
        time_str = incident.get("start_time", "unknown time")

    text = POST_TEMPLATE.format(
        initial_type=incident.get("initial_type", "Incident").title(),
        address=incident.get("address", "location withheld").title(),
        city=incident.get("city", "").title(),
        time_str=time_str,
    )
    return text[:280]


def get_x_client() -> "tweepy.Client":
    if tweepy is None:
        raise RuntimeError("tweepy is not installed. Run: pip install tweepy")
    required = ["X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_SECRET"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_SECRET"],
    )


def main(dry_run: bool = False):
    state = load_state()

    if state.get("last_start_time") is None:
        cutoff = datetime.utcnow() - timedelta(hours=INITIAL_LOOKBACK_HOURS)
        state["last_start_time"] = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")
        log.info(
            "No prior state found — starting from a %d-hour lookback (%s) "
            "instead of the full historical dataset.",
            INITIAL_LOOKBACK_HOURS, state["last_start_time"],
        )

    log.info("Starting run. last_start_time=%s", state.get("last_start_time"))

    try:
        incidents = fetch_new_incidents(state.get("last_start_time"))
    except (RuntimeError, json.JSONDecodeError) as e:
        log.error("Failed to fetch incidents: %s", e)
        return

    log.info("Fetched %d new dispatch records.", len(incidents))

    seen_ids = set(state.get("seen_ids", []))
    new_incidents = [inc for inc in incidents if inc.get("incident_id") not in seen_ids]
    to_post = [inc for inc in new_incidents if matches_filter(inc)]
    log.info("%d incidents match alert filters.", len(to_post))

    client = None
    if not dry_run and to_post:
        client = get_x_client()

    posted_count = 0
    failed_start_times = []
    for inc in to_post:
        if posted_count >= MAX_POSTS_PER_RUN:
            log.warning("Hit MAX_POSTS_PER_RUN cap (%d); remaining incidents deferred to next run.", MAX_POSTS_PER_RUN)
            break

        text = format_post(inc)
        if dry_run:
            log.info("[DRY RUN] Would post:\n%s\n", text)
        else:
            try:
                client.create_tweet(text=text)
                log.info("Posted incident %s: %s", inc.get("incident_id"), inc.get("initial_type"))
            except Exception as e:
                log.error("Failed to post incident %s: %s", inc.get("incident_id"), e)
                # Deliberately NOT added to seen_ids, and its start_time is
                # kept out of the watermark below, so this incident gets
                # re-fetched and retried on the next run instead of being
                # silently dropped.
                failed_start_times.append(inc["start_time"])
                continue
            time.sleep(SECONDS_BETWEEN_POSTS)

        seen_ids.add(inc.get("incident_id"))
        posted_count += 1

    if not dry_run:
        # Mark every non-matching incident as handled too (not just posted
        # ones), so future runs don't keep re-evaluating the same old records.
        for inc in new_incidents:
            if not matches_filter(inc):
                seen_ids.add(inc.get("incident_id"))

        if failed_start_times:
            # Don't let the watermark pass the earliest failure — retry it
            # (and anything after it) next run instead of losing it silently.
            state["last_start_time"] = min(failed_start_times)
        elif incidents:
            state["last_start_time"] = incidents[-1]["start_time"]

        state["seen_ids"] = list(seen_ids)
        save_state(state)

    verb = "Would have posted" if dry_run else "Posted"
    log.info("Run complete. %s %d incident(s).", verb, posted_count)


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
