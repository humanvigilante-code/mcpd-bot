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
import math
import os
import subprocess
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import tweepy
except ImportError:
    tweepy = None

try:
    from staticmap import StaticMap, CircleMarker
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    StaticMap = None

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

# Attach a static map with a marker at the incident location, when
# coordinates are available. Uses free OpenStreetMap tiles — no API key,
# no extra cost. OSM's tile usage policy asks for a real User-Agent and
# reasonable request volume; fine for this bot's low daily post count, but
# if you scale this up a lot, switch to a paid tile provider (Mapbox, Stadia
# Maps) instead of hammering OSM's free servers.
ATTACH_MAP = True
MAP_SIZE = (1000, 650)   # size of the actual map area (excludes banner/legend)
MAP_ZOOM = 16             # higher = more zoomed in / more legible street detail
MAP_TILE_URL = "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"
MAP_USER_AGENT = "mcpd-crime-bot/1.0 (personal public-safety alert bot)"
TOP_BANNER_HEIGHT = 80     # incident type/time annotation bar
LEGEND_LINE_HEIGHT = 24     # per nearby-incident legend row
LEGEND_HEADER_HEIGHT = 34   # legend title row above the list

# Nearby-incident annotation: shows recent-area context (colored markers +
# a matching text legend) alongside the main incident (red marker). Just an
# extra free Socrata read per post — no added cost, only extra latency.
# Only genuinely serious nearby incidents are shown (same filter as the main
# alert), over a longer lookback since "nearby history" is meant to span
# further back than "what just happened."
NEARBY_RADIUS_METERS = 500
NEARBY_LOOKBACK_DAYS = 365
NEARBY_MAX_ANNOTATED = 10
NEARBY_FETCH_LIMIT = 200    # raw records pulled before filtering to serious types

# Main incident: red, full size. Nearby incidents: orange, half size — shape
# is what distinguishes one nearby cluster from another, not color, since
# every nearby marker uses the same orange.
MAIN_MARKER_COLOR = "#e8342a"
MAIN_MARKER_SIZE = 20
NEARBY_MARKER_COLOR = "#ff9900"
NEARBY_MARKER_SIZE = MAIN_MARKER_SIZE // 2

# Nearby incidents within this distance of EACH OTHER, or of the main
# incident itself, are treated as "the same place" and share one marker
# style instead of each getting its own — a repeated shape reappearing is
# meant to read as "this spot keeps coming up," not as visual noise from
# assigning unique styles to points that are really on top of each other.
# ~0.05 mi ≈ 260 ft, roughly a city block — small enough to mean "same
# building/corner," not "same neighborhood."
SAME_LOCATION_THRESHOLD_MILES = 0.05

# Reserved shape for nearby incidents essentially at the main incident's own
# location, so it reads as its own category ("happened right here before")
# and never collides with a regular cluster's shape — "circle" is excluded
# from the rotation below for exactly that reason.
AT_SCENE_STYLE = ("circle", NEARBY_MARKER_COLOR)

# Distinct shape per *cluster* of nearby incidents (color is constant now,
# so shape alone tells clusters apart). "circle" is deliberately left out —
# it's reserved for AT_SCENE_STYLE above.
NEARBY_MARKER_SHAPES = [
    "square",
    "triangle",
    "diamond",
    "plus",
    "x",
    "asterisk",
    "pentagon",
    "hexagon",
    "inverted_triangle",
]

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


def _socrata_get(params: dict) -> list[dict]:
    """Shared GET helper for the Socrata endpoint.

    data.montgomerycountymd.gov sits behind bot-protection that fingerprints
    the TLS handshake, not just headers — Python's `requests` (urllib3) gets
    blocked with a 403 even with a spoofed User-Agent, while plain `curl`
    (a different TLS signature) passes. So we shell out to curl instead of
    using `requests` for this call.

    No -f flag on purpose: -f hides the response body and status code on
    HTTP errors, which makes failures impossible to diagnose. Instead we
    append the status code with -w and check it ourselves.
    """
    params = dict(params)
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    url = SOCRATA_ENDPOINT + "?" + urllib.parse.urlencode(params)

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
    return _socrata_get(params)


def fetch_nearby_incidents(lat: float, lon: float, before_time: str, exclude_id: str | None = None) -> list[dict]:
    """Serious-category historical incidents (same filter as the main alert)
    within NEARBY_RADIUS_METERS of (lat, lon), in the NEARBY_LOOKBACK_DAYS
    before `before_time`, capped at NEARBY_MAX_ANNOTATED (most recent first).
    Used to give map viewers area context. Best-effort: callers should treat
    a raised exception as "no nearby data available" and fall back gracefully."""
    where = f"within_circle(geolocation, {lat}, {lon}, {NEARBY_RADIUS_METERS}) AND start_time < '{before_time}'"
    try:
        cutoff = datetime.fromisoformat(before_time) - timedelta(days=NEARBY_LOOKBACK_DAYS)
        where += f" AND start_time > '{cutoff.strftime('%Y-%m-%dT%H:%M:%S.000')}'"
    except ValueError:
        pass  # if before_time doesn't parse, just skip the lower bound

    params = {
        "$where": where,
        "$order": "start_time DESC",
        # Fetch a much larger raw pool than we'll show, since most dispatch
        # types (alarms, traffic, disturbances, etc.) get discarded by the
        # matches_filter() pass below — only serious ones are kept.
        "$limit": NEARBY_FETCH_LIMIT,
    }
    results = _socrata_get(params)
    if exclude_id:
        results = [r for r in results if r.get("incident_id") != exclude_id]
    results = [r for r in results if matches_filter(r)]
    return results[:NEARBY_MAX_ANNOTATED]


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


def _require_x_env() -> None:
    required = ["X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_SECRET"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def get_x_client() -> "tweepy.Client":
    if tweepy is None:
        raise RuntimeError("tweepy is not installed. Run: pip install tweepy")
    _require_x_env()

    return tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_SECRET"],
    )


def get_x_api_v1() -> "tweepy.API":
    """Media upload isn't available on tweepy's v2 Client yet, so use the
    older v1.1 API object (same OAuth 1.0a credentials) just for that step."""
    if tweepy is None:
        raise RuntimeError("tweepy is not installed. Run: pip install tweepy")
    _require_x_env()

    auth = tweepy.OAuth1UserHandler(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_SECRET"],
    )
    return tweepy.API(auth)


def _distance_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine great-circle distance in miles."""
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _lonlat_to_px(lon: float, lat: float, zoom: int, x_center: float, y_center: float,
                   width: int, height: int, tile_size: int = 256) -> tuple[float, float]:
    """Same Web Mercator tile-space projection staticmap uses internally to
    place its own markers, replicated here (rather than reaching into
    staticmap's private API) so we can draw custom shapes at the exact pixel
    spot a marker at (lon, lat) would land, after the map has been rendered
    centered/zoomed around the main incident."""
    x = ((lon + 180.0) / 360.0) * (2 ** zoom)
    y = (1 - math.log(math.tan(lat * math.pi / 180) + 1 / math.cos(lat * math.pi / 180)) / math.pi) / 2 * (2 ** zoom)
    px = (x - x_center) * tile_size + width / 2
    py = (y - y_center) * tile_size + height / 2
    return px, py


def _draw_shape(draw: "ImageDraw.ImageDraw", shape: str, cx: float, cy: float, size: float, color: str) -> None:
    """Draws one of several distinct marker shapes centered at (cx, cy). Each
    draws a white halo first for contrast against whatever's underneath
    (map tiles of varying color, or the light legend background)."""
    s = size
    halo = "white"

    def poly(points, w=2):
        draw.polygon(points, fill=color, outline=halo, width=w)

    if shape == "square":
        draw.rectangle([cx - s, cy - s, cx + s, cy + s], fill=color, outline=halo, width=2)
    elif shape == "triangle":
        poly([(cx, cy - s * 1.15), (cx + s * 1.05, cy + s * 0.85), (cx - s * 1.05, cy + s * 0.85)])
    elif shape == "inverted_triangle":
        poly([(cx, cy + s * 1.15), (cx + s * 1.05, cy - s * 0.85), (cx - s * 1.05, cy - s * 0.85)])
    elif shape == "diamond":
        poly([(cx, cy - s * 1.2), (cx + s * 1.2, cy), (cx, cy + s * 1.2), (cx - s * 1.2, cy)])
    elif shape == "pentagon":
        pts = [
            (cx + s * 1.15 * math.cos(math.radians(-90 + i * 72)),
             cy + s * 1.15 * math.sin(math.radians(-90 + i * 72)))
            for i in range(5)
        ]
        poly(pts)
    elif shape == "hexagon":
        pts = [
            (cx + s * 1.1 * math.cos(math.radians(i * 60)),
             cy + s * 1.1 * math.sin(math.radians(i * 60)))
            for i in range(6)
        ]
        poly(pts)
    elif shape == "plus":
        w = s * 0.5
        draw.rectangle([cx - w - 2, cy - s * 1.2 - 2, cx + w + 2, cy + s * 1.2 + 2], fill=halo)
        draw.rectangle([cx - s * 1.2 - 2, cy - w - 2, cx + s * 1.2 + 2, cy + w + 2], fill=halo)
        draw.rectangle([cx - w, cy - s * 1.2, cx + w, cy + s * 1.2], fill=color)
        draw.rectangle([cx - s * 1.2, cy - w, cx + s * 1.2, cy + w], fill=color)
    elif shape == "x":
        lw = max(3, int(s * 0.5))
        for dx, dy in [(1, 1), (1, -1)]:
            draw.line([(cx - s * 1.1 * dx, cy - s * 1.1 * dy), (cx + s * 1.1 * dx, cy + s * 1.1 * dy)],
                      fill=halo, width=lw + 4)
        for dx, dy in [(1, 1), (1, -1)]:
            draw.line([(cx - s * 1.1 * dx, cy - s * 1.1 * dy), (cx + s * 1.1 * dx, cy + s * 1.1 * dy)],
                      fill=color, width=lw)
    elif shape == "asterisk":
        lw = max(3, int(s * 0.35))
        for ang_deg in (0, 60, 120):
            ang = math.radians(ang_deg)
            dx, dy = math.cos(ang) * s * 1.25, math.sin(ang) * s * 1.25
            draw.line([(cx - dx, cy - dy), (cx + dx, cy + dy)], fill=halo, width=lw + 4)
        for ang_deg in (0, 60, 120):
            ang = math.radians(ang_deg)
            dx, dy = math.cos(ang) * s * 1.25, math.sin(ang) * s * 1.25
            draw.line([(cx - dx, cy - dy), (cx + dx, cy + dy)], fill=color, width=lw)
    else:  # "circle" and any unrecognized shape name
        draw.ellipse([cx - s, cy - s, cx + s, cy + s], fill=color, outline=halo, width=2)


def _load_font(size: int) -> "ImageFont.FreeTypeFont":
    """Tries common bold font paths on macOS and the Ubuntu GitHub Actions
    runner, falling back to PIL's built-in bitmap font if none are found."""
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",           # macOS
        "/System/Library/Fonts/Helvetica.ttc",                          # macOS
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",         # Ubuntu
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",  # Ubuntu
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    try:
        return ImageFont.load_default(size=size)  # Pillow >= 10.1
    except TypeError:
        return ImageFont.load_default()


def generate_map_image(
    latitude: float,
    longitude: float,
    incident_type: str | None = None,
    time_str: str | None = None,
    nearby: list[dict] | None = None,
) -> str:
    """Renders a map with a red marker at (latitude, longitude), an optional
    top banner annotating the incident type/time, and — if `nearby` incidents
    are passed in — a distinctly shaped-and-colored marker per nearby
    incident plus a matching legend at the bottom (type, time, distance).
    Saves to a temp PNG and returns the file path; caller must delete it
    after use.

    Only the main incident is added as a staticmap marker (so the map is
    always centered exactly on it); nearby markers are drawn afterward with
    PIL at their correctly projected pixel positions, since staticmap's own
    marker types don't support shapes beyond circles/icons."""
    if StaticMap is None:
        raise RuntimeError("staticmap is not installed. Run: pip install staticmap")

    m = StaticMap(*MAP_SIZE, url_template=MAP_TILE_URL, headers={"User-Agent": MAP_USER_AGENT})
    m.add_marker(CircleMarker((longitude, latitude), MAIN_MARKER_COLOR, MAIN_MARKER_SIZE))

    map_image = m.render(zoom=MAP_ZOOM).convert("RGB")
    map_w, map_h = map_image.size

    # Cluster nearby incidents by location before assigning styles: anything
    # within SAME_LOCATION_THRESHOLD_MILES of the main incident gets the
    # reserved AT_SCENE_STYLE; anything within that same distance of an
    # already-seen nearby cluster joins that cluster's style; only a genuinely
    # new location gets the next unused (shape, color) from the palettes.
    valid_nearby = []
    clusters = []  # [{"lat": float, "lon": float, "style": (shape, color)}, ...]
    next_style_idx = 0
    for nb in (nearby or []):
        try:
            nb_lat, nb_lon = float(nb.get("latitude", 0)), float(nb.get("longitude", 0))
        except (TypeError, ValueError):
            continue
        if not (nb_lat and nb_lon):
            continue

        if _distance_miles(latitude, longitude, nb_lat, nb_lon) <= SAME_LOCATION_THRESHOLD_MILES:
            shape, color = AT_SCENE_STYLE
        else:
            match = next(
                (c for c in clusters if _distance_miles(c["lat"], c["lon"], nb_lat, nb_lon) <= SAME_LOCATION_THRESHOLD_MILES),
                None,
            )
            if match:
                shape, color = match["style"]
            else:
                shape = NEARBY_MARKER_SHAPES[next_style_idx % len(NEARBY_MARKER_SHAPES)]
                color = NEARBY_MARKER_COLOR
                clusters.append({"lat": nb_lat, "lon": nb_lon, "style": (shape, color)})
                next_style_idx += 1

        valid_nearby.append((nb, nb_lat, nb_lon, shape, color))

    top_h = TOP_BANNER_HEIGHT if incident_type else 0
    bottom_h = (LEGEND_HEADER_HEIGHT + len(valid_nearby) * LEGEND_LINE_HEIGHT + 10) if valid_nearby else 0

    canvas = Image.new("RGB", (map_w, top_h + map_h + bottom_h), "white")
    canvas.paste(map_image, (0, top_h))
    draw = ImageDraw.Draw(canvas)

    # Draw nearby shapes on the map now that they're pasted at their real
    # offset (top_h). Points that fall outside the visible map area (rare,
    # since NEARBY_RADIUS_METERS is small relative to the zoom level) are
    # simply skipped on the map but still listed in the legend below.
    for nb, nb_lat, nb_lon, shape, color in valid_nearby:
        px, py = _lonlat_to_px(nb_lon, nb_lat, m.zoom, m.x_center, m.y_center, map_w, map_h)
        if 0 <= px <= map_w and 0 <= py <= map_h:
            _draw_shape(draw, shape, px, py + top_h, NEARBY_MARKER_SIZE, color)

    if incident_type:
        draw.rectangle([0, 0, map_w, top_h], fill="#8b1a1a")
        draw.text((16, 10), incident_type.upper()[:60], font=_load_font(26), fill="white")
        if time_str:
            draw.text((16, 46), time_str, font=_load_font(17), fill="#f2d5d5")

    if valid_nearby:
        y0 = top_h + map_h
        draw.rectangle([0, y0, map_w, y0 + bottom_h], fill="#f2f2f2")
        draw.text(
            (14, y0 + 8),
            f"Nearby serious incidents, last {NEARBY_LOOKBACK_DAYS} days "
            f"(shapes match map markers; repeated shape = same location):",
            font=_load_font(16), fill="#333333",
        )
        y = y0 + LEGEND_HEADER_HEIGHT
        for nb, nb_lat, nb_lon, shape, color in valid_nearby:
            dist = _distance_miles(latitude, longitude, nb_lat, nb_lon)
            try:
                nb_time = datetime.fromisoformat(nb["start_time"]).strftime("%b %d, %Y, %I:%M %p")
            except Exception:
                nb_time = nb.get("start_time", "")
            _draw_shape(draw, shape, 27, y + 9, NEARBY_MARKER_SIZE - 1, color)
            label = f"{nb.get('initial_type', 'Incident').title()[:45]} — {nb_time} ({dist:.1f} mi)"
            draw.text((44, y), label, font=_load_font(15), fill="#333333")
            y += LEGEND_LINE_HEIGHT

    fd, path = tempfile.mkstemp(suffix=".png", prefix="mcpd_map_")
    os.close(fd)
    canvas.save(path)
    return path


def main(dry_run: bool = False):
    state = load_state()

    if state.get("last_start_time") is None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=INITIAL_LOOKBACK_HOURS)
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
    api_v1 = None
    if not dry_run and to_post:
        client = get_x_client()
        if ATTACH_MAP and StaticMap is not None:
            api_v1 = get_x_api_v1()

    posted_count = 0
    failed_start_times = []
    for inc in to_post:
        if posted_count >= MAX_POSTS_PER_RUN:
            log.warning("Hit MAX_POSTS_PER_RUN cap (%d); remaining incidents deferred to next run.", MAX_POSTS_PER_RUN)
            break

        text = format_post(inc)

        # Try to build a map image for this incident. Missing/zero coordinates
        # (some records have "0"/"0" instead of a real geocode) or a rendering
        # failure just falls back to a text-only post — never blocks the alert.
        # Nearby-incident lookup is similarly best-effort: if it fails, the
        # map is still built, just without the orange markers/legend.
        map_path = None
        nearby_count = 0
        try:
            lat, lon = float(inc.get("latitude", 0)), float(inc.get("longitude", 0))
            if ATTACH_MAP and StaticMap is not None and lat and lon:
                nearby = []
                try:
                    nearby = fetch_nearby_incidents(lat, lon, inc["start_time"], exclude_id=inc.get("incident_id"))
                except Exception as e:
                    log.warning("Could not fetch nearby incidents for %s: %s", inc.get("incident_id"), e)
                nearby_count = len(nearby)

                try:
                    time_str_for_map = datetime.fromisoformat(inc["start_time"]).strftime("%b %d, %I:%M %p")
                except Exception:
                    time_str_for_map = inc.get("start_time")

                map_path = generate_map_image(
                    lat, lon,
                    incident_type=inc.get("initial_type", "Incident").title(),
                    time_str=time_str_for_map,
                    nearby=nearby,
                )
        except Exception as e:
            log.warning("Could not build map for incident %s: %s", inc.get("incident_id"), e)
            map_path = None

        if dry_run:
            log.info("[DRY RUN] Would post (map=%s, nearby=%d):\n%s\n", bool(map_path), nearby_count, text)
            if map_path:
                os.remove(map_path)
        else:
            try:
                media_ids = None
                if map_path:
                    media = api_v1.media_upload(map_path)
                    media_ids = [media.media_id]
                client.create_tweet(text=text, media_ids=media_ids)
                log.info(
                    "Posted incident %s: %s (map=%s, nearby=%d)",
                    inc.get("incident_id"), inc.get("initial_type"), bool(map_path), nearby_count,
                )
            except Exception as e:
                log.error("Failed to post incident %s: %s", inc.get("incident_id"), e)
                # Deliberately NOT added to seen_ids, and its start_time is
                # kept out of the watermark below, so this incident gets
                # re-fetched and retried on the next run instead of being
                # silently dropped.
                failed_start_times.append(inc["start_time"])
                continue
            finally:
                if map_path:
                    os.remove(map_path)
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


def test_map(coords: str | None = None):
    """Standalone check: render one sample map and save it locally so you can
    open and look at it, with no X credentials or state involved. Optional
    coords arg like '39.0840,-77.1528' (lat,lon); defaults to a point in
    Rockville, MD if omitted. Attempts a real nearby-incidents lookup (best
    effort) so the preview matches what a live post would actually look like."""
    if coords:
        lat_str, lon_str = coords.split(",")
        lat, lon = float(lat_str), float(lon_str)
    else:
        lat, lon = 39.0840, -77.1528  # Rockville, MD — the county seat

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000")
    nearby = []
    try:
        nearby = fetch_nearby_incidents(lat, lon, now_str)
        print(f"Found {len(nearby)} real nearby incident(s) in the last {NEARBY_LOOKBACK_DAYS} days to annotate.")
    except Exception as e:
        print(f"(Could not fetch real nearby incidents for this preview — showing map without them: {e})")

    out_path = Path(__file__).parent / "test_map.png"
    tmp_path = generate_map_image(
        lat, lon,
        incident_type="Sample: Robbery",
        time_str=datetime.now().strftime("%b %d, %I:%M %p") + " (sample — not a real incident)",
        nearby=nearby,
    )
    Path(tmp_path).replace(out_path)
    print(f"Map saved to: {out_path}")
    print(f"(marker at lat={lat}, lon={lon})")


if __name__ == "__main__":
    if "--test-map" in sys.argv:
        idx = sys.argv.index("--test-map")
        coords_arg = sys.argv[idx + 1] if len(sys.argv) > idx + 1 and "," in sys.argv[idx + 1] else None
        test_map(coords_arg)
    else:
        dry_run = "--dry-run" in sys.argv
        main(dry_run=dry_run)
