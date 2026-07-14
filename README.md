# MCPD Crime Alert Bot

Polls Montgomery County, MD's public dispatch data every run and auto-posts new,
alert-worthy incidents to your X account — no manual approval per post.

## How it works

1. `mcpd_crime_bot.py` queries the county's open data feed
   (`data.montgomerycountymd.gov`, dataset `98cc-bc7d`, "Police Dispatched
   Incidents") for any incidents dispatched since the last run.
2. It filters to categories worth alerting on (robbery, assault, burglary,
   shooting, weapons, carjacking, etc. — edit the list in the script) and
   skips alarm calls by default, since those are frequently false alarms.
3. It posts each matching incident to X and remembers what it already posted
   in `state.json`, so nothing gets duplicated.
4. You run it on a schedule (every 5–15 minutes) via cron or GitHub Actions.

Test run first (`python mcpd_crime_bot.py --dry-run`) — logs what it *would*
post without touching X or your credentials.

## One real tradeoff to decide

The dataset has both `initial_type` (the category dispatchers assign when the
call comes in) and `close_type`/`disposition_desc` (assigned after officers
respond and the call is closed — e.g. an "ALARM - ROBBERY" that turns out to
be a false alarm). The script currently posts on `initial_type`, favoring
speed. That means a small percentage of posts could later prove to be a false
alarm or reclassified incident. If accuracy matters more than the extra few
minutes of delay, switch the filter to wait for `end_time` to be populated
and check `disposition_desc` instead — I can make that change if you want it.

## Setup

### 1. Get X API access

- Go to [developer.x.com](https://developer.x.com), sign up, create a Project + App.
- Set the app's permissions to **Read and Write**.
- Generate: API Key & Secret, and Access Token & Secret (make sure you
  regenerate the access token *after* setting Read/Write permissions, or it
  will be read-only).
- Map console names to the .env variables: **Consumer Key** → `X_API_KEY`,
  **Consumer Key Secret** → `X_API_SECRET`, **Access Token** → `X_ACCESS_TOKEN`,
  **Access Token Secret** → `X_ACCESS_SECRET`. The **Bearer Token** is not used
  by this script (it's for app-only/read requests; posting needs the OAuth 1.0a
  user-context credentials above).
- Put real values only in your local `.env` file (already gitignored) — never
  in this README or any other tracked file.

- X API billing is pay-per-use now (no free tier for new developer accounts as
  of Feb 2026) — you'll need to add a payment method and buy credits in the
  [Developer Console](https://console.x.com). Set a **spending limit** there
  so costs can't run away.

### 2. Install and configure

```bash
cd mcpd_crime_bot
pip install -r requirements.txt
cp .env.example .env
# edit .env and paste in your X credentials
python mcpd_crime_bot.py --dry-run   # sanity check, no posting
python mcpd_crime_bot.py             # live run
```

### 3. Schedule it — pick one

**Option A: Your own always-on machine (simplest, $0 extra)**
Add a cron entry, e.g. every 10 minutes:
```
*/10 * * * * cd /path/to/mcpd_crime_bot && /usr/bin/python3 mcpd_crime_bot.py >> cron.log 2>&1
```

**Option B: GitHub Actions (free, no computer to keep on)**
Push this folder to a GitHub repo, move `post-alerts.yml` into
`.github/workflows/post-alerts.yml`, add your credentials as repo Secrets, and
GitHub runs it every 10 minutes for you. Full instructions are in comments at
the top of that file — note `state.json` needs to be committed (not
gitignored) for this option, since GitHub Actions runners don't persist files
between runs on their own.

## Cost breakdown

**X API (pay-per-use, per X's official pricing as of Feb 2026):**
| Item | Cost |
|---|---|
| Post created (no link) | $0.015 each |
| Post created (with a link) | $0.20 each |
| No monthly minimum, no subscription | — |

The template in this script doesn't include links, so you're on the $0.015/post
rate. At realistic filtered volume (serious-crime categories only, excluding
alarms/traffic/minor calls) Montgomery County typically sees somewhere in the
range of 10–40 qualifying incidents a day depending on your keyword list —
roughly:

| Posts/month | X API cost |
|---|---|
| 300 (~10/day) | ~$4.50 |
| 900 (~30/day) | ~$13.50 |
| 3,000 (unfiltered, all dispatches) | ~$45 |

You'll need to buy X API credits upfront in the Developer Console; check the
console for the current minimum purchase amount.

**County data feed:** free, no key required (an optional free "app token"
just raises your rate limit — get one at
data.montgomerycountymd.gov/profile/app_tokens).

**Hosting:** $0 on GitHub Actions (well within the free-tier minutes for a
10-minute cron) or $0 extra if you run it on a computer you already leave on.

**Realistic total: roughly $5–$15/month** if you keep the filter to genuinely
serious incident types, plus whatever your Developer Console's minimum credit
purchase is.

## Notes on the data

- Update cadence: incidents in the feed showed timestamps from within the
  past hour when I checked, so "every 10 minutes" is a reasonable alert
  interval — this is dispatch data, not a delayed report.
- No victim/suspect names are in this dataset — only category, block-level
  address, and timing. That keeps the bot from posting anything defamatory
  or identifying.
- `initial_type` is assigned by the 911 dispatcher before anyone arrives on
  scene, so it's a preliminary category, not a confirmed charge.
