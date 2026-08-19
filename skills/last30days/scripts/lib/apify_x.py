"""X search backend via an Apify tweet-scraper actor through an api-dispatch gateway.

Runs an Apify pay-per-result tweet-scraper actor (see ``ACTOR_ID``) through a
private api-dispatch gateway instead of calling Apify directly: the gateway
holds the Apify credentials, spins them, and meters spend. Auth is two env values, resolved from the skill config, the process
environment, or an optional env file:

    API_DISPATCH_SERVICE_URL   gateway base URL
    API_DISPATCH_SERVICE_KEY   gateway service key
    API_DISPATCH_ENV_FILE      optional path to a .env file holding the two
                               values above (checked last)

Note: this backend is named "apify" in the X chain. It is unrelated to the
legacy ``env.is_apify_available`` alias, which points at the ScrapeCreators
TikTok source for backward compatibility.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import http, log
from .relevance import token_overlap_relevance as _compute_relevance
from .xquik import expand_xquik_queries as _expand_queries

# Actor executed through the gateway. Pay-per-result; never a rental actor
# (the gateway hard-fails rental-tier actors on free-plan tokens). NOT
# apidojo/tweet-scraper: that actor gates free-plan Apify tokens (returns
# only {"noResults": true} sentinels plus an upsell status message, verified
# 2026-08-14). The kaitoeasyapi actor exposes the same input/output shape
# and works on free-plan tokens.
ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"

# Depth configurations: total results cap and number of query variants.
DEPTH_CONFIG = {
    "quick": {"limit": 12, "queries": 1},
    "default": {"limit": 30, "queries": 2},
    "deep": {"limit": 60, "queries": 3},
}

# Optional likes floor for TOPIC search only (not handle/mention lookups,
# where the whole point is a named account whatever its reach). X ranks a
# narrow query's "Top" tab off a small pool, so a niche topic returns
# small-account posts; this actor also bills per result, so an operator floor
# both sharpens the pool and stops paying for the noise. Unset or 0 = off, so
# no existing caller changes behaviour.
MIN_FAVES_VAR = "LAST30DAYS_X_MIN_FAVES"

URL_VAR = "API_DISPATCH_SERVICE_URL"
KEY_VAR = "API_DISPATCH_SERVICE_KEY"
ENV_FILE_VAR = "API_DISPATCH_ENV_FILE"

# Apify caps waitForFinish at 60s; stay just under it so the gateway's
# single apify HTTP call returns the finished run in the common case.
_WAIT_FOR_FINISH = 55
# How long the gateway holds the /api/execute request for the job result.
_EXECUTE_WAIT = 90
# Ceiling on post-run polling (runs that outlive waitForFinish), per call.
_POLL_DEADLINE = 120
_POLL_INTERVAL = 5
# Hard ceiling on the WALL CLOCK of one _run_search_terms call. The nested
# poll loops each carried their own _POLL_DEADLINE, so a single call could
# stack execute + envelope poll + run poll + dataset fetch into ~10 minutes
# while holding a pipeline worker thread. One budget bounds the whole call and
# clamps every HTTP timeout to what is left of it.
_CALL_DEADLINE = 300
# Floor for a clamped socket timeout, so the last request in the budget still
# gets a fair chance instead of being issued with ~0s.
_MIN_TIMEOUT = 5
# One attempt per gateway request. http.request's default retries=5 runs its
# own retry loop with exponential backoff sleeps that _clamped() cannot see:
# with retries=5 a single _execute burns 5 x timeout + 30s of backoff (measured
# 630s against this 300s budget), so the budget above only bounds the call when
# each request is issued once. Retrying is not lost — the poll loops re-issue
# under the budget, and a transient failure fails over to the next X backend.
_HTTP_RETRIES = 1

_env_file_cache: Optional[Dict[str, str]] = None


def _log(msg: str):
    log.source_log("ApifyX", msg, tty_only=False)


def _env_file_values() -> Dict[str, str]:
    """Values from the optional API_DISPATCH_ENV_FILE, parsed once per process."""
    global _env_file_cache
    if _env_file_cache is not None:
        return _env_file_cache
    values: Dict[str, str] = {}
    path = os.environ.get(ENV_FILE_VAR, "").strip()
    if path:
        try:
            for line in Path(path).read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                values[key.strip()] = val.strip().strip('"').strip("'")
        except OSError as exc:
            _log(f"cannot read {ENV_FILE_VAR}={path}: {exc}")
    _env_file_cache = values
    return values


def _resolve(config: Optional[Dict[str, Any]], var: str) -> str:
    """Resolve a gateway setting: skill config > process env > env file."""
    for source in (config or {}, os.environ, _env_file_values()):
        val = str(source.get(var) or "").strip()
        if val:
            return val
    return ""


def _min_faves(config: Optional[Dict[str, Any]] = None) -> int:
    """Likes floor, resolved like every other setting here: skill config >
    process env > env file. 0 (off) when unset or unparseable.

    Reading os.environ alone would have made the documented
    ``~/.config/last30days/.env`` path dead for this knob (the same trap the
    API_DISPATCH keys hit before they were registered in env.py), leaving a
    user who set a floor there still paying per result for the noise it was
    meant to cut.
    """
    raw = _resolve(config, MIN_FAVES_VAR)
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        _log(f"ignoring non-integer {MIN_FAVES_VAR}={raw!r}")
        return 0


def gateway_config(config: Optional[Dict[str, Any]] = None) -> tuple[str, str]:
    """Return (base_url, service_key); empty strings when unconfigured."""
    return _resolve(config, URL_VAR), _resolve(config, KEY_VAR)


def is_available(config: Optional[Dict[str, Any]] = None) -> bool:
    """True when both gateway URL and key resolve (no network call)."""
    url, key = gateway_config(config)
    return bool(url and key)


def _remaining(deadline: Optional[float]) -> float:
    """Seconds left in the call budget (inf when no budget is in force)."""
    if deadline is None:
        return float("inf")
    return deadline - time.monotonic()


def _clamped(timeout: int, deadline: Optional[float]) -> int:
    """A socket timeout that cannot outlive the call budget."""
    left = _remaining(deadline)
    if left == float("inf"):
        return timeout
    return max(_MIN_TIMEOUT, min(timeout, int(left)))


def _execute(
    config: Optional[Dict[str, Any]],
    service_input: Dict[str, Any],
    wait: int,
    timeout: int,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    """One POST /api/execute round-trip. Raises http.HTTPError on HTTP failure."""
    url, key = gateway_config(config)
    return http.post(
        f"{url.rstrip('/')}/api/execute",
        {"service": "apify", "input": service_input, "wait": wait},
        headers={"x-service-key": key},
        timeout=_clamped(timeout, deadline),
        retries=_HTTP_RETRIES,
    )


def _get_job(
    config: Optional[Dict[str, Any]], job_id: str, deadline: Optional[float] = None
) -> Dict[str, Any]:
    """Fetch a gateway job envelope (GET /api/jobs/apify/{jobId})."""
    url, key = gateway_config(config)
    wrapper = http.get(
        f"{url.rstrip('/')}/api/jobs/apify/{job_id}",
        headers={"x-service-key": key},
        timeout=_clamped(30, deadline),
        retries=_HTTP_RETRIES,
    )
    job = wrapper.get("job")
    return job if isinstance(job, dict) else wrapper


def _await_envelope(
    config: Optional[Dict[str, Any]],
    envelope: Dict[str, Any],
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    """Poll a queued/running gateway job until it settles or the deadline hits.

    The /api/execute ``wait`` usually covers the job, but a busy gateway queue
    can hand back a still-running envelope; this closes that gap. ``deadline``
    is the shared call budget (see ``_CALL_DEADLINE``); the local
    ``_POLL_DEADLINE`` only ever shortens it, never extends it.
    """
    local = time.monotonic() + _POLL_DEADLINE
    stop = local if deadline is None else min(local, deadline)
    while envelope.get("status") in ("queued", "running"):
        job_id = str(envelope.get("jobId") or "")
        if not job_id or time.monotonic() + _POLL_INTERVAL >= stop:
            return {
                **envelope,
                "error": f"gateway job {job_id or '?'} still "
                         f"{envelope.get('status')} after poll deadline",
            }
        time.sleep(_POLL_INTERVAL)
        envelope = _get_job(config, job_id, deadline=stop)
    return envelope


def _envelope_error(envelope: Dict[str, Any]) -> str:
    """Non-empty when a gateway job envelope reports failure."""
    if envelope.get("error"):
        return str(envelope["error"])
    if envelope.get("status") == "failed":
        return "gateway job failed"
    return ""


def _run_data(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """The Apify run object ({data: {...}}) inside a completed job envelope."""
    result = envelope.get("result")
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            return data
    return {}


def _await_run(
    config: Optional[Dict[str, Any]],
    run: Dict[str, Any],
    deadline: Optional[float] = None,
) -> tuple[Dict[str, Any], str]:
    """Poll a not-yet-finished Apify run until it settles or the deadline hits.

    Returns (run_data, error). ``error`` is non-empty on failure/timeout.
    ``deadline`` is the shared call budget; the local ``_POLL_DEADLINE`` only
    shortens it. Every nested wait is bounded by the same ``stop``, so one
    iteration can no longer overshoot the budget by its own poll deadline.
    """
    local = time.monotonic() + _POLL_DEADLINE
    stop = local if deadline is None else min(local, deadline)
    status = str(run.get("status") or "")
    run_id = str(run.get("id") or "")
    while status in ("READY", "RUNNING") and run_id:
        if time.monotonic() + _POLL_INTERVAL >= stop:
            return run, f"actor run {run_id} still {status} after poll deadline"
        time.sleep(_POLL_INTERVAL)
        envelope = _await_envelope(config, _execute(
            config, {"kind": "runStatus", "runId": run_id}, wait=30, timeout=60,
            deadline=stop,
        ), deadline=stop)
        err = _envelope_error(envelope)
        if err:
            return run, err
        run = _run_data(envelope)
        status = str(run.get("status") or "")
    if status != "SUCCEEDED":
        return run, f"actor run finished as {status or 'unknown'}"
    return run, ""


def _fetch_dataset(
    config: Optional[Dict[str, Any]],
    dataset_id: str,
    limit: int,
    deadline: Optional[float] = None,
) -> tuple[List[Dict[str, Any]], str]:
    """Fetch a finished run's dataset items. Returns (items, error)."""
    envelope = _await_envelope(config, _execute(
        config,
        {
            "kind": "dataset",
            "datasetId": dataset_id,
            "options": {"clean": True, "limit": limit},
        },
        wait=60,
        timeout=90,
        deadline=deadline,
    ), deadline=deadline)
    err = _envelope_error(envelope)
    if err:
        return [], err
    result = envelope.get("result")
    if not isinstance(result, list):
        return [], ""
    return [row for row in result if isinstance(row, dict)], ""


def _fatal_http(exc: Exception) -> str | None:
    """Non-empty when a gateway HTTP failure is permanent for this run.

    402 is as fatal as 401/403 here: the gateway fronts a PAY-PER-RESULT
    actor, so an exhausted Apify balance / gateway quota is a spend failure
    that will repeat for every stream in the run. Swallowing it as transient
    made the pipeline report "X returned nothing" instead of failing over with
    an honest error (xquik._execute_search already treats 402 this way).
    """
    status = getattr(exc, "status_code", None)
    if status == 402:
        return "api-dispatch/apify quota exhausted (402)"
    if status in (401, 403):
        return f"api-dispatch auth failed ({status})"
    return None


def _run_search_terms(
    search_terms: List[str],
    max_items: int,
    config: Optional[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], str | None]:
    """Run one actor invocation for a batch of search terms.

    Returns ``(raw_tweets, fatal_error)``. Fatal errors (gateway auth,
    failed run) come back as a string so the caller can settle honestly;
    transient HTTP errors log and return ``([], None)``.
    """
    if not is_available(config):
        return [], f"No {URL_VAR}/{KEY_VAR} configured"
    deadline = time.monotonic() + _CALL_DEADLINE
    try:
        envelope = _execute(
            config,
            {
                "kind": "run",
                "actorId": ACTOR_ID,
                "input": {
                    "searchTerms": search_terms,
                    "maxItems": max_items,
                    "queryType": "Top",
                },
                "options": {"waitForFinish": _WAIT_FOR_FINISH},
            },
            wait=_EXECUTE_WAIT,
            timeout=_EXECUTE_WAIT + 30,
            deadline=deadline,
        )
    except http.HTTPError as exc:
        fatal = _fatal_http(exc)
        if fatal:
            return [], fatal
        _log(f"HTTP error from gateway: {exc}")
        return [], None
    except Exception as exc:
        _log(f"gateway error: {exc}")
        return [], None

    try:
        envelope = _await_envelope(config, envelope, deadline=deadline)
        err = _envelope_error(envelope)
        if err:
            return [], f"apify run failed: {err}"

        run, err = _await_run(config, _run_data(envelope), deadline=deadline)
        if err:
            return [], f"apify run failed: {err}"
        dataset_id = str(run.get("defaultDatasetId") or "")
        if not dataset_id:
            return [], "apify run has no defaultDatasetId"
        rows, err = _fetch_dataset(config, dataset_id, max_items, deadline=deadline)
        if err:
            return [], f"apify dataset fetch failed: {err}"
        return rows, None
    except http.HTTPError as exc:
        fatal = _fatal_http(exc)
        if fatal:
            return [], fatal
        _log(f"HTTP error from gateway: {exc}")
        return [], None
    except Exception as exc:
        _log(f"gateway error: {exc}")
        return [], None


def _looks_like_tweet(row: Dict[str, Any]) -> bool:
    """Filter apidojo sentinel rows ({"noResults": true}) and junk."""
    if row.get("noResults"):
        return False
    return bool(row.get("id") or row.get("url") or row.get("twitterUrl"))


def search_x(
    topic: str,
    from_date: str,
    to_date: str,
    depth: str = "default",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Search X via the Apify tweet scraper through the gateway.

    Args:
        topic: Search topic
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        depth: Research depth - "quick", "default", or "deep"
        config: Skill config dict (gateway settings resolve through it)

    Returns:
        Dict with "items" list and optional "error" string.
    """
    if not is_available(config):
        return {"items": [], "error": f"No {URL_VAR}/{KEY_VAR} configured"}

    cfg = DEPTH_CONFIG.get(depth, DEPTH_CONFIG["default"])
    queries = _expand_queries(topic, depth)
    floor = _min_faves(config)
    # Operators appended to every topic query: the date window, plus the
    # optional likes floor.
    operators = f" since:{from_date} until:{to_date}"
    if floor:
        operators += f" min_faves:{floor}"
    terms = [f"{q}{operators}" for q in queries]
    _log(f"Searching: {', '.join(queries)}{f' (min_faves:{floor})' if floor else ''}")
    rows, fatal = _run_search_terms(terms, cfg["limit"], config)
    if fatal:
        return {"items": [], "error": fatal}

    items: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in rows:
        if not _looks_like_tweet(row):
            continue
        tweet_id = str(row.get("id", ""))
        if tweet_id and tweet_id in seen_ids:
            continue
        seen_ids.add(tweet_id)
        item = _parse_tweet(row, len(items), topic)
        if item:
            items.append(item)
    return {"items": items}


def _is_own(url: str, handle: str) -> bool:
    """True when a tweet URL is authored by ``handle`` (their own post)."""
    u = (url or "").lower()
    h = handle.lower().lstrip("@").strip()
    return bool(h) and (f"x.com/{h}/status" in u or f"twitter.com/{h}/status" in u)


def search_handles(
    handles: List[str],
    topic: str,
    from_date: str,
    to_date: str,
    *,
    count_per: int = 8,
    config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """FROM lane: tweets authored BY each handle (their own timeline).

    The topic is NOT AND'd into the query (the from:-AND bug, #610) — we pull
    the raw timeline and use ``topic`` for relevance ranking only. All handles
    batch into one actor run (one run per lane, not per handle).
    """
    if not is_available(config) or not handles:
        return []
    clean = [str(h).lstrip("@").strip() for h in handles]
    clean = [h for h in clean if h]
    if not clean:
        return []
    terms = [f"from:{h} since:{from_date} until:{to_date}" for h in clean]
    _log(f"Searching: {len(clean)} FROM-lane handle(s)")
    rows, fatal = _run_search_terms(terms, count_per * len(clean), config)
    if fatal:
        _log(f"FROM lane failed: {fatal}")
        return []
    items: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in rows:
        if not _looks_like_tweet(row):
            continue
        tweet_id = str(row.get("id", ""))
        if tweet_id and tweet_id in seen_ids:
            continue
        seen_ids.add(tweet_id)
        item = _parse_tweet(row, len(items), topic, id_prefix="XF")
        if item:
            items.append(item)
    return items


def search_mentions(
    handles: List[str],
    from_date: str,
    to_date: str,
    *,
    topic: str = "",
    count_per: int = 5,
    config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """ABOUT lane: tweets mentioning each handle, authored by OTHERS.

    Queries ``@handle`` then drops tweets authored by any queried handle so
    only third-party mentions remain.
    """
    if not is_available(config) or not handles:
        return []
    clean = [str(h).lstrip("@").strip() for h in handles]
    clean = [h for h in clean if h]
    if not clean:
        return []
    terms = [f"@{h} since:{from_date} until:{to_date}" for h in clean]
    _log(f"Searching: {len(clean)} ABOUT-lane handle(s)")
    rows, fatal = _run_search_terms(terms, count_per * len(clean), config)
    if fatal:
        _log(f"ABOUT lane failed: {fatal}")
        return []
    items: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in rows:
        if not _looks_like_tweet(row):
            continue
        tweet_id = str(row.get("id", ""))
        if tweet_id and tweet_id in seen_ids:
            continue
        seen_ids.add(tweet_id)
        item = _parse_tweet(row, len(items), topic, id_prefix="XA")
        if item and not any(_is_own(item.get("url", ""), h) for h in clean):
            items.append(item)
    return items


def parse_x_response(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract items from a search_x() response (already normalized)."""
    return response.get("items", [])


def _parse_tweet(
    tweet: Dict[str, Any], index: int, query: str, id_prefix: str = "XP"
) -> Dict[str, Any] | None:
    """Parse one apidojo/tweet-scraper row into the standard item format."""
    author = tweet.get("author") or {}
    username = str(
        author.get("userName") or author.get("username") or author.get("screen_name") or ""
    ).lstrip("@")
    tweet_id = str(tweet.get("id", ""))

    url = str(tweet.get("url") or tweet.get("twitterUrl") or "")
    if not url and username and tweet_id:
        url = f"https://x.com/{username}/status/{tweet_id}"
    if not url:
        return None
    if not username:
        # Fall back to the permalink's author segment.
        for host in ("x.com/", "twitter.com/"):
            if host in url and "/status/" in url:
                username = url.split(host, 1)[1].split("/status/", 1)[0]
                break
    if not username:
        return None

    # Parse date (ISO or classic Twitter format).
    date = None
    created_at = tweet.get("createdAt") or tweet.get("created_at") or ""
    if created_at:
        try:
            if len(created_at) > 10 and created_at[10] == "T":
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    text = str(tweet.get("text") or tweet.get("fullText") or "").strip()[:500]

    # Leading-run @mentions = who the post is directed at (reply target).
    from .query import leading_mentions
    mentioned_handles = leading_mentions(text)

    engagement = {
        "likes": _safe_int(tweet.get("likeCount")),
        "reposts": _safe_int(tweet.get("retweetCount")),
        "replies": _safe_int(tweet.get("replyCount")),
        "quotes": _safe_int(tweet.get("quoteCount")),
        "views": _safe_int(tweet.get("viewCount")),
        "bookmarks": _safe_int(tweet.get("bookmarkCount")),
    }

    return {
        "id": f"{id_prefix}{index + 1}",
        "text": text,
        "url": url,
        "author_handle": username,
        "date": date,
        "engagement": engagement,
        "mentioned_handles": mentioned_handles,
        "relevance": _compute_relevance(query, text) if query else 0.7,
        "why_relevant": "",
    }


def _safe_int(value: Any) -> int | None:
    """Convert value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
