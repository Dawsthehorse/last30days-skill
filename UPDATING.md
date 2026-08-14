# Updating this fork (apify-dispatch branch)

This fork carries one change on top of upstream `mvanhorn/last30days-skill`:
the X source runs an Apify pay-per-result tweet scraper (see `ACTOR_ID` in
`apify_x.py`; kaitoeasyapi's, because apidojo's gates free-plan Apify tokens)
through the private api-dispatch gateway instead of the stock Twitter/X
backends. The stock
backends remain as failover.

## What the branch changes

- `skills/last30days/scripts/lib/apify_x.py` (new) — the gateway-backed X backend
- `skills/last30days/scripts/lib/env.py` — `apify` added first in `_X_BACKEND_ORDER`,
  availability check, diagnose status
- `skills/last30days/scripts/lib/backends.py` — `apify` probe + paid flag
- `skills/last30days/scripts/lib/pipeline.py` — dispatch arm + FROM/ABOUT handle lanes
- `tests/test_apify_x.py` (new), small assertion updates in
  `tests/test_backend_descriptors.py`, env isolation fixture in `tests/conftest.py`

## Configuration

The backend needs two values, resolved from skill config, process env, or an
env file:

```
API_DISPATCH_SERVICE_URL=<gateway base URL>
API_DISPATCH_SERVICE_KEY=<gateway service key>
```

Either set them user-level, or point `API_DISPATCH_ENV_FILE` at a `.env` file
that contains them (e.g. the Business repo root `.env`):

```
setx API_DISPATCH_ENV_FILE C:\Users\dawso\Desktop\Projects\Dawson\Business\.env
```

Pin the backend explicitly with `LAST30DAYS_X_BACKEND=apify` if wanted; by
default it is simply first in the failover chain when configured.

## Pulling upstream updates

Git carries the patch forward; this is the whole reapply mechanism:

```
git fetch upstream
git merge upstream/main
# resolve conflicts only if upstream touched the files listed above
uv run pytest tests/test_apify_x.py tests/test_backend_descriptors.py -q
git push origin apify-dispatch
```

Remotes: `origin` = Dawsthehorse/last30days-skill (this fork),
`upstream` = mvanhorn/last30days-skill.

The skill is installed by junction from `%USERPROFILE%\.claude\skills\last30days`
to `skills\last30days` in this checkout, so a merge here is live immediately —
no reinstall step.
