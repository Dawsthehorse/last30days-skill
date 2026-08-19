import os
import unittest
from unittest.mock import patch

from lib.apify_x import (
    DEPTH_CONFIG,
    _is_own,
    _looks_like_tweet,
    _parse_tweet,
    _safe_int,
    gateway_config,
    is_available,
    parse_x_response,
    search_handles,
    search_mentions,
    search_x,
)

CONFIG = {
    "API_DISPATCH_SERVICE_URL": "https://gw.example",
    "API_DISPATCH_SERVICE_KEY": "test-key",
}


def _run_envelope(status="SUCCEEDED", dataset_id="ds1", job_status="completed"):
    return {
        "ok": True,
        "jobId": "job-1",
        "status": job_status,
        "result": {"data": {"id": "run-1", "status": status, "defaultDatasetId": dataset_id}},
    }


def _dataset_envelope(rows):
    return {"ok": True, "jobId": "job-2", "status": "completed", "result": rows}


def _tweet(tid="111", username="aidev", text="AI agents are amazing", **extra):
    row = {
        "id": tid,
        "text": text,
        "createdAt": "2026-02-15T10:00:00Z",
        "likeCount": 50,
        "retweetCount": 12,
        "replyCount": 3,
        "quoteCount": 1,
        "viewCount": 2000,
        "bookmarkCount": 5,
        "author": {"userName": username},
    }
    row.update(extra)
    return row


class TestAvailability(unittest.TestCase):
    def test_config_values_resolve(self):
        self.assertTrue(is_available(CONFIG))
        self.assertEqual(gateway_config(CONFIG), ("https://gw.example", "test-key"))

    def test_missing_values_unavailable(self):
        self.assertFalse(is_available({}))
        self.assertFalse(is_available({"API_DISPATCH_SERVICE_URL": "https://gw.example"}))

    def test_process_env_resolves(self):
        import os
        with patch.dict(os.environ, CONFIG):
            self.assertTrue(is_available({}))

    def test_env_file_fallback(self):
        import os
        import tempfile
        import lib.apify_x as ax
        with tempfile.NamedTemporaryFile(
            "w", suffix=".env", delete=False, encoding="utf-8"
        ) as f:
            f.write("# comment\n")
            f.write('API_DISPATCH_SERVICE_URL="https://file.example"\n')
            f.write("API_DISPATCH_SERVICE_KEY=file-key\n")
            path = f.name
        try:
            with patch.dict(os.environ, {"API_DISPATCH_ENV_FILE": path}):
                ax._env_file_cache = None
                self.assertTrue(is_available({}))
                self.assertEqual(gateway_config({}), ("https://file.example", "file-key"))
        finally:
            ax._env_file_cache = None
            os.unlink(path)

    def test_global_config_file_reaches_is_available(self):
        """The documented ~/.config/last30days/.env path must actually work.

        get_config() copies only allowlisted keys into config, so a missing
        registration leaves the config tier of _resolve() permanently dead
        while every unit test (which hand-builds a config dict) still passes.
        """
        import pathlib
        import tempfile

        import lib.env as env_mod

        original = env_mod.CONFIG_FILE
        path = pathlib.Path(tempfile.mkdtemp()) / ".env"
        path.write_text(
            "API_DISPATCH_SERVICE_URL=https://from-global\n"
            "API_DISPATCH_SERVICE_KEY=global-key\n",
            encoding="utf-8",
        )
        try:
            env_mod.CONFIG_FILE = path
            config = env_mod.get_config()
            self.assertTrue(is_available(config))
            self.assertEqual(
                gateway_config(config), ("https://from-global", "global-key")
            )
            self.assertEqual(env_mod.x_backend_chain(config)[0], "apify")
        finally:
            env_mod.CONFIG_FILE = original

    def test_config_wins_over_env(self):
        import os
        with patch.dict(os.environ, {"API_DISPATCH_SERVICE_URL": "https://env.example"}):
            url, _ = gateway_config(CONFIG)
        self.assertEqual(url, "https://gw.example")


class TestSearchX(unittest.TestCase):
    def test_unconfigured_returns_error(self):
        result = search_x("test", "2026-01-01", "2026-03-01", config={})
        self.assertEqual(result["items"], [])
        self.assertIn("API_DISPATCH", result["error"])

    @patch("lib.apify_x.http.post")
    def test_successful_search(self, mock_post):
        mock_post.side_effect = [
            _run_envelope(),
            _dataset_envelope([_tweet()]),
        ]
        result = search_x("AI agents", "2026-02-01", "2026-03-01", config=CONFIG)
        self.assertNotIn("error", result)
        self.assertEqual(len(result["items"]), 1)
        item = result["items"][0]
        self.assertEqual(item["author_handle"], "aidev")
        self.assertEqual(item["engagement"]["likes"], 50)
        self.assertEqual(item["date"], "2026-02-15")
        # First call is the actor run against the gateway execute endpoint.
        url, payload = mock_post.call_args_list[0][0][0], mock_post.call_args_list[0][0][1]
        self.assertEqual(url, "https://gw.example/api/execute")
        from lib.apify_x import ACTOR_ID
        self.assertEqual(payload["service"], "apify")
        self.assertEqual(payload["input"]["actorId"], ACTOR_ID)
        headers = mock_post.call_args_list[0][1]["headers"]
        self.assertEqual(headers["x-service-key"], "test-key")

    @patch("lib.apify_x.http.post")
    def test_search_terms_carry_date_window(self, mock_post):
        mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
        search_x("AI agents", "2026-02-01", "2026-03-01", depth="quick", config=CONFIG)
        terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
        self.assertEqual(len(terms), 1)
        self.assertIn("since:2026-02-01", terms[0])
        self.assertIn("until:2026-03-01", terms[0])

    @patch("lib.apify_x.http.post")
    def test_min_faves_floor_applies_to_topic_search(self, mock_post):
        mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
        with patch.dict(os.environ, {"LAST30DAYS_X_MIN_FAVES": "500"}):
            search_x("AI agents", "2026-02-01", "2026-03-01", depth="quick",
                     config=CONFIG)
        terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
        self.assertIn("min_faves:500", terms[0])

    @patch("lib.apify_x.http.post")
    def test_min_faves_absent_or_junk_leaves_the_term_alone(self, mock_post):
        # "-5" is a sign typo, not a floor of 5: it must read as off, and the
        # caller must not be told a floor is in force while every result bills.
        for value in ("", "0", "lots", "-5"):
            mock_post.reset_mock()
            mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
            with patch.dict(os.environ, {"LAST30DAYS_X_MIN_FAVES": value}):
                search_x("AI agents", "2026-02-01", "2026-03-01", depth="quick",
                         config=CONFIG)
            terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
            self.assertNotIn("min_faves", terms[0], msg=value)

    @patch("lib.apify_x.http.post")
    def test_min_faves_resolves_from_the_skill_config(self, mock_post):
        # The documented ~/.config/last30days/.env path lands in the config
        # dict, not os.environ. A floor set there must apply, or the user pays
        # per result for the noise it was meant to cut.
        mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
        cfg = {**CONFIG, "LAST30DAYS_X_MIN_FAVES": "750"}
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LAST30DAYS_X_MIN_FAVES", None)
            search_x("AI agents", "2026-02-01", "2026-03-01", depth="quick",
                     config=cfg)
        terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
        self.assertIn("min_faves:750", terms[0])

    @patch("lib.apify_x.http.post")
    def test_min_faves_never_reaches_handle_or_mention_lookups(self, mock_post):
        # The floor is a topic-search knob. A named account is the point of
        # these lanes whatever its reach, so a floor must not silently empty
        # them.
        for fn in (search_handles, search_mentions):
            mock_post.reset_mock()
            mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
            with patch.dict(os.environ, {"LAST30DAYS_X_MIN_FAVES": "500"}):
                if fn is search_handles:
                    fn(["someone"], "AI", "2026-02-01", "2026-03-01", config=CONFIG)
                else:
                    fn(["someone"], "2026-02-01", "2026-03-01", config=CONFIG)
            terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
            self.assertNotIn("min_faves", terms[0], msg=fn.__name__)

    @patch("lib.apify_x.http.post")
    def test_auth_error_is_fatal(self, mock_post):
        from lib import http as http_mod
        mock_post.side_effect = http_mod.HTTPError("Unauthorized", status_code=401)
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(result["items"], [])
        self.assertIn("auth failed", result["error"])

    @patch("lib.apify_x.http.post")
    def test_transient_http_error_settles_empty_without_error(self, mock_post):
        from lib import http as http_mod
        mock_post.side_effect = http_mod.HTTPError("Server Error", status_code=500)
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(result["items"], [])
        self.assertNotIn("error", result)

    @patch("lib.apify_x.http.post")
    def test_failed_gateway_job_is_error(self, mock_post):
        mock_post.return_value = {
            "ok": False,
            "jobId": "job-1",
            "status": "failed",
            "error": "apify returned 402: payment required",
        }
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(result["items"], [])
        self.assertIn("402", result["error"])

    @patch("lib.apify_x.time.sleep", return_value=None)
    @patch("lib.apify_x.http.post")
    def test_running_actor_is_polled_to_completion(self, mock_post, _sleep):
        mock_post.side_effect = [
            _run_envelope(status="RUNNING"),
            _run_envelope(status="SUCCEEDED"),
            _dataset_envelope([_tweet()]),
        ]
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(len(result["items"]), 1)
        # Second call is the runStatus poll.
        poll_input = mock_post.call_args_list[1][0][1]["input"]
        self.assertEqual(poll_input["kind"], "runStatus")
        self.assertEqual(poll_input["runId"], "run-1")

    @patch("lib.apify_x.time.sleep", return_value=None)
    @patch("lib.apify_x.http.get")
    @patch("lib.apify_x.http.post")
    def test_running_gateway_job_is_polled_via_jobs_endpoint(
        self, mock_post, mock_get, _sleep
    ):
        # /api/execute hands back a still-running gateway job; the jobs
        # endpoint then reports it completed.
        mock_post.side_effect = [
            {"ok": True, "jobId": "job-9", "status": "running"},
            _dataset_envelope([_tweet()]),
        ]
        mock_get.return_value = {"ok": True, "job": _run_envelope()}
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(len(result["items"]), 1)
        url = mock_get.call_args[0][0]
        self.assertEqual(url, "https://gw.example/api/jobs/apify/job-9")

    @patch("lib.apify_x.http.post")
    def test_non_succeeded_run_is_error(self, mock_post):
        mock_post.return_value = _run_envelope(status="FAILED")
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(result["items"], [])
        self.assertIn("FAILED", result["error"])

    @patch("lib.apify_x.http.post")
    def test_no_results_sentinel_rows_skipped(self, mock_post):
        mock_post.side_effect = [
            _run_envelope(),
            _dataset_envelope([{"noResults": True}, _tweet()]),
        ]
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(len(result["items"]), 1)

    @patch("lib.apify_x.http.post")
    def test_duplicate_tweet_ids_deduped(self, mock_post):
        mock_post.side_effect = [
            _run_envelope(),
            _dataset_envelope([_tweet(tid="1"), _tweet(tid="1"), _tweet(tid="2")]),
        ]
        result = search_x("test", "2026-01-01", "2026-03-01", config=CONFIG)
        self.assertEqual(len(result["items"]), 2)


class TestParseTweet(unittest.TestCase):
    def test_valid_tweet(self):
        item = _parse_tweet(_tweet(), 0, "AI agents")
        self.assertIsNotNone(item)
        self.assertEqual(item["id"], "XP1")
        self.assertEqual(item["url"], "https://x.com/aidev/status/111")
        self.assertEqual(item["author_handle"], "aidev")
        self.assertEqual(item["engagement"]["views"], 2000)
        self.assertGreater(item["relevance"], 0)

    def test_explicit_url_wins(self):
        item = _parse_tweet(
            _tweet(url="https://x.com/aidev/status/999"), 0, "test"
        )
        self.assertEqual(item["url"], "https://x.com/aidev/status/999")

    def test_username_recovered_from_url(self):
        tweet = {"id": "5", "text": "t", "url": "https://x.com/someone/status/5"}
        item = _parse_tweet(tweet, 0, "test")
        self.assertIsNotNone(item)
        self.assertEqual(item["author_handle"], "someone")

    def test_no_url_no_author_returns_none(self):
        self.assertIsNone(_parse_tweet({"id": "1", "text": "t"}, 0, "test"))

    def test_twitter_date_format(self):
        item = _parse_tweet(
            _tweet(createdAt="Wed Jan 15 14:30:00 +0000 2026"), 0, "test"
        )
        self.assertEqual(item["date"], "2026-01-15")

    def test_invalid_date_graceful(self):
        item = _parse_tweet(_tweet(createdAt="not-a-date"), 0, "test")
        self.assertIsNone(item["date"])

    def test_text_truncated_at_500(self):
        item = _parse_tweet(_tweet(text="x" * 600), 0, "test")
        self.assertEqual(len(item["text"]), 500)

    def test_leading_mentions_captured(self):
        item = _parse_tweet(_tweet(text="@jack @pmarca thoughts on this"), 0, "topic")
        self.assertEqual(["jack", "pmarca"], item["mentioned_handles"])

    def test_index_offset_and_prefix(self):
        item = _parse_tweet(_tweet(), 4, "test", id_prefix="XF")
        self.assertEqual(item["id"], "XF5")


class TestLooksLikeTweet(unittest.TestCase):
    def test_sentinel_rejected(self):
        self.assertFalse(_looks_like_tweet({"noResults": True}))

    def test_empty_rejected(self):
        self.assertFalse(_looks_like_tweet({}))

    def test_id_accepted(self):
        self.assertTrue(_looks_like_tweet({"id": "1"}))


class TestSafeInt(unittest.TestCase):
    def test_values(self):
        self.assertEqual(_safe_int(42), 42)
        self.assertEqual(_safe_int("100"), 100)
        self.assertIsNone(_safe_int(None))
        self.assertIsNone(_safe_int("abc"))
        self.assertEqual(_safe_int(0), 0)


class TestParseXResponse(unittest.TestCase):
    def test_extracts_items(self):
        self.assertEqual(len(parse_x_response({"items": [{"id": "1"}]})), 1)
        self.assertEqual(parse_x_response({}), [])


class TestFromLane(unittest.TestCase):
    @patch("lib.apify_x.http.post")
    def test_from_query_shape_and_no_topic_anded(self, mock_post):
        mock_post.side_effect = [
            _run_envelope(),
            _dataset_envelope([_tweet(username="elonmusk")]),
        ]
        items = search_handles(
            ["@elonmusk"], "Grok 4", "2026-05-19", "2026-06-18",
            count_per=8, config=CONFIG,
        )
        terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
        self.assertEqual(terms, ["from:elonmusk since:2026-05-19 until:2026-06-18"])
        self.assertEqual(1, len(items))
        self.assertEqual("XF1", items[0]["id"])

    def test_unconfigured_or_no_handles_returns_empty(self):
        self.assertEqual([], search_handles(["@x"], "t", "a", "b", config={}))
        self.assertEqual([], search_handles([], "t", "a", "b", config=CONFIG))

    @patch("lib.apify_x.http.post")
    def test_handles_batch_into_one_run(self, mock_post):
        mock_post.side_effect = [_run_envelope(), _dataset_envelope([])]
        search_handles(["h1", "h2"], "topic", "2026-05-19", "2026-06-18",
                       count_per=8, config=CONFIG)
        payload = mock_post.call_args_list[0][0][1]["input"]["input"]
        self.assertEqual(len(payload["searchTerms"]), 2)
        self.assertEqual(payload["maxItems"], 16)


class TestAboutLane(unittest.TestCase):
    @patch("lib.apify_x.http.post")
    def test_mentions_drop_own_tweets(self, mock_post):
        mock_post.side_effect = [
            _run_envelope(),
            _dataset_envelope([
                _tweet(tid="1", username="fan", text="@elonmusk nice"),
                _tweet(tid="2", username="elonmusk", text="my own post"),
            ]),
        ]
        items = search_mentions(
            ["elonmusk"], "2026-05-19", "2026-06-18",
            topic="Grok 4", count_per=5, config=CONFIG,
        )
        terms = mock_post.call_args_list[0][0][1]["input"]["input"]["searchTerms"]
        self.assertEqual(terms, ["@elonmusk since:2026-05-19 until:2026-06-18"])
        authors = {it["author_handle"] for it in items}
        self.assertIn("fan", authors)
        self.assertNotIn("elonmusk", authors)


class TestIsOwn(unittest.TestCase):
    def test_own_tweet_detected(self):
        self.assertTrue(_is_own("https://x.com/elonmusk/status/123", "elonmusk"))
        self.assertTrue(_is_own("https://twitter.com/elonmusk/status/123", "@elonmusk"))

    def test_other_author_not_own(self):
        self.assertFalse(_is_own("https://x.com/someoneelse/status/123", "elonmusk"))


class TestDepthConfig(unittest.TestCase):
    def test_all_depths_have_limit_and_queries(self):
        for depth_name, cfg in DEPTH_CONFIG.items():
            self.assertIn("limit", cfg, f"{depth_name} missing 'limit'")
            self.assertIn("queries", cfg, f"{depth_name} missing 'queries'")

    def test_deep_has_highest_limit(self):
        self.assertGreater(DEPTH_CONFIG["deep"]["limit"], DEPTH_CONFIG["default"]["limit"])
        self.assertGreater(DEPTH_CONFIG["default"]["limit"], DEPTH_CONFIG["quick"]["limit"])


class TestChainIntegration(unittest.TestCase):
    def test_apify_first_in_chain_when_configured(self):
        from lib import env
        chain = env.x_backend_chain(dict(CONFIG))
        self.assertEqual(chain[0], "apify")

    def test_apify_absent_when_unconfigured(self):
        from lib import env
        self.assertNotIn("apify", env.x_backend_chain({}))

    def test_pin_forces_apify(self):
        from lib import env
        config = dict(CONFIG)
        config["LAST30DAYS_X_BACKEND"] = "apify"
        self.assertEqual(["apify"], env.x_backend_chain(config))

    @patch("lib.bird_x.get_bird_status")
    @patch("lib.xurl_x.is_available", return_value=False)
    def test_diagnose_reports_apify_source(self, _xurl, mock_bird):
        from lib import env
        mock_bird.return_value = {"installed": False, "authenticated": False,
                                  "username": "", "can_install": False}
        status = env.get_x_source_status(dict(CONFIG))
        self.assertEqual("apify", status["source"])
        self.assertTrue(status["apify_available"])

    def test_normalize_propagates_mentions(self):
        from lib import normalize
        item = _parse_tweet(_tweet(text="@jack hi"), 0, "topic")
        normalized = normalize.normalize_source_items(
            "x", [item], "2026-01-19", "2026-02-18"
        )
        self.assertEqual(["jack"], normalized[0].metadata.get("mentioned_handles"))


if __name__ == "__main__":
    unittest.main()
