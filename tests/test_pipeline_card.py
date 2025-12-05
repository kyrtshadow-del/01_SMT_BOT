import unittest

from bot_new import _merge_event_params  # type: ignore


class PipelineCardMergeTests(unittest.TestCase):
    def test_latest_params_backfill_missing_keys(self):
        merged = _merge_event_params(
            {"speed": 10},
            {"params": {"sats": 12, "speed": 7}},
        )
        self.assertEqual(merged["speed"], "10")
        self.assertEqual(merged["sats"], "12")

    def test_handles_none_inputs(self):
        merged = _merge_event_params(None, None)
        self.assertEqual(merged, {})


if __name__ == "__main__":
    unittest.main()
