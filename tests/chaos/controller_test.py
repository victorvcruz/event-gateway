import concurrent.futures
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import Store


class StoreTest(unittest.TestCase):
    def test_rule_is_one_shot_under_concurrency(self):
        store = Store()
        store.put_rule("one", {"process": "worker", "point": "after", "match": {"correlation_id": "o-1"}})
        payload = {"process": "worker", "point": "after", "correlation_id": "o-1"}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            actions = list(pool.map(lambda _: store.record_hit(payload)[0], range(16)))
        self.assertEqual(1, actions.count("kill"))
        self.assertIsNotNone(store.hit("one"))

    def test_match_is_exact_subset(self):
        store = Store()
        store.put_rule("one", {"process": "worker", "point": "after", "match": {"event_type": "order.created"}})
        self.assertEqual("continue", store.record_hit({"process": "worker", "point": "after", "event_type": "stock.failed"})[0])
        self.assertEqual("kill", store.record_hit({"process": "worker", "point": "after", "event_type": "order.created", "extra": "ok"})[0])


if __name__ == "__main__":
    unittest.main()
