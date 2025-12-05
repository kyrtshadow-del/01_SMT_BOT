import unittest
from unittest.mock import patch


class DummyClient:
    def __init__(self, host, token):
        self.host = host
        self.token = token
        self.requests = []

    def request(self, *args, **kwargs):
        self.requests.append((args, kwargs))
        return self.token

    def load_messages_interval(self, *args, **kwargs):
        self.requests.append((args, kwargs))
        return self.token


class WialonPoolTests(unittest.TestCase):
    def test_deduplicates_tokens_and_obeys_limit(self):
        created_tokens = []

        class RecordingClient(DummyClient):
            def __init__(self, host, token):
                created_tokens.append(token)
                super().__init__(host, token)

        with patch('pipeline.sources.wialon_pool.WialonClient', RecordingClient):
            from pipeline.sources.wialon_pool import WialonTokenPool

            pool = WialonTokenPool(
                host='https://example',
                tokens=[('primary', 'AAA'), ('extra-1', 'BBB'), ('dup', 'BBB')],
                parallel_limit=3,
                rotate_after_units=10,
                cooldown_seconds=0,
            )

        # Only two unique tokens should be instantiated.
        self.assertEqual(created_tokens, ['AAA', 'BBB'])
        self.assertEqual(len(pool._states), 2)
        self.assertEqual(pool.parallel_limit, 3)

    def test_lease_exposes_label(self):
        with patch('pipeline.sources.wialon_pool.WialonClient', DummyClient):
            from pipeline.sources.wialon_pool import PooledWialonClient, WialonTokenPool

            pool = WialonTokenPool(
                host='https://example',
                tokens=[('primary', 'AAA'), ('extra-1', 'BBB')],
                parallel_limit=2,
                rotate_after_units=5,
                cooldown_seconds=0,
            )
            client = PooledWialonClient(pool)
            labels = set()
            for _ in range(4):
                with client.lease() as lease:
                    labels.add(lease.label)
                    lease.client.request('svc')
            self.assertEqual(labels, {'primary', 'extra-1'})

    def test_rotate_triggers_cooldown_and_sleep(self):
        from pipeline.sources import wialon_pool as pool_module

        class FakeTime:
            def __init__(self):
                self.now = 0.0
                self.sleeps = []

            def monotonic(self):
                return self.now

            def sleep(self, seconds):
                self.sleeps.append(seconds)
                self.now += seconds

        fake_time = FakeTime()

        with patch('pipeline.sources.wialon_pool.WialonClient', DummyClient), patch.object(
            pool_module, 'time', fake_time
        ):
            from pipeline.sources.wialon_pool import WialonTokenPool

            pool = WialonTokenPool(
                host='https://example',
                tokens=[('primary', 'AAA')],
                parallel_limit=1,
                rotate_after_units=2,
                cooldown_seconds=10,
            )

            # Process two units -> should start cooldown.
            state = pool.borrow()
            pool.release(state)
            state = pool.borrow()
            pool.release(state)

            self.assertGreater(state.cooldown_until, fake_time.monotonic())
            self.assertFalse(state.is_available)

            # Next borrow should sleep for cooldown duration and make token available again.
            state = pool.borrow()
            self.assertEqual(fake_time.sleeps, [10])
            self.assertTrue(state.is_available)

    def test_session_recreates_client_after_failure(self):
        creations = []

        class RecordingClient(DummyClient):
            def __init__(self, host, token):
                creations.append(token)
                super().__init__(host, token)

        with patch('pipeline.sources.wialon_pool.WialonClient', RecordingClient):
            from pipeline.sources.wialon_pool import PooledWialonClient, WialonTokenPool

            pool = WialonTokenPool(
                host='https://example',
                tokens=[('primary', 'AAA')],
                parallel_limit=1,
                rotate_after_units=5,
                cooldown_seconds=0,
            )
            client = PooledWialonClient(pool)

            with self.assertRaises(RuntimeError):
                with client.lease() as lease:
                    # Simulate failure.
                    raise RuntimeError('boom')

            # First creation happened during pool init, second after failure.
            self.assertEqual(creations, ['AAA', 'AAA'])


if __name__ == '__main__':
    unittest.main()
