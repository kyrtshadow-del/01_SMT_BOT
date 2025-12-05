import unittest

from bot_new import WialonClient


class WialonDeviceDetailsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = WialonClient("https://example.com", "token")

    def tearDown(self) -> None:
        self.client.close()

    def test_extract_device_details(self):
        item = {
            "id": 123,
            "uid": "356789012345678",
            "hw": {"id": 42, "name": "Example HW"},
        }
        details = self.client._extract_device_details(item)
        self.assertEqual(details["unit_id"], 123)
        self.assertEqual(details["device_uid"], "356789012345678")
        self.assertEqual(details["device_type_id"], 42)
        self.assertEqual(details["device_type_name"], "Example HW")


if __name__ == "__main__":
    unittest.main()
