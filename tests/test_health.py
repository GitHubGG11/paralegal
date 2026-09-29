import unittest

from src.health import health_message


class HealthTests(unittest.TestCase):
    def test_learning_environment_is_wired_for_unit_tests(self):
        self.assertEqual(health_message(), "Learning worker is running.")


if __name__ == "__main__":
    unittest.main()
