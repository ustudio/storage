import mock
import unittest

from storage import retry


class UnRetriableError(StandardError):
    do_not_retry = True


class TestRetry(unittest.TestCase):
    def test_does_not_retry_on_success(self):
        successful_function = mock.Mock(return_value="result")

        result = retry.attempt(successful_function, 1, 2, foo="bar", biz="baz")

        successful_function.assert_called_with(1, 2, foo="bar", biz="baz")
        self.assertEqual("result", result)

    def test_retries_on_failure(self):
        failing_function = mock.Mock(side_effect=[RuntimeError, RuntimeError, "result"])

        result = retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(3, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")
        self.assertEqual("result", result)

    def test_reraises_last_exception_on_attempt_exhaustion(self):
        failing_function = mock.Mock(side_effect=[RuntimeError, RuntimeError, RuntimeError])

        with self.assertRaises(RuntimeError):
            retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(3, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")

    def test_does_not_retry_unretriable_errors(self):
        failing_function = mock.Mock(side_effect=UnRetriableError)

        with self.assertRaises(UnRetriableError):
            retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(1, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")
