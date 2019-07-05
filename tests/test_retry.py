import mock
import unittest

from storage import retry


class UnRetriableError(Exception):
    do_not_retry = True


class TestRetry(unittest.TestCase):
    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_does_not_retry_on_success(self, mock_sleep, mock_uniform):
        successful_function = mock.Mock(return_value="result")

        result = retry.attempt(successful_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(0, mock_uniform.call_count)
        self.assertEqual(0, mock_sleep.call_count)

        successful_function.assert_called_with(1, 2, foo="bar", biz="baz")
        self.assertEqual("result", result)

    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_retries_on_failure(self, mock_sleep, mock_uniform):
        mock_uniform.side_effect = [0.5, 2.4]

        failing_function = mock.Mock(side_effect=[RuntimeError, RuntimeError, "result"])

        result = retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3)
        ])

        mock_sleep.assert_has_calls([
            mock.call(0.5),
            mock.call(2.4)
        ])

        self.assertEqual(3, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")
        self.assertEqual("result", result)

    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_reraises_last_exception_on_attempt_exhaustion(self, mock_sleep, mock_uniform):
        mock_uniform.side_effect = [0.5, 2.4, 3.6, 5.6]

        failing_function = mock.Mock(
            side_effect=[RuntimeError, RuntimeError, RuntimeError, RuntimeError, RuntimeError])

        with self.assertRaises(RuntimeError):
            retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(4, mock_uniform.call_count)
        self.assertEqual(4, mock_sleep.call_count)

        self.assertEqual(5, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")

    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_does_not_retry_unretriable_errors(self, mock_sleep, mock_uniform):
        failing_function = mock.Mock(side_effect=UnRetriableError)

        with self.assertRaises(UnRetriableError):
            retry.attempt(failing_function, 1, 2, foo="bar", biz="baz")

        self.assertEqual(0, mock_uniform.call_count)
        self.assertEqual(0, mock_sleep.call_count)

        self.assertEqual(1, failing_function.call_count)
        failing_function.assert_called_with(1, 2, foo="bar", biz="baz")
